use std::{
    net::SocketAddr,
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

use actix_web::{
    post,
    web::{self},
};

use serde::{Deserialize, Serialize};
use store_rocksdb::RocksDBStore;
use tokio::{net::UdpSocket, sync::mpsc, time};
use tracing::{debug, error, info};

use crate::{error::JMAPServerError, JMAPServer};

use super::{
    EpochId, GenerationId, JMAPCluster, JMAPPeer, PeerId, ShardId, HEARTBEAT_WINDOW,
    HEARTBEAT_WINDOW_MASK,
};

pub const DEFAULT_SWIM_PORT: u16 = 7911;
const PING_INTERVAL: u64 = 1000;
const UDP_MAX_PAYLOAD: usize = 65500;
const HB_MAX_PAUSE_MS: f64 = 0.0;
const HB_MIN_STD_DEV: f64 = 300.0;
const HB_PHI_SUSPECT_THRESHOLD: f64 = 5.0;
const HB_PHI_CONVICT_THRESHOLD: f64 = 9.0;

#[derive(Debug)]
pub enum SWIMStatus {
    Seed,
    Alive,
    Suspected,
    Leaving,
    Offline,
}

#[derive(Debug, Serialize, Deserialize)]
struct SWIMPeerState {
    pub peer_id: PeerId,
    pub epoch: EpochId,
    pub generation: GenerationId,
}

impl From<&JMAPPeer> for SWIMPeerState {
    fn from(peer: &JMAPPeer) -> Self {
        SWIMPeerState {
            peer_id: peer.peer_id,
            epoch: peer.epoch,
            generation: peer.generation,
        }
    }
}

impl From<&JMAPCluster> for SWIMPeerState {
    fn from(cluster: &JMAPCluster) -> Self {
        SWIMPeerState {
            peer_id: cluster.peer_id,
            epoch: cluster.epoch.load(Ordering::Relaxed),
            generation: cluster.generation,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SWIMSync {
    pub key: String,
    pub peers: Vec<SWIMPeerInfo>,
}

impl From<&JMAPCluster> for SWIMSync {
    fn from(cluster: &JMAPCluster) -> Self {
        SWIMSync {
            key: cluster.key.clone(),
            peers: build_peer_info(cluster),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SWIMPeerInfo {
    pub peer_id: PeerId,
    pub shard_id: ShardId,
    pub epoch: EpochId,
    pub generation: GenerationId,
    pub swim_addr: SocketAddr,
    pub rpc_url: String,
    pub jmap_url: String,
}

impl From<&JMAPPeer> for SWIMPeerInfo {
    fn from(peer: &JMAPPeer) -> Self {
        SWIMPeerInfo {
            peer_id: peer.peer_id,
            shard_id: peer.shard_id,
            epoch: peer.epoch,
            generation: peer.generation,
            swim_addr: peer.swim_addr,
            rpc_url: peer.rpc_url.clone(),
            jmap_url: peer.jmap_url.clone(),
        }
    }
}

impl From<&JMAPCluster> for SWIMPeerInfo {
    fn from(cluster: &JMAPCluster) -> Self {
        SWIMPeerInfo {
            peer_id: cluster.peer_id,
            shard_id: cluster.shard_id,
            epoch: cluster.epoch.load(std::sync::atomic::Ordering::Relaxed),
            generation: cluster.generation,
            swim_addr: cluster.swim_addr,
            rpc_url: cluster.rpc_url.clone(),
            jmap_url: cluster.jmap_url.clone(),
        }
    }
}

impl From<SWIMPeerInfo> for JMAPPeer {
    fn from(peer: SWIMPeerInfo) -> Self {
        JMAPPeer {
            peer_id: peer.peer_id,
            shard_id: peer.shard_id,
            epoch: peer.epoch,
            generation: peer.generation,
            swim_addr: peer.swim_addr,
            rpc_url: peer.rpc_url,
            jmap_url: peer.jmap_url,
            status: SWIMStatus::Alive,
            last_heartbeat: Instant::now(),
            hb_window: vec![0; HEARTBEAT_WINDOW],
            hb_window_pos: 0,
            hb_sum: 0,
            hb_sq_sum: 0,
            hb_is_full: false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum SWIMRequest {
    Join(u16),
    Synchronize(String),
    Ping(Vec<SWIMPeerState>),
    Pong(Vec<SWIMPeerState>),
}

pub async fn start_swim(core: web::Data<JMAPServer<RocksDBStore>>, bind_addr: SocketAddr) {
    let _socket = Arc::new(match UdpSocket::bind(bind_addr).await {
        Ok(socket) => socket,
        Err(e) => {
            error!("Failed to bind UDP socket on '{}': {}", bind_addr, e);
            std::process::exit(1);
        }
    });
    let (tx, mut rx) = mpsc::channel(128);

    let socket = _socket.clone();
    tokio::spawn(async move {
        let mut last_ping = Instant::now();
        let mut last_peer_pinged = u32::MAX as usize;
        let cluster = core.cluster.as_ref().unwrap();

        loop {
            match time::timeout(Duration::from_millis(PING_INTERVAL), rx.recv()).await {
                Ok(Some((request, source_addr))) => {
                    if let Some((target_addr, response)) =
                        handle_request(core.clone(), request, source_addr)
                    {
                        match bincode::serialize(&response) {
                            Ok(bytes) => {
                                if let Err(e) = socket.send_to(&bytes, &target_addr).await {
                                    error!("Failed to send UDP packet to {}: {}", target_addr, e);
                                }
                            }
                            Err(e) => {
                                error!("Failed to serialize UDP packet: {}", e);
                            }
                        }
                    }
                }
                Ok(None) => {
                    debug!("SWIM thread exiting.");
                }
                Err(_) => (),
            }

            if last_ping.elapsed().as_millis() as u64 >= PING_INTERVAL {
                let mut ping_peer = None;

                if let Ok(mut local_peers) = cluster.peers.write() {
                    if !local_peers.is_empty() {
                        for peer in local_peers.iter_mut() {
                            // Failure detection
                            if !matches!(peer.status, SWIMStatus::Offline) {
                                check_heartbeat(peer);
                            }
                        }

                        // Find next peer to ping
                        let num_peers = local_peers.len();
                        for _ in 0..num_peers {
                            last_peer_pinged = (last_peer_pinged + 1) % num_peers;
                            let peer = &local_peers[last_peer_pinged];

                            match peer.status {
                                SWIMStatus::Seed => {
                                    ping_peer = Some((false, peer.swim_addr));
                                    break;
                                }
                                SWIMStatus::Alive | SWIMStatus::Suspected => {
                                    ping_peer = Some((true, peer.swim_addr));
                                    break;
                                }
                                SWIMStatus::Leaving | SWIMStatus::Offline => (),
                            }
                        }
                    }
                } else {
                    error!("Failed to acquire cluster.peers write lock.");
                }

                if let Some((has_joined, target_addr)) = ping_peer {
                    //debug!("Sending pingReq to {}.", target_addr.port());
                    let request = if has_joined {
                        // Increase Epoch
                        cluster.epoch.fetch_add(1, Ordering::Relaxed);
                        SWIMRequest::Ping(build_peer_state(cluster))
                    } else {
                        SWIMRequest::Join(cluster.swim_addr.port())
                    };

                    match bincode::serialize(&request) {
                        Ok(bytes) => {
                            if let Err(e) = socket.send_to(&bytes, &target_addr).await {
                                error!("Failed to send UDP packet to {}: {}", target_addr, e);
                            }
                        }
                        Err(e) => {
                            error!("Failed to serialize UDP packet: {}", e);
                        }
                    }
                }

                last_ping = Instant::now();
            }
        }
    });

    let socket = _socket;
    tokio::spawn(async move {
        let socket = socket.clone();
        let mut buf = vec![0; UDP_MAX_PAYLOAD];

        loop {
            //TODO encrypt packets
            match socket.recv_from(&mut buf).await {
                Ok((size, addr)) => {
                    if let Ok(message) = bincode::deserialize::<SWIMRequest>(&buf[..size]) {
                        if let Err(e) = tx.send((message, addr)).await {
                            error!("SWIM process ended, tx.send() failed: {}", e);
                            break;
                        }
                    } else {
                        debug!("Received invalid gossip message from {}", addr);
                    }
                }
                Err(e) => {
                    error!("SWIM process ended, socket.recv_from() failed: {}", e);
                    break;
                }
            }
        }
    });
}

fn handle_request(
    core: web::Data<JMAPServer<RocksDBStore>>,
    request: SWIMRequest,
    source_addr: SocketAddr,
) -> Option<(SocketAddr, SWIMRequest)> {
    //debug!("Received request {:?} from {}.", request, source_addr);

    // Increase epoch
    core.cluster
        .as_ref()
        .unwrap()
        .epoch
        .fetch_add(1, Ordering::Relaxed);

    match request {
        SWIMRequest::Join(reply_port) => Some((
            SocketAddr::from((source_addr.ip(), reply_port)),
            SWIMRequest::Synchronize(core.cluster.as_ref().unwrap().rpc_url.clone()),
        )),
        SWIMRequest::Synchronize(rpc_url) => {
            full_sync(core, rpc_url);
            None
        }
        SWIMRequest::Ping(peer_list) => {
            sync_peer_state(core, peer_list).map(|(core, target_addr)| {
                (
                    target_addr,
                    SWIMRequest::Pong(build_peer_state(core.cluster.as_ref().unwrap())),
                )
            })
        }
        SWIMRequest::Pong(peer_list) => {
            sync_peer_state(core, peer_list);
            None
        }
    }
}

#[post("/api/swim")]
async fn swim_http_sync(
    request: web::Bytes,
    core: web::Data<JMAPServer<RocksDBStore>>,
) -> Result<web::Bytes, JMAPServerError> {
    let cluster = core
        .cluster
        .as_ref()
        .ok_or_else(|| JMAPServerError::from("Cluster not configured."))?;

    let request = bincode::deserialize::<SWIMSync>(&request).map_err(|e| {
        JMAPServerError::from(format!(
            "Failed to deserialize SWIM sync request: {}",
            e.to_string()
        ))
    })?;

    if request.key != cluster.key {
        debug!("Received SWIM sync with invalid key: {}", request.key);
        return Err(JMAPServerError::from("Invalid cluster key."));
    }

    //debug!("Full sync request: {:?}", request);

    sync_peer_info(
        cluster,
        request.peers,
        cluster
            .peers
            .read()
            .map(|peers| {
                peers
                    .iter()
                    .any(|peer| matches!(peer.status, SWIMStatus::Seed))
            })
            .unwrap_or(false),
    );

    Ok(bincode::serialize(&build_peer_info(cluster))
        .map_err(|e| {
            JMAPServerError::from(format!(
                "Failed to serialize SWIM sync request: {}",
                e.to_string()
            ))
        })?
        .into())
}

fn full_sync(core: web::Data<JMAPServer<RocksDBStore>>, rpc_url: String) {
    let cluster = core.cluster.as_ref().unwrap();
    if !cluster.full_sync_active.load(Ordering::Relaxed) {
        cluster.full_sync_active.store(true, Ordering::Relaxed);
        tokio::spawn(async move {
            let cluster = core.cluster.as_ref().unwrap();
            let rpc_url = format!("{}/api/swim", rpc_url);

            match reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .connect_timeout(Duration::from_secs(5))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new())
                .post(&rpc_url)
                .body(bincode::serialize(&SWIMSync::from(cluster)).unwrap_or_else(|_| Vec::new()))
                .send()
                .await
            {
                Ok(response) => match response.bytes().await {
                    Ok(bytes) => match bincode::deserialize::<Vec<SWIMPeerInfo>>(&bytes) {
                        Ok(peer_info) => {
                            sync_peer_info(cluster, peer_info, false);
                            debug!("Successful full sync with {}.", rpc_url);
                        }
                        Err(err) => {
                            error!(
                                "Failed to deserialize SWIM sync response from {}: {}",
                                rpc_url, err
                            );
                        }
                    },
                    Err(err) => {
                        error!(
                            "Failed to process SWIM sync request to {}: {}",
                            rpc_url, err
                        );
                    }
                },
                Err(err) => {
                    error!("Failed to post SWIM sync request to {}: {}", rpc_url, err);
                }
            }
            cluster.full_sync_active.store(false, Ordering::Relaxed);
        });
    } else {
        debug!("Full sync already in progress.");
    }
}

fn sync_peer_state(
    core: web::Data<JMAPServer<RocksDBStore>>,
    peers: Vec<SWIMPeerState>,
) -> Option<(web::Data<JMAPServer<RocksDBStore>>, SocketAddr)> {
    if peers.is_empty() {
        debug!("Received empty SWIM sync packet.");
        return None;
    }
    let cluster = core.cluster.as_ref().unwrap();
    let mut local_peers = cluster.peers.write().ok()?;
    let mut source_peer_idx = None;

    let mut do_full_sync = local_peers.len() + 1 != peers.len();
    'outer: for (pos, peer) in peers.iter().enumerate() {
        if cluster.peer_id != peer.peer_id {
            for (idx, mut local_peer) in local_peers.iter_mut().enumerate() {
                if local_peer.peer_id == peer.peer_id {
                    if peer.epoch > local_peer.epoch {
                        local_peer.epoch = peer.epoch;
                        update_heartbeat(&mut local_peer);

                        if local_peer.generation != peer.generation && !do_full_sync {
                            do_full_sync = true;
                        }
                    }
                    // Keep idx of first item, the source peer.
                    if pos == 0 {
                        source_peer_idx = idx.into();
                    }
                    continue 'outer;
                }
            }
            if !do_full_sync {
                do_full_sync = true;
            }
        }
    }

    if let Some(source_peer_idx) = source_peer_idx {
        if do_full_sync {
            let rpc_url = local_peers[source_peer_idx].rpc_url.clone();
            drop(local_peers);
            full_sync(core, rpc_url);
            None
        } else {
            let swim_addr = local_peers[source_peer_idx].swim_addr;
            drop(local_peers);
            Some((core, swim_addr))
        }
    } else {
        debug!(
            "Received SWIM sync packet from unknown peer: {}",
            peers.first().unwrap().peer_id
        );
        None
    }
}

fn sync_peer_info(cluster: &JMAPCluster, peers: Vec<SWIMPeerInfo>, mut remove_seeds: bool) {
    if let Ok(mut local_peers) = cluster.peers.write() {
        'outer: for (pos, peer) in peers.into_iter().enumerate() {
            if peer.peer_id != cluster.peer_id {
                if remove_seeds {
                    local_peers.clear();
                    remove_seeds = false;
                }

                for local_peer in local_peers.iter_mut() {
                    if local_peer.peer_id == peer.peer_id {
                        let mut update_peer_info =
                            pos == 0 && local_peer.generation != peer.generation;

                        if peer.epoch > local_peer.epoch {
                            if !update_peer_info && local_peer.generation != peer.generation {
                                update_peer_info = true;
                            }

                            local_peer.epoch = peer.epoch;
                            update_heartbeat(local_peer);
                        }

                        // Update peer info if generationId has changed and
                        // the request comes from the peer itself, or if the epoch is higher.
                        if update_peer_info {
                            local_peer.generation = peer.generation;
                            local_peer.swim_addr = peer.swim_addr;
                            local_peer.shard_id = peer.shard_id;
                            local_peer.rpc_url = peer.rpc_url;
                            local_peer.jmap_url = peer.jmap_url;
                        }

                        continue 'outer;
                    }
                }

                // Peer not found, add it to the list.
                info!(
                    "Adding new peer {}, shard {} listening at {}.",
                    peer.peer_id, peer.shard_id, peer.swim_addr
                );
                local_peers.push(peer.into());
            } else if peer.epoch > cluster.epoch.load(Ordering::Relaxed) {
                info!("Updating local epoch to {}", peer.epoch);
                cluster.epoch.store(peer.epoch + 1, Ordering::Relaxed);
            }
        }
    } else {
        error!("Failed to acquire cluster.peers write lock.");
    }
}

fn build_peer_state(cluster: &JMAPCluster) -> Vec<SWIMPeerState> {
    if let Ok(local_peers) = cluster.peers.read() {
        let mut result: Vec<SWIMPeerState> = Vec::with_capacity(local_peers.len() + 1);
        result.push(cluster.into());
        for peer in local_peers.iter() {
            result.push(peer.into());
        }
        result
    } else {
        error!("Failed to acquire cluster.peers read lock.");
        vec![]
    }
}

fn build_peer_info(cluster: &JMAPCluster) -> Vec<SWIMPeerInfo> {
    if let Ok(local_peers) = cluster.peers.read() {
        let mut result: Vec<SWIMPeerInfo> = Vec::with_capacity(local_peers.len() + 1);
        result.push(cluster.into());
        for peer in local_peers.iter() {
            if !matches!(peer.status, SWIMStatus::Seed) {
                result.push(peer.into());
            }
        }
        result
    } else {
        error!("Failed to acquire cluster.peers read lock.");
        vec![]
    }
}

fn update_heartbeat(peer: &mut JMAPPeer) {
    let now = Instant::now();
    if !matches!(peer.status, SWIMStatus::Alive) {
        peer.status = SWIMStatus::Alive;
    }
    let hb_diff = now.duration_since(peer.last_heartbeat).as_millis() as u32;
    peer.hb_window_pos = (peer.hb_window_pos + 1) & HEARTBEAT_WINDOW_MASK;

    if !peer.hb_is_full && peer.hb_window_pos == 0 && peer.hb_sum > 0 {
        peer.hb_is_full = true;
    }

    if peer.hb_is_full {
        peer.hb_sum -= peer.hb_window[peer.hb_window_pos];
        peer.hb_sq_sum -= u32::pow(peer.hb_window[peer.hb_window_pos], 2);
    }

    peer.hb_window[peer.hb_window_pos] = hb_diff;
    peer.hb_sum += hb_diff;
    peer.hb_sq_sum += u32::pow(hb_diff, 2);

    peer.last_heartbeat = now;
}

/*
   Phi Accrual Failure Detection
   Ported from https://github.com/akka/akka/blob/main/akka-remote/src/main/scala/akka/remote/PhiAccrualFailureDetector.scala
*/
fn check_heartbeat(peer: &mut JMAPPeer) {
    if peer.hb_sum == 0 {
        return;
    }

    let hb_diff = Instant::now()
        .duration_since(peer.last_heartbeat)
        .as_millis() as f64;
    let sample_size = if peer.hb_is_full {
        HEARTBEAT_WINDOW
    } else {
        peer.hb_window_pos + 1
    } as f64;
    let hb_mean = (peer.hb_sum as f64 / sample_size) + HB_MAX_PAUSE_MS;
    let hb_variance = (peer.hb_sq_sum as f64 / sample_size) - (hb_mean * hb_mean);
    let hb_std_dev = hb_variance.sqrt();
    let y = (hb_diff - hb_mean) / hb_std_dev.max(HB_MIN_STD_DEV);
    let e = (-y * (1.5976 + 0.070566 * y * y)).exp();
    let phi = if hb_diff > hb_mean {
        -(e / (1.0 + e)).log10()
    } else {
        -(1.0 - 1.0 / (1.0 + e)).log10()
    };

    if phi > HB_PHI_CONVICT_THRESHOLD {
        peer.status = SWIMStatus::Offline;
    } else if phi > HB_PHI_SUSPECT_THRESHOLD {
        peer.status = SWIMStatus::Suspected;
    }

    debug!(
        "Heartbeat[{}]: mean={:.2}ms, variance={:.2}ms, std_dev={:.2}ms, phi={:.2}, status={:?}",
        peer.peer_id, hb_mean, hb_variance, hb_std_dev, phi, peer.status
    );
}
