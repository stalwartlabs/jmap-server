use std::{net::SocketAddr, sync::Arc, time::Instant};

use actix_web::web::{self};

use serde::{Deserialize, Serialize};
use store::Store;
use tokio::{net::UdpSocket, sync::mpsc};
use tracing::{debug, error, info};

use crate::{cluster::rpc::new_rpc_channel, JMAPServer};

use super::{
    raft::{LogIndex, TermId},
    rpc, Cluster, EpochId, GenerationId, Message, Peer, PeerId, ShardId, HEARTBEAT_WINDOW,
    HEARTBEAT_WINDOW_MASK, IPC_CHANNEL_BUFFER,
};

pub const PING_INTERVAL: u64 = 500;
const UDP_MAX_PAYLOAD: usize = 65500;

// Phi Accrual Failure Detector defaults
const HB_MAX_PAUSE_MS: f64 = 0.0;
const HB_MIN_STD_DEV: f64 = 300.0;
const HB_PHI_SUSPECT_THRESHOLD: f64 = 5.0;
const HB_PHI_CONVICT_THRESHOLD: f64 = 9.0;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum State {
    Seed,
    Alive,
    Suspected,
    Leaving,
    Offline,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PeerStatus {
    pub peer_id: PeerId,
    pub epoch: EpochId,
    pub generation: GenerationId,
    pub last_log_term: TermId,
    pub last_log_index: LogIndex,
}

impl From<&Peer> for PeerStatus {
    fn from(peer: &Peer) -> Self {
        PeerStatus {
            peer_id: peer.peer_id,
            epoch: peer.epoch,
            generation: peer.generation,
            last_log_term: peer.last_log_term,
            last_log_index: peer.last_log_index,
        }
    }
}

impl From<&Cluster> for PeerStatus {
    fn from(cluster: &Cluster) -> Self {
        PeerStatus {
            peer_id: cluster.peer_id,
            epoch: cluster.epoch,
            generation: cluster.generation,
            last_log_term: cluster.last_log_term,
            last_log_index: cluster.last_log_index,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PeerInfo {
    pub peer_id: PeerId,
    pub shard_id: ShardId,
    pub epoch: EpochId,
    pub last_log_term: TermId,
    pub last_log_index: LogIndex,
    pub generation: GenerationId,
    pub addr: SocketAddr,
    pub jmap_url: String,
}

impl From<&Peer> for PeerInfo {
    fn from(peer: &Peer) -> Self {
        PeerInfo {
            peer_id: peer.peer_id,
            shard_id: peer.shard_id,
            epoch: peer.epoch,
            generation: peer.generation,
            addr: peer.addr,
            last_log_index: peer.last_log_index,
            last_log_term: peer.last_log_term,
            jmap_url: peer.jmap_url.clone(),
        }
    }
}

impl From<&Cluster> for PeerInfo {
    fn from(cluster: &Cluster) -> Self {
        PeerInfo {
            peer_id: cluster.peer_id,
            shard_id: cluster.shard_id,
            epoch: cluster.epoch,
            last_log_index: cluster.last_log_index,
            last_log_term: cluster.last_log_term,
            generation: cluster.generation,
            addr: cluster.addr,
            jmap_url: cluster.jmap_url.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Join(PeerInfo),
    Ping(Vec<PeerStatus>),
    Pong(Vec<PeerStatus>),
}

pub async fn start_gossip(
    bind_addr: SocketAddr,
) -> (
    mpsc::Receiver<(SocketAddr, Request)>,
    mpsc::Sender<(SocketAddr, Request)>,
) {
    let _socket = Arc::new(match UdpSocket::bind(bind_addr).await {
        Ok(socket) => socket,
        Err(e) => {
            error!("Failed to bind UDP socket on '{}': {}", bind_addr, e);
            std::process::exit(1);
        }
    });
    let (tx, gossip_rx) = mpsc::channel::<(SocketAddr, Request)>(IPC_CHANNEL_BUFFER);
    let (gossip_tx, mut rx) = mpsc::channel::<(SocketAddr, Request)>(IPC_CHANNEL_BUFFER);

    let socket = _socket.clone();
    tokio::spawn(async move {
        while let Some((target_addr, response)) = rx.recv().await {
            //debug!("Sending packet to {}: {:?}", target_addr, response);
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
    });

    let socket = _socket;
    tokio::spawn(async move {
        let socket = socket.clone();
        let mut buf = vec![0; UDP_MAX_PAYLOAD];

        loop {
            //TODO encrypt packets
            //TODO use leb128 serialization
            match socket.recv_from(&mut buf).await {
                Ok((size, addr)) => {
                    if let Ok(request) = bincode::deserialize::<Request>(&buf[..size]) {
                        //debug!("Received packet from {}: {:?}", addr, request);
                        if let Err(e) = tx.send((addr, request)).await {
                            error!("Gossip process error, tx.send() failed: {}", e);
                        }
                    } else {
                        debug!("Received invalid gossip message from {}", addr);
                    }
                }
                Err(e) => {
                    error!("Gossip process ended, socket.recv_from() failed: {}", e);
                }
            }
        }
    });

    (gossip_rx, gossip_tx)
}

pub async fn handle_gossip<T>(core: &web::Data<JMAPServer<T>>, request: Request) -> Message
where
    T: for<'x> Store<'x> + 'static,
{
    match request {
        // Join request, add node and perform full sync.
        Request::Join(peer) => {
            let peer_id = peer.peer_id;
            let peers = sync_peer_info(core, vec![peer], true).unwrap();
            core.cluster
                .lock()
                .get_peer(peer_id)
                .map(|p| {
                    Message::new_rpc(p.rpc_channel.clone(), rpc::Request::SynchronizePeers(peers))
                })
                .unwrap_or(Message::None)
        }

        // Hearbeat request, reply with the cluster status.
        Request::Ping(peer_list) => handle_ping(core, peer_list, true).await,

        // Heartbeat response, update the cluster status if needed.
        Request::Pong(peer_list) => handle_ping(core, peer_list, false).await,
    }
}

pub async fn handle_ping<T>(
    core: &web::Data<JMAPServer<T>>,
    peers: Vec<PeerStatus>,
    is_ping: bool,
) -> Message
where
    T: for<'x> Store<'x> + 'static,
{
    if peers.is_empty() {
        debug!("Received empty peers sync packet.");
        return Message::None;
    }

    let mut cluster = core.cluster.lock();
    let mut source_peer_idx = None;

    // Increase epoch
    cluster.epoch += 1;

    let mut do_full_sync = cluster.peers.len() + 1 != peers.len();
    'outer: for (pos, peer) in peers.iter().enumerate() {
        if cluster.peer_id != peer.peer_id {
            for (idx, mut local_peer) in cluster.peers.iter_mut().enumerate() {
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
            Message::new_rpc(
                cluster.peers[source_peer_idx].rpc_channel.clone(),
                rpc::Request::SynchronizePeers(build_peer_info(&cluster)),
            )
        } else if is_ping {
            Message::new_gossip(
                cluster.peers[source_peer_idx].addr,
                Request::Pong(build_peer_status(&cluster)),
            )
        } else {
            Message::None
        }
    } else {
        debug!(
            "Received peers sync packet from unknown peer: {}",
            peers.first().unwrap().peer_id
        );
        Message::None
    }
}

pub fn sync_peer_info<T>(
    core: &web::Data<JMAPServer<T>>,
    peers: Vec<PeerInfo>,
    return_peers: bool,
) -> Option<Vec<PeerInfo>>
where
    T: for<'x> Store<'x> + 'static,
{
    let mut remove_seeds = false;

    let mut cluster = core.cluster.lock();

    'outer: for (pos, peer) in peers.into_iter().enumerate() {
        if peer.peer_id != cluster.peer_id {
            for local_peer in cluster.peers.iter_mut() {
                if !local_peer.is_seed() {
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
                            if local_peer.addr != peer.addr {
                                // Peer changed its address, reconnect.
                                local_peer.addr = peer.addr;
                                local_peer.rpc_channel =
                                    new_rpc_channel(core.clone(), peer.peer_id, peer.addr);
                            }
                            local_peer.generation = peer.generation;
                            local_peer.shard_id = peer.shard_id;
                            local_peer.jmap_url = format!("{}/jmap", peer.jmap_url);
                        }

                        continue 'outer;
                    }
                } else if !remove_seeds {
                    remove_seeds = true;
                }
            }

            // Peer not found, add it to the list.
            info!(
                "Adding new peer {}, shard {} listening at {}.",
                peer.peer_id, peer.shard_id, peer.addr
            );
            cluster.peers.push(Peer::new(core.clone(), peer));
        } else if peer.epoch > cluster.epoch {
            info!("Updating local epoch to {}", peer.epoch);
            cluster.epoch = peer.epoch + 1;
        }
    }

    if remove_seeds {
        cluster.peers.retain(|peer| !peer.is_seed());
    }

    if return_peers {
        Some(build_peer_info(&cluster))
    } else {
        None
    }
}

pub fn build_peer_status(cluster: &Cluster) -> Vec<PeerStatus> {
    let mut result: Vec<PeerStatus> = Vec::with_capacity(cluster.peers.len() + 1);
    result.push(cluster.into());
    for peer in cluster.peers.iter() {
        result.push(peer.into());
    }
    result
}

pub fn build_peer_info(cluster: &Cluster) -> Vec<PeerInfo> {
    let mut result: Vec<PeerInfo> = Vec::with_capacity(cluster.peers.len() + 1);
    result.push(cluster.into());
    for peer in cluster.peers.iter() {
        if !peer.is_seed() {
            result.push(peer.into());
        }
    }
    result
}

fn update_heartbeat(peer: &mut Peer) {
    if !peer.is_alive() {
        peer.state = State::Alive;
    }
    peer.hb_window_pos = (peer.hb_window_pos + 1) & HEARTBEAT_WINDOW_MASK;

    if !peer.hb_is_full && peer.hb_window_pos == 0 && peer.hb_sum > 0 {
        peer.hb_is_full = true;
    }

    if peer.hb_is_full {
        peer.hb_sum -= peer.hb_window[peer.hb_window_pos];
        peer.hb_sq_sum -= u32::pow(peer.hb_window[peer.hb_window_pos], 2);
    }

    let hb_diff = peer.last_heartbeat.elapsed().as_millis() as u32;
    peer.hb_window[peer.hb_window_pos] = hb_diff;
    peer.hb_sum += hb_diff;
    peer.hb_sq_sum += u32::pow(hb_diff, 2);

    peer.last_heartbeat = Instant::now();
}

/*
   Phi Accrual Failure Detection
   Ported from https://github.com/akka/akka/blob/main/akka-remote/src/main/scala/akka/remote/PhiAccrualFailureDetector.scala
*/
pub fn check_heartbeat(peer: &mut Peer) -> bool {
    if peer.hb_sum == 0 {
        return false;
    }

    let hb_diff = peer.last_heartbeat.elapsed().as_millis() as f64;
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

    /*debug!(
        "Heartbeat[{}]: mean={:.2}ms, variance={:.2}ms, std_dev={:.2}ms, phi={:.2}, status={:?}",
        peer.peer_id, hb_mean, hb_variance, hb_std_dev, phi, peer.state
    );*/

    if phi > HB_PHI_CONVICT_THRESHOLD {
        peer.state = State::Offline;
        false
    } else if phi > HB_PHI_SUSPECT_THRESHOLD {
        peer.state = State::Suspected;
        true
    } else {
        true
    }
}
