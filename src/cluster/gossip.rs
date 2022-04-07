use std::{net::SocketAddr, sync::Arc, time::Instant};

use serde::{Deserialize, Serialize};
use store::log::{LogIndex, TermId};
use store::tracing::{debug, error, info};
use store::{leb128::Leb128, Store};
use tokio::sync::{oneshot, watch};
use tokio::{net::UdpSocket, sync::mpsc};

use crate::cluster::rpc::spawn_peer_rpc;

use super::{
    rpc, Cluster, EpochId, Event, GenerationId, Peer, PeerId, PeerList, ShardId, HEARTBEAT_WINDOW,
    HEARTBEAT_WINDOW_MASK,
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

impl<T> From<&Cluster<T>> for PeerStatus
where
    T: for<'x> Store<'x> + 'static,
{
    fn from(cluster: &Cluster<T>) -> Self {
        PeerStatus {
            peer_id: cluster.peer_id,
            epoch: cluster.epoch,
            generation: cluster.generation,
            last_log_term: cluster.last_log.term,
            last_log_index: cluster.last_log.index,
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

impl<T> From<&Cluster<T>> for PeerInfo
where
    T: for<'x> Store<'x> + 'static,
{
    fn from(cluster: &Cluster<T>) -> Self {
        PeerInfo {
            peer_id: cluster.peer_id,
            shard_id: cluster.shard_id,
            epoch: cluster.epoch,
            last_log_index: cluster.last_log.index,
            last_log_term: cluster.last_log.term,
            generation: cluster.generation,
            addr: cluster.addr,
            jmap_url: cluster.jmap_url.clone(),
        }
    }
}

#[derive(Debug)]
pub enum Request {
    Join { id: usize, port: u16 },
    JoinReply { id: usize },
    Ping(Vec<PeerStatus>),
    Pong(Vec<PeerStatus>),
}

impl Request {
    pub fn from_bytes(bytes: &[u8]) -> Option<Request> {
        let mut it = bytes.iter();
        match usize::from_leb128_it(&mut it)? {
            0 => Request::Join {
                id: usize::from_leb128_it(&mut it)?,
                port: u16::from_leb128_it(&mut it)?,
            },
            1 => Request::JoinReply {
                id: usize::from_leb128_it(&mut it)?,
            },
            mut num_peers => {
                num_peers -= 2;
                if num_peers > (UDP_MAX_PAYLOAD / std::mem::size_of::<PeerStatus>()) {
                    return None;
                }
                let mut peers = Vec::with_capacity(num_peers - 2);
                while num_peers > 0 {
                    let peer_id = if let Some(peer_id) = PeerId::from_leb128_it(&mut it) {
                        peer_id
                    } else {
                        break;
                    };
                    peers.push(PeerStatus {
                        peer_id,
                        epoch: EpochId::from_leb128_it(&mut it)?,
                        generation: GenerationId::from_leb128_it(&mut it)?,
                        last_log_term: TermId::from_leb128_it(&mut it)?,
                        last_log_index: LogIndex::from_leb128_it(&mut it)?,
                    });
                    num_peers -= 1;
                }
                match num_peers {
                    0 => Request::Ping(peers),
                    1 => Request::Pong(peers),
                    _ => return None,
                }
            }
        }
        .into()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let (flag, peers) = match self {
            Request::Join { id, port } => {
                let mut bytes = Vec::with_capacity(
                    std::mem::size_of::<usize>() + std::mem::size_of::<u16>() + 1,
                );
                bytes.push(0);
                id.to_leb128_bytes(&mut bytes);
                port.to_leb128_bytes(&mut bytes);
                return bytes;
            }
            Request::JoinReply { id } => {
                let mut bytes = Vec::with_capacity(std::mem::size_of::<usize>() + 1);
                bytes.push(1);
                id.to_leb128_bytes(&mut bytes);
                return bytes;
            }
            Request::Ping(peers) => (2, peers),
            Request::Pong(peers) => (3, peers),
        };

        debug_assert!(!peers.is_empty());

        let mut bytes = Vec::with_capacity(
            std::mem::size_of::<usize>() + (peers.len() * std::mem::size_of::<PeerStatus>()),
        );
        (flag + peers.len()).to_leb128_bytes(&mut bytes);

        for peer in peers {
            peer.peer_id.to_leb128_bytes(&mut bytes);
            peer.epoch.to_leb128_bytes(&mut bytes);
            peer.generation.to_leb128_bytes(&mut bytes);
            peer.last_log_term.to_leb128_bytes(&mut bytes);
            peer.last_log_index.to_leb128_bytes(&mut bytes);
        }

        bytes
    }
}

/*
  Quidnunc: an inquisitive and gossipy person, from Latin quid nunc? 'what now?'.
  Spawns the gossip process in charge of discovering peers and detecting failures.
*/
pub async fn spawn_quidnunc(
    bind_addr: SocketAddr,
    mut shutdown_rx: watch::Receiver<bool>,
    mut gossip_rx: mpsc::Receiver<(SocketAddr, Request)>,
    main_tx: mpsc::Sender<Event>,
) {
    let _socket = Arc::new(match UdpSocket::bind(bind_addr).await {
        Ok(socket) => socket,
        Err(e) => {
            error!("Failed to bind UDP socket on '{}': {}", bind_addr, e);
            std::process::exit(1);
        }
    });

    let socket = _socket.clone();
    tokio::spawn(async move {
        while let Some((target_addr, response)) = gossip_rx.recv().await {
            //debug!("Sending packet to {}: {:?}", target_addr, response);
            if let Err(e) = socket.send_to(&response.to_bytes(), &target_addr).await {
                error!("Failed to send UDP packet to {}: {}", target_addr, e);
            }
        }
    });

    let socket = _socket;
    tokio::spawn(async move {
        let mut buf = vec![0; UDP_MAX_PAYLOAD];

        loop {
            //TODO encrypt packets
            tokio::select! {
                packet = socket.recv_from(&mut buf) => {
                    match packet {
                        Ok((size, addr)) => {
                            if let Some(request) = Request::from_bytes(&buf[..size]) {
                                //debug!("Received packet from {}", addr);
                                if let Err(e) = main_tx.send(Event::Gossip { addr, request }).await {
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
                },
                _ = shutdown_rx.changed() => {
                    debug!("Gossip listener shutting down.");
                    break;
                }
            };
        }
    });
}

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn send_gossip(&self, dest: SocketAddr, request: Request) {
        if let Err(err) = self.gossip_tx.send((dest, request)).await {
            error!("Failed to send gossip message: {}", err);
        };
    }

    pub async fn handle_ping(&mut self, peers: Vec<PeerStatus>, send_pong: bool) {
        if peers.is_empty() {
            debug!("Received empty ping packet.");
        }

        let mut source_peer_idx = None;

        // Increase epoch
        self.epoch += 1;

        let mut do_full_sync = self.peers.len() + 1 != peers.len();

        'outer: for (pos, peer) in peers.iter().enumerate() {
            if self.peer_id != peer.peer_id {
                for (idx, mut local_peer) in self.peers.iter_mut().enumerate() {
                    if local_peer.peer_id == peer.peer_id {
                        if peer.epoch > local_peer.epoch {
                            local_peer.epoch = peer.epoch;
                            local_peer.last_log_index = peer.last_log_index;
                            local_peer.last_log_term = peer.last_log_term;
                            if local_peer.update_heartbeat() {
                                // This peer reconnected
                                if pos != 0 && local_peer.is_in_shard(self.shard_id) {
                                    // Wake up RPC process
                                    println!("Waking up RPC process");
                                    local_peer.dispatch_request(rpc::Request::Ping).await;
                                } else {
                                    do_full_sync = true;
                                }
                            }

                            if (local_peer.generation != peer.generation) && !do_full_sync {
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
                self.peers[source_peer_idx]
                    .dispatch_request(rpc::Request::UpdatePeers {
                        peers: self.build_peer_info(),
                    })
                    .await;
            } else if send_pong {
                self.send_gossip(
                    self.peers[source_peer_idx].addr,
                    Request::Pong(self.build_peer_status()),
                )
                .await;
            }
        } else {
            debug!(
                "Received peers sync packet from unknown peer id: {}",
                peers.first().unwrap().peer_id
            );
        }
    }

    pub async fn handle_join(&mut self, id: usize, mut dest: SocketAddr, port: u16) {
        dest.set_port(port);
        self.send_gossip(dest, Request::JoinReply { id }).await;
    }

    pub async fn handle_join_reply(&mut self, id: usize) {
        if let Some(peer) = self.peers.get(id) {
            if peer.is_seed() {
                peer.dispatch_request(rpc::Request::UpdatePeers {
                    peers: self.build_peer_info(),
                })
                .await;
            }
        }
    }

    pub async fn sync_peer_info(&mut self, peers: Vec<PeerInfo>) {
        let mut remove_seeds = false;
        let mut peers_changed = false;
        let is_leading = self.is_leading();

        'outer: for (pos, peer) in peers.into_iter().enumerate() {
            if peer.peer_id != self.peer_id {
                for local_peer in self.peers.iter_mut() {
                    if !local_peer.is_seed() {
                        if local_peer.peer_id == peer.peer_id {
                            let mut update_peer_info =
                                pos == 0 && local_peer.generation != peer.generation;

                            if peer.epoch > local_peer.epoch {
                                if !update_peer_info && local_peer.generation != peer.generation {
                                    update_peer_info = true;
                                }
                                local_peer.epoch = peer.epoch;
                                local_peer.last_log_index = peer.last_log_index;
                                local_peer.last_log_term = peer.last_log_term;
                                if local_peer.update_heartbeat()
                                    && local_peer.is_in_shard(self.shard_id)
                                {
                                    // Wake up RPC process
                                    local_peer.dispatch_request(rpc::Request::Ping).await;
                                }
                            }

                            // Update peer info if generationId has changed and
                            // the request comes from the peer itself, or if the epoch is higher.
                            if update_peer_info {
                                if local_peer.addr != peer.addr {
                                    // Peer changed its address, reconnect.
                                    let (tx, online_rx) = spawn_peer_rpc(
                                        self.tx.clone(),
                                        self.peer_id,
                                        self.key.clone(),
                                        peer.peer_id,
                                        peer.addr,
                                    );
                                    local_peer.addr = peer.addr;
                                    local_peer.tx = tx;
                                    local_peer.online_rx = online_rx;
                                }
                                local_peer.generation = peer.generation;
                                local_peer.shard_id = peer.shard_id;
                                local_peer.jmap_url = format!("{}/jmap", peer.jmap_url);
                                peers_changed = true;
                            }

                            continue 'outer;
                        }
                    } else if !remove_seeds {
                        remove_seeds = true;
                    }
                }

                // Add new peer to the list.
                info!(
                    "Discovered new peer {} (shard {}) listening at {}.",
                    peer.peer_id, peer.shard_id, peer.addr
                );
                let peer_id = peer.peer_id;
                let is_follower = is_leading && peer.shard_id == self.shard_id;
                self.peers.push(Peer::new(self, peer, State::Alive));
                if is_follower {
                    self.add_follower(peer_id);
                }
                if !peers_changed {
                    peers_changed = true;
                }
            } else if peer.epoch > self.epoch {
                debug!(
                    "This node was already part of the cluster, updating local epoch to {}",
                    peer.epoch
                );
                self.epoch = peer.epoch + 1;
            }
        }

        if remove_seeds {
            self.peers.retain(|peer| !peer.is_seed());
        }

        // Update store
        if peers_changed {
            self.core.queue_set_key(
                "peer_list",
                PeerList::from(
                    self.peers
                        .iter()
                        .map(|p| p.into())
                        .collect::<Vec<PeerInfo>>(),
                ),
            );
        }
    }

    pub fn build_peer_status(&self) -> Vec<PeerStatus> {
        let mut result: Vec<PeerStatus> = Vec::with_capacity(self.peers.len() + 1);
        result.push(self.into());
        for peer in self.peers.iter() {
            result.push(peer.into());
        }
        result
    }

    pub fn build_peer_info(&self) -> Vec<PeerInfo> {
        let mut result: Vec<PeerInfo> = Vec::with_capacity(self.peers.len() + 1);
        result.push(self.into());
        for peer in self.peers.iter() {
            if !peer.is_seed() {
                result.push(peer.into());
            }
        }
        result
    }

    pub async fn handle_update_peers(
        &mut self,
        response_tx: oneshot::Sender<rpc::Response>,
        peers: Vec<PeerInfo>,
    ) {
        self.sync_peer_info(peers).await;
        response_tx
            .send(rpc::Response::UpdatePeers {
                peers: self.build_peer_info(),
            })
            .unwrap_or_else(|_| error!("Oneshot response channel closed."));
    }
}

impl Peer {
    fn update_heartbeat(&mut self) -> bool {
        let hb_diff =
            std::cmp::min(self.last_heartbeat.elapsed().as_millis(), 60 * 60 * 1000) as u64;
        self.last_heartbeat = Instant::now();

        match self.state {
            State::Seed | State::Offline | State::Leaving => {
                debug!("Peer {} is back online.", self.addr);
                self.state = State::Alive;

                // Do not count stale heartbeats.
                return true;
            }
            State::Suspected => {
                debug!("Suspected peer {} was confirmed alive.", self.addr);
                self.state = State::Alive;
            }
            State::Alive => (),
        }

        self.hb_window_pos = (self.hb_window_pos + 1) & HEARTBEAT_WINDOW_MASK;

        if !self.hb_is_full && self.hb_window_pos == 0 && self.hb_sum > 0 {
            self.hb_is_full = true;
        }

        if self.hb_is_full {
            let hb_window = self.hb_window[self.hb_window_pos] as u64;
            self.hb_sum -= hb_window;
            self.hb_sq_sum -= hb_window.saturating_mul(hb_window);
        }

        self.hb_window[self.hb_window_pos] = hb_diff as u32;
        self.hb_sum += hb_diff;
        self.hb_sq_sum += hb_diff.saturating_mul(hb_diff);

        false
    }

    /*
       Phi Accrual Failure Detection
       Ported from https://github.com/akka/akka/blob/main/akka-remote/src/main/scala/akka/remote/PhiAccrualFailureDetector.scala
    */
    pub fn check_heartbeat(&mut self) -> bool {
        if self.hb_sum == 0 {
            println!("No pings received from {}.", self.addr);
            return false;
        }

        let hb_diff = self.last_heartbeat.elapsed().as_millis() as f64;
        let sample_size = if self.hb_is_full {
            HEARTBEAT_WINDOW
        } else {
            self.hb_window_pos + 1
        } as f64;
        let hb_mean = (self.hb_sum as f64 / sample_size) + HB_MAX_PAUSE_MS;
        let hb_variance = (self.hb_sq_sum as f64 / sample_size) - (hb_mean * hb_mean);
        let hb_std_dev = hb_variance.sqrt();
        let y = (hb_diff - hb_mean) / hb_std_dev.max(HB_MIN_STD_DEV);
        let e = (-y * (1.5976 + 0.070566 * y * y)).exp();
        let phi = if hb_diff > hb_mean {
            -(e / (1.0 + e)).log10()
        } else {
            -(1.0 - 1.0 / (1.0 + e)).log10()
        };

        /*debug!(
            "Heartbeat from {}: mean={:.2}ms, variance={:.2}ms, std_dev={:.2}ms, phi={:.2}, samples={}, status={:?}",
            self.addr, hb_mean, hb_variance, hb_std_dev, phi, sample_size, if phi > HB_PHI_CONVICT_THRESHOLD {
                State::Offline
            } else if phi > HB_PHI_SUSPECT_THRESHOLD {
                State::Suspected
            } else {
                State::Alive
            }
        );*/

        if phi > HB_PHI_CONVICT_THRESHOLD {
            debug!("Peer {} is offline.", self.addr);
            self.state = State::Offline;
            false
        } else if phi > HB_PHI_SUSPECT_THRESHOLD {
            self.state = State::Suspected;
            true
        } else {
            true
        }
    }
}
