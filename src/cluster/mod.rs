use std::{
    collections::hash_map::DefaultHasher,
    fmt::Display,
    hash::{Hash, Hasher},
    net::{SocketAddr, ToSocketAddrs},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use actix_web::web;
use serde::{Deserialize, Serialize};
use store::{
    bincode,
    log::{LogIndex, RaftId, TermId},
    serialize::{StoreDeserialize, StoreSerialize},
    Store,
};
use store::{
    config::EnvSettings,
    tracing::{error, info},
};
use tokio::sync::{mpsc, oneshot, watch};

use crate::{JMAPServer, DEFAULT_HTTP_PORT, DEFAULT_RPC_PORT};

use self::{gossip::PeerInfo, rpc::spawn_peer_rpc};

pub mod follower;
pub mod gossip;
pub mod leader;
pub mod log;
pub mod main;
pub mod raft;
pub mod rpc;

pub type PeerId = u64;
pub type ShardId = u32;
pub type EpochId = u64;
pub type GenerationId = u64;

pub const IPC_CHANNEL_BUFFER: usize = 1024;
const HEARTBEAT_WINDOW: usize = 1 << 10;
const HEARTBEAT_WINDOW_MASK: usize = HEARTBEAT_WINDOW - 1;

pub struct Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    // Local node peer and shard id
    pub peer_id: PeerId,
    pub shard_id: ShardId,

    // Gossip state
    pub generation: GenerationId,
    pub epoch: EpochId,

    // Local gossip address and API urls
    pub addr: SocketAddr,
    pub jmap_url: String,

    // Cluster key
    pub key: String,

    // Peer list
    pub peers: Vec<Peer>,
    pub last_peer_pinged: usize,

    // IPC
    pub core: web::Data<JMAPServer<T>>,
    pub tx: mpsc::Sender<Event>,
    pub gossip_tx: mpsc::Sender<(SocketAddr, gossip::Request)>,

    // Raft status
    pub term: TermId,
    pub last_log: RaftId,
    pub uncommitted_index: LogIndex,
    pub state: raft::State,
}

#[derive(Debug)]
pub enum Event {
    Gossip {
        addr: SocketAddr,
        request: gossip::Request,
    },
    RpcRequest {
        peer_id: PeerId,
        request: rpc::Request,
        response_tx: oneshot::Sender<rpc::Response>,
    },
    RpcResponse {
        peer_id: PeerId,
        response: rpc::Response,
    },
    StepDown {
        term: TermId,
    },
    StoreChanged {
        last_log: RaftId,
    },
    AdvanceCommitIndex {
        peer_id: PeerId,
        commit_index: LogIndex,
    },
    Shutdown,

    #[cfg(test)]
    IsOffline(bool),
}

#[derive(Debug)]
pub struct Peer {
    pub peer_id: PeerId,
    pub shard_id: ShardId,
    pub tx: mpsc::Sender<rpc::RpcEvent>,
    pub online_rx: watch::Receiver<bool>,

    // Peer status
    pub epoch: EpochId,
    pub generation: GenerationId,
    pub state: gossip::State,

    // Peer addresses
    pub addr: SocketAddr,
    pub jmap_url: String,

    // Heartbeat state
    pub last_heartbeat: Instant,
    pub hb_window: Vec<u32>,
    pub hb_window_pos: usize,
    pub hb_sum: u64,
    pub hb_sq_sum: u64,
    pub hb_is_full: bool,

    // Raft state
    pub last_log_index: LogIndex,
    pub last_log_term: TermId,
    pub commit_index: LogIndex,
    pub vote_granted: bool,
}

impl Display for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)
    }
}

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn is_enabled(&self) -> bool {
        !self.key.is_empty()
    }

    pub fn shard_status(&self) -> (u32, u32) {
        let mut total = 0;
        let mut healthy = 0;
        for peer in &self.peers {
            if peer.is_in_shard(self.shard_id) {
                if peer.is_healthy() {
                    healthy += 1;
                }
                total += 1;
            }
        }
        (total, healthy)
    }

    pub fn is_peer_healthy(&self, peer_id: PeerId) -> bool {
        self.peers
            .iter()
            .any(|p| p.peer_id == peer_id && p.is_healthy())
    }

    pub fn get_peer(&self, peer_id: PeerId) -> Option<&Peer> {
        self.peers.iter().find(|p| p.peer_id == peer_id)
    }

    pub fn is_known_peer(&self, peer_id: PeerId) -> bool {
        self.peers.iter().any(|p| p.peer_id == peer_id)
    }

    pub fn get_peer_mut(&mut self, peer_id: PeerId) -> Option<&mut Peer> {
        self.peers.iter_mut().find(|p| p.peer_id == peer_id)
    }

    async fn init(
        settings: &EnvSettings,
        core: web::Data<JMAPServer<T>>,
        tx: mpsc::Sender<Event>,
        gossip_tx: mpsc::Sender<(SocketAddr, gossip::Request)>,
    ) -> Self {
        let key = settings.get("cluster").unwrap();

        // Obtain public addresses to advertise
        let advertise_addr = settings.parse_ipaddr("advertise-addr", "127.0.0.1");
        let rpc_port = settings.parse("rpc-port").unwrap_or(DEFAULT_RPC_PORT);
        let default_url = format!(
            "http://{}:{}",
            advertise_addr,
            settings.parse("http-port").unwrap_or(DEFAULT_HTTP_PORT)
        );

        // Obtain peer id from disk or generate a new one.
        let peer_id = if let Some(peer_id) = core.get_key("peer_id").await.unwrap() {
            peer_id
        } else {
            // Generate peerId for this node.
            let mut s = DefaultHasher::new();
            gethostname::gethostname().hash(&mut s);
            thread::current().id().hash(&mut s);
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|_| Duration::new(0, 0))
                .as_nanos()
                .hash(&mut s);

            let peer_id = s.finish();
            core.set_key("peer_id", peer_id).await.unwrap();
            peer_id
        };

        // Obtain shard id from disk or generate a new one.
        let shard_id = if let Some(shard_id) = core.get_key("shard_id").await.unwrap() {
            shard_id
        } else {
            let shard_id = settings.parse("shard-id").unwrap_or(0);
            core.set_key("shard_id", shard_id).await.unwrap();
            shard_id
        };
        info!(
            "This node will join shard '{}' with id '{}'.",
            shard_id, peer_id
        );

        // Create advertise addresses
        let addr = SocketAddr::from((advertise_addr, rpc_port));
        let jmap_url = settings.parse("jmap-url").unwrap_or_else(|| {
            info!(
                "Warning: Parameter 'jmap-url' not specified, using default '{}'.",
                default_url
            );
            default_url.clone()
        });

        // Calculate generationId
        let mut generation = DefaultHasher::new();
        peer_id.hash(&mut generation);
        shard_id.hash(&mut generation);
        addr.hash(&mut generation);
        jmap_url.hash(&mut generation);

        // Rollback uncommitted entries for a previous leader term.
        core.commit_leader(LogIndex::MAX, true).await.unwrap();

        // Apply committed updates and rollback uncommited ones for
        // a previous follower term.
        core.commit_follower(LogIndex::MAX, true).await.unwrap();

        let last_log = core
            .get_last_log()
            .await
            .unwrap()
            .unwrap_or_else(RaftId::none);
        let mut cluster = Cluster {
            peer_id,
            shard_id,
            generation: generation.finish(),
            epoch: 0,
            addr,
            key,
            jmap_url: format!("{}/jmap", jmap_url),
            term: last_log.term,
            uncommitted_index: last_log.index,
            last_log,
            state: raft::State::default(),
            core,
            peers: vec![],
            last_peer_pinged: u32::MAX as usize,
            tx,
            gossip_tx,
        };

        // Add previously discovered peers
        if let Some(peer_list) = cluster.core.get_key::<PeerList>("peer_list").await.unwrap() {
            for peer in peer_list.peers {
                cluster
                    .peers
                    .push(Peer::new(&cluster, peer, gossip::State::Offline));
            }
        };

        // Add any seed nodes
        if let Some(seed_nodes) = settings.parse_list("seed-nodes") {
            for (node_id, seed_node) in seed_nodes.into_iter().enumerate() {
                let peer_addr = if !seed_node.contains(':') {
                    format!("{}:{}", seed_node, rpc_port)
                } else {
                    seed_node.to_string()
                }
                .to_socket_addrs()
                .map_err(|e| {
                    error!("Failed to parse seed node '{}': {}", seed_node, e);
                    std::process::exit(1);
                })
                .unwrap()
                .next()
                .unwrap_or_else(|| {
                    error!("Failed to parse seed node '{}'.", seed_node);
                    std::process::exit(1);
                });

                if !cluster.peers.iter().any(|p| p.addr == peer_addr) {
                    info!("Adding seed node '{}'.", peer_addr);
                    cluster
                        .peers
                        .push(Peer::new_seed(&cluster, node_id as PeerId, peer_addr));
                }
            }
        }

        cluster
    }
}

impl Peer {
    pub fn new_seed<T>(cluster: &Cluster<T>, peer_id: PeerId, addr: SocketAddr) -> Self
    where
        T: for<'x> Store<'x> + 'static,
    {
        let (tx, online_rx) = spawn_peer_rpc(
            cluster.tx.clone(),
            cluster.peer_id,
            cluster.key.clone(),
            peer_id,
            addr,
        );
        Peer {
            peer_id,
            shard_id: 0,
            tx,
            online_rx,
            epoch: 0,
            generation: 0,
            addr,
            state: gossip::State::Seed,
            jmap_url: "".to_string(),
            last_heartbeat: Instant::now(),
            hb_window: vec![0; HEARTBEAT_WINDOW],
            hb_window_pos: 0,
            hb_sum: 0,
            hb_sq_sum: 0,
            hb_is_full: false,
            last_log_index: 0,
            last_log_term: 0,
            commit_index: 0,
            vote_granted: false,
        }
    }

    pub fn new<T>(cluster: &Cluster<T>, peer: PeerInfo, state: gossip::State) -> Self
    where
        T: for<'x> Store<'x> + 'static,
    {
        let (tx, online_rx) = spawn_peer_rpc(
            cluster.tx.clone(),
            cluster.peer_id,
            cluster.key.clone(),
            peer.peer_id,
            peer.addr,
        );
        Peer {
            peer_id: peer.peer_id,
            shard_id: peer.shard_id,
            tx,
            online_rx,
            epoch: peer.epoch,
            generation: peer.generation,
            addr: peer.addr,
            jmap_url: peer.jmap_url,
            state,
            last_heartbeat: Instant::now(),
            hb_window: vec![0; HEARTBEAT_WINDOW],
            hb_window_pos: 0,
            hb_sum: 0,
            hb_sq_sum: 0,
            hb_is_full: false,
            last_log_index: peer.last_log_index,
            last_log_term: peer.last_log_term,
            commit_index: peer.last_log_index,
            vote_granted: false,
        }
    }

    pub fn is_seed(&self) -> bool {
        self.state == gossip::State::Seed
    }

    pub fn is_alive(&self) -> bool {
        self.state == gossip::State::Alive
    }

    pub fn is_suspected(&self) -> bool {
        self.state == gossip::State::Suspected
    }

    pub fn is_healthy(&self) -> bool {
        matches!(self.state, gossip::State::Alive | gossip::State::Suspected)
    }

    pub fn is_offline(&self) -> bool {
        matches!(self.state, gossip::State::Offline | gossip::State::Left)
    }

    pub fn is_in_shard(&self, shard_id: ShardId) -> bool {
        self.shard_id == shard_id
    }
}

#[derive(Serialize, Deserialize)]
pub struct PeerList {
    peers: Vec<PeerInfo>,
}

impl From<Vec<PeerInfo>> for PeerList {
    fn from(peers: Vec<PeerInfo>) -> Self {
        Self { peers }
    }
}

impl StoreSerialize for PeerList {
    fn serialize(&self) -> Option<Vec<u8>> {
        bincode::serialize(self).ok()
    }
}

impl StoreDeserialize for PeerList {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }
}
