use self::gossip::PeerInfo;
use crate::JMAPServer;
use actix_web::web;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::atomic::AtomicU8, time::Instant};
use store::log::raft::{LogIndex, RaftId, TermId};
use store::{
    bincode,
    serialize::{StoreDeserialize, StoreSerialize},
    Store,
};
use tokio::sync::{mpsc, oneshot, watch};

pub mod follower;
pub mod gossip;
pub mod init;
pub mod leader;
pub mod log;
pub mod main;
pub mod peer;
pub mod raft;
pub mod rpc;

pub type PeerId = u64;
pub type ShardId = u32;
pub type EpochId = u64;
pub type GenerationId = u64;

pub const IPC_CHANNEL_BUFFER: usize = 1024;
const HEARTBEAT_WINDOW: usize = 1 << 10;
const HEARTBEAT_WINDOW_MASK: usize = HEARTBEAT_WINDOW - 1;

pub const RAFT_LOG_BEHIND: u8 = 0;
pub const RAFT_LOG_UPDATED: u8 = 1;
pub const RAFT_LOG_LEADER: u8 = 2;

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
    pub hostname: String,

    // Cluster key
    pub config: Config,

    // Peer list
    pub peers: Vec<Peer>,
    pub last_peer_pinged: usize,

    // IPC
    pub core: web::Data<JMAPServer<T>>,
    pub tx: mpsc::Sender<Event>,
    pub gossip_tx: mpsc::Sender<(SocketAddr, self::gossip::request::Request)>,
    pub commit_index_tx: watch::Sender<LogIndex>,

    // Raft status
    pub term: TermId,
    pub last_log: RaftId,
    pub uncommitted_index: LogIndex,
    pub state: raft::State,
}

pub struct Config {
    pub key: String,
    pub raft_batch_max: usize,       // 10 * 1024 * 1024
    pub raft_election_timeout: u64,  // 1000
    pub rpc_frame_max: usize,        // 50 * 1024 * 1024
    pub rpc_inactivity_timeout: u64, // 5 * 60 * 1000
    pub rpc_timeout: u64,            // 1000
    pub rpc_retries_max: u32,        // 5
    pub rpc_backoff_max: u64,        // 3 * 60 * 1000 (1 minute)
}

#[derive(Debug)]
pub enum Event {
    Gossip {
        addr: SocketAddr,
        request: self::gossip::request::Request,
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
    UpdateLastLog {
        last_log: RaftId,
    },
    AdvanceUncommittedIndex {
        uncommitted_index: LogIndex,
    },
    AdvanceCommitIndex {
        peer_id: PeerId,
        commit_index: LogIndex,
    },
    Shutdown,

    #[cfg(test)]
    SetOffline {
        is_offline: bool,
        notify_peers: bool,
    },
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
    pub hostname: String,

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

pub struct ClusterIpc {
    pub tx: mpsc::Sender<Event>,
    pub state: AtomicU8,
    pub leader_hostname: store::parking_lot::Mutex<Option<String>>,
    pub commit_index_rx: watch::Receiver<LogIndex>,
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
