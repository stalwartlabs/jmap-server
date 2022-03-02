use std::{
    collections::hash_map::DefaultHasher,
    fs,
    hash::{Hash, Hasher},
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use actix_web::web;
use store::Store;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

use crate::{config::EnvSettings, JMAPServer, DEFAULT_HTTP_PORT, DEFAULT_RPC_PORT};

use self::{
    gossip::PeerInfo,
    raft::{LogIndex, TermId},
    rpc::start_peer_rpc,
};

pub mod gossip;
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
    pub tx: mpsc::Sender<Message>,
    pub gossip_tx: mpsc::Sender<(SocketAddr, gossip::Request)>,

    // Raft status
    pub term: TermId,
    pub last_log_index: LogIndex,
    pub last_log_term: TermId,
    pub commit_index: LogIndex,
    pub state: raft::State,
}

#[derive(Debug)]
pub enum Message {
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
}

pub struct Peer {
    pub peer_id: PeerId,
    pub shard_id: ShardId,
    pub tx: mpsc::Sender<rpc::RpcMessage>,

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
    pub hb_sum: u32,
    pub hb_sq_sum: u32,
    pub hb_is_full: bool,

    // Raft state
    pub last_log_index: LogIndex,
    pub last_log_term: TermId,
    pub vote_granted: bool,
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

    fn init(
        settings: &EnvSettings,
        core: web::Data<JMAPServer<T>>,
        tx: mpsc::Sender<Message>,
        gossip_tx: mpsc::Sender<(SocketAddr, gossip::Request)>,
    ) -> Option<Self> {
        let key = settings.get("cluster")?;

        // Obtain public addresses to advertise
        let advertise_addr = settings.parse_ipaddr("advertise-addr", "127.0.0.1");
        let rpc_port = settings.parse("rpc-port").unwrap_or(DEFAULT_RPC_PORT);
        let default_url = format!(
            "http://{}:{}",
            advertise_addr,
            settings.parse("http-port").unwrap_or(DEFAULT_HTTP_PORT)
        );

        // Obtain peer id from disk or generate a new one.
        let buf = PathBuf::from(
            settings
                .get("db-path")
                .unwrap_or_else(|| "stalwart-jmap".to_string()),
        );
        if !buf.exists() {
            if let Err(e) = std::fs::create_dir_all(&buf) {
                error!("Failed to create database directory: {}", e);
                std::process::exit(1);
            }
        }
        let mut peer_file = buf.clone();
        peer_file.push("peer_id");
        let peer_id = if peer_file.exists() {
            String::from_utf8(fs::read(peer_file).expect("Failed to read peer_id file."))
                .expect("Failed to parse peer_id file.")
                .parse()
                .expect("Failed to parse peer_id file.")
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

            fs::write(peer_file, peer_id.to_string().as_bytes())
                .expect("Failed to write peer_id file.");
            info!("Assigned peer_id '{}' to this node.", peer_id);
            peer_id
        };

        // Obtain shard id from disk or generate a new one.
        let mut shard_file = buf;
        shard_file.push("shard_id");
        let shard_id = if shard_file.exists() {
            String::from_utf8(fs::read(shard_file).expect("Failed to read shard_id file."))
                .expect("Failed to parse shard_id file.")
                .parse()
                .expect("Failed to parse shard_id file.")
        } else {
            let shard_id = settings.parse("shard-id").unwrap_or(0);
            fs::write(shard_file, shard_id.to_string().as_bytes())
                .expect("Failed to write shard_id file.");
            info!("Node will join shard id '{}'.", shard_id);
            shard_id
        };

        let peers = if let Some(seed_nodes) = settings.parse_list("seed-nodes") {
            let mut peers = Vec::with_capacity(seed_nodes.len());
            for (node_id, seed_node) in seed_nodes.into_iter().enumerate() {
                peers.push(Peer::new_seed(
                    tx.clone(),
                    peer_id,
                    key.clone(),
                    node_id as PeerId,
                    if !seed_node.contains(':') {
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
                    }),
                ));
            }
            peers
        } else {
            Vec::new()
        };

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

        Cluster {
            peer_id,
            shard_id,
            generation: generation.finish(),
            epoch: 0,
            core,
            addr,
            key,
            jmap_url: format!("{}/jmap", jmap_url),
            term: 0,
            last_log_index: 0,
            last_log_term: 0,
            commit_index: 0,
            state: raft::State::default(),
            peers,
            last_peer_pinged: u32::MAX as usize,
            tx,
            gossip_tx,
        }
        .into()
    }
}

impl Peer {
    pub fn new_seed(
        main_tx: mpsc::Sender<Message>,
        local_peer_id: PeerId,
        key: String,
        peer_id: PeerId,
        addr: SocketAddr,
    ) -> Self {
        Peer {
            peer_id,
            shard_id: 0,
            tx: start_peer_rpc(main_tx, local_peer_id, key, peer_id, addr),
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
            vote_granted: false,
        }
    }

    pub fn new<T>(cluster: &Cluster<T>, peer: PeerInfo) -> Self
    where
        T: for<'x> Store<'x> + 'static,
    {
        Peer {
            peer_id: peer.peer_id,
            shard_id: peer.shard_id,
            tx: start_peer_rpc(
                cluster.tx.clone(),
                cluster.peer_id,
                cluster.key.clone(),
                peer.peer_id,
                peer.addr,
            ),
            epoch: peer.epoch,
            generation: peer.generation,
            addr: peer.addr,
            jmap_url: peer.jmap_url,
            state: gossip::State::Alive,
            last_heartbeat: Instant::now(),
            hb_window: vec![0; HEARTBEAT_WINDOW],
            hb_window_pos: 0,
            hb_sum: 0,
            hb_sq_sum: 0,
            hb_is_full: false,
            last_log_index: 0,
            last_log_term: 0,
            vote_granted: false,
        }
    }

    pub fn is_seed(&self) -> bool {
        self.state == gossip::State::Seed
    }

    pub fn is_alive(&self) -> bool {
        self.state == gossip::State::Alive
    }

    pub fn is_healthy(&self) -> bool {
        matches!(self.state, gossip::State::Alive | gossip::State::Suspected)
    }

    pub fn is_offline(&self) -> bool {
        self.state == gossip::State::Offline
    }

    pub fn is_in_shard(&self, shard_id: ShardId) -> bool {
        self.shard_id == shard_id
    }
}
