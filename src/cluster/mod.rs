use std::{
    collections::hash_map::DefaultHasher,
    fs,
    hash::{Hash, Hasher},
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    sync::Mutex,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use tracing::{error, info};

use crate::{config::EnvSettings, DEFAULT_HTTP_PORT};

use self::{
    gossip::{PeerInfo, PeerStatus, DEFAULT_GOSSIP_PORT},
    raft::{LogIndex, TermId},
};

pub mod gossip;
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

pub struct Cluster {
    // Local node peer and shard id
    pub peer_id: PeerId,
    pub shard_id: ShardId,

    // Gossip state
    pub generation: GenerationId,
    pub epoch: EpochId,

    // Local gossip address and API urls
    pub gossip_addr: SocketAddr,
    pub jmap_url: String,
    pub rpc_url: String,

    // Cluster key
    pub key: String,

    // Peer list
    pub peers: Vec<Peer>,

    // Raft status
    pub term: TermId,
    pub last_log_index: LogIndex,
    pub last_log_term: TermId,
    pub commit_index: LogIndex,
    pub state: raft::State,
}

pub enum Message {
    SyncRequest {
        addr: SocketAddr,
        url: String,
    },
    SyncResponse {
        url: String,
        peers: Vec<PeerInfo>,
    },
    VoteRequest {
        urls: Vec<String>,
        term: TermId,
        last_log_index: LogIndex,
        last_log_term: TermId,
    },
    QueryLeader {
        urls: Vec<String>,
    },
    Ping {
        addr: SocketAddr,
        peers: Vec<PeerStatus>,
    },
    Pong {
        addr: SocketAddr,
        peers: Vec<PeerStatus>,
    },
    Join {
        addr: SocketAddr,
        port: u16,
    },
    None,
}

impl Cluster {
    pub fn is_enabled(&self) -> bool {
        !self.key.is_empty()
    }

    pub fn quorum(&self) -> u32 {
        ((self
            .peers
            .iter()
            .filter(|p| p.shard_id == self.shard_id)
            .count() as f64
            + 1.0)
            / 2.0)
            .floor() as u32
    }
}

pub struct Peer {
    pub peer_id: PeerId,
    pub shard_id: ShardId,

    // Peer status
    pub epoch: EpochId,
    pub generation: GenerationId,
    pub state: gossip::State,

    // Peer addresses
    pub gossip_addr: SocketAddr,
    pub rpc_url: String,
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
}

impl Default for Cluster {
    fn default() -> Self {
        Cluster {
            peer_id: 0,
            shard_id: 0,
            generation: 0,
            epoch: 0,
            key: String::new(),
            gossip_addr: SocketAddr::new(
                std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
                DEFAULT_GOSSIP_PORT,
            ),
            jmap_url: String::new(),
            rpc_url: String::new(),
            peers: Vec::new(),
            term: 0,
            last_log_index: 0,
            last_log_term: 0,
            commit_index: 0,
            state: raft::State::None,
        }
    }
}

impl Peer {
    pub fn new_seed(peer_id: PeerId, gossip_addr: SocketAddr) -> Self {
        Peer {
            peer_id,
            shard_id: 0,
            epoch: 0,
            generation: 0,
            gossip_addr,
            state: gossip::State::Seed,
            rpc_url: "".to_string(),
            jmap_url: "".to_string(),
            last_heartbeat: Instant::now(),
            hb_window: vec![0; HEARTBEAT_WINDOW],
            hb_window_pos: 0,
            hb_sum: 0,
            hb_sq_sum: 0,
            hb_is_full: false,
            last_log_index: 0,
            last_log_term: 0,
        }
    }
}

impl From<&EnvSettings> for Mutex<Cluster> {
    fn from(settings: &EnvSettings) -> Self {
        let key = if let Some(key) = settings.get("cluster") {
            key
        } else {
            return Cluster::default().into();
        };

        // Obtain public addresses to advertise
        let advertise_addr = settings.parse_ipaddr("advertise-addr", "127.0.0.1");
        let gossip_port = settings.parse("gossip-port").unwrap_or(DEFAULT_GOSSIP_PORT);
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
                    node_id as PeerId,
                    if !seed_node.contains(':') {
                        format!("{}:{}", seed_node, gossip_port)
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
        let gossip_addr = SocketAddr::from((advertise_addr, gossip_port));
        let rpc_url = settings.parse("rpc-url").unwrap_or_else(|| {
            info!(
                "Warning: Parameter 'rpc-url' not specified, using default '{}'.",
                default_url
            );
            default_url.clone()
        });
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
        gossip_addr.hash(&mut generation);
        rpc_url.hash(&mut generation);
        jmap_url.hash(&mut generation);

        Cluster {
            peer_id,
            shard_id,
            generation: generation.finish(),
            epoch: 0,
            gossip_addr,
            key,
            rpc_url,
            jmap_url,
            term: 0,
            last_log_index: 0,
            last_log_term: 0,
            commit_index: 0,
            state: raft::State::None,
            peers,
        }
        .into()
    }
}
