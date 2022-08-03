pub mod commit;
pub mod election;
pub mod follower;
pub mod leader;
pub mod log;
pub mod vote;

use self::election::election_timeout;
use super::{rpc, RAFT_LOG_BEHIND, RAFT_LOG_LEADER, RAFT_LOG_UPDATED};
use super::{Cluster, Peer, PeerId};
use std::time::Instant;
use tokio::sync::{mpsc, watch};

pub const COMMIT_TIMEOUT_MS: u64 = 1000;

#[derive(Debug)]
pub enum State {
    Leader {
        tx: watch::Sender<crate::cluster::leader::Event>,
        rx: watch::Receiver<crate::cluster::leader::Event>,
    },
    Wait {
        election_due: Instant,
    },
    Candidate {
        election_due: Instant,
    },
    VotedFor {
        peer_id: PeerId,
        election_due: Instant,
    },
    Follower {
        peer_id: PeerId,
        tx: mpsc::Sender<crate::cluster::log::Event>,
    },
}

impl Default for State {
    fn default() -> Self {
        State::Wait {
            election_due: election_timeout(false),
        }
    }
}
