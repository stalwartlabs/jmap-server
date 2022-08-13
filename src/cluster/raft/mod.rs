pub mod commit;
pub mod election;
pub mod follower;
pub mod leader;
pub mod log;
pub mod vote;

use self::election::{ELECTION_TIMEOUT_RAND_FROM, ELECTION_TIMEOUT_RAND_TO};

use super::{rpc, RAFT_LOG_BEHIND, RAFT_LOG_LEADER, RAFT_LOG_UPDATED};
use super::{Cluster, Peer, PeerId};
use std::time::{Duration, Instant};
use store::rand::Rng;
use tokio::sync::{mpsc, watch};

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

impl State {
    pub fn init() -> Self {
        State::Wait {
            election_due: Instant::now()
                + Duration::from_millis(
                    1000 + store::rand::thread_rng()
                        .gen_range(ELECTION_TIMEOUT_RAND_FROM..ELECTION_TIMEOUT_RAND_TO),
                ),
        }
    }
}
