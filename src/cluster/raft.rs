use std::time::{Duration, Instant};

use rand::Rng;
use store::Store;
use tokio::sync::watch;
use tracing::{debug, info};

use crate::cluster::log::start_log_sync;

use super::{
    rpc::{Request, Response},
    Cluster, Peer, PeerId,
};

pub type TermId = u64;
pub type LogIndex = u64;

pub const ELECTION_TIMEOUT: u64 = 1000;
pub const ELECTION_TIMEOUT_RAND_FROM: u64 = 150;
pub const ELECTION_TIMEOUT_RAND_TO: u64 = 300;

#[derive(Debug)]
pub enum State {
    Leader {
        follower_tx: Vec<watch::Sender<LogIndex>>,
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
    },
}

impl Default for State {
    fn default() -> Self {
        State::Wait {
            election_due: election_timeout(false),
        }
    }
}

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn has_election_quorum(&self) -> bool {
        let (total, healthy) = self.shard_status();
        healthy >= ((total as f64 + 1.0) / 2.0).floor() as u32
    }

    pub fn is_election_due(&self) -> bool {
        match self.state {
            State::Candidate { election_due }
            | State::Wait { election_due }
            | State::VotedFor { election_due, .. }
                if election_due >= Instant::now() =>
            {
                false
            }
            _ => true,
        }
    }

    pub fn time_to_next_election(&self) -> Option<u64> {
        match self.state {
            State::Candidate { election_due }
            | State::Wait { election_due }
            | State::VotedFor { election_due, .. } => {
                let now = Instant::now();
                Some(if election_due > now {
                    (election_due - now).as_millis() as u64
                } else {
                    0
                })
            }
            _ => None,
        }
    }

    pub fn log_is_behind_or_eq(&self, last_log_term: TermId, last_log_index: LogIndex) -> bool {
        last_log_term > self.last_log_term
            || (last_log_term == self.last_log_term && last_log_index >= self.last_log_index)
    }

    pub fn log_is_behind(&self, last_log_term: TermId, last_log_index: LogIndex) -> bool {
        last_log_term > self.last_log_term
            || (last_log_term == self.last_log_term && last_log_index > self.last_log_index)
    }

    pub fn can_grant_vote(&self, candidate_peer_id: PeerId) -> bool {
        match self.state {
            State::Wait { .. } => true,
            State::VotedFor { peer_id, .. } => candidate_peer_id == peer_id,
            State::Leader { .. } | State::Follower { .. } | State::Candidate { .. } => false,
        }
    }

    pub fn leader_peer_id(&self) -> Option<PeerId> {
        match self.state {
            State::Leader { .. } => Some(self.peer_id),
            State::Follower { peer_id, .. } => Some(peer_id),
            _ => None,
        }
    }

    pub fn is_leading(&self) -> bool {
        matches!(self.state, State::Leader { .. })
    }

    pub fn is_candidate(&self) -> bool {
        matches!(self.state, State::Candidate { .. })
    }

    pub fn is_following(&self) -> bool {
        matches!(self.state, State::Follower { .. })
    }

    pub fn start_election_timer(&mut self, now: bool) {
        self.state = State::Wait {
            election_due: election_timeout(now),
        };
        self.reset();
    }

    pub fn step_down(&mut self, term: TermId) {
        self.reset();
        self.term = term;
        self.state = State::Wait {
            election_due: match self.state {
                State::Wait { election_due }
                | State::Candidate { election_due }
                | State::VotedFor { election_due, .. }
                    if election_due < Instant::now() =>
                {
                    election_due
                }
                _ => election_timeout(false),
            },
        };
        debug!("Stepping down for term {}.", self.term);
    }

    pub fn vote_for(&mut self, peer_id: PeerId) {
        self.state = State::VotedFor {
            peer_id,
            election_due: election_timeout(false),
        };
        self.reset();
        debug!("Voted for peer {} for term {}.", peer_id, self.term);
    }

    pub fn follow_leader(&mut self, peer_id: PeerId) {
        self.state = State::Follower { peer_id };
        self.reset();
        debug!("Following peer {} for term {}.", peer_id, self.term);
    }

    pub async fn send_append_entries(&self, last_log_term: LogIndex) {
        if let State::Leader { follower_tx } = &self.state {
            for tx in follower_tx {
                tx.send(last_log_term).ok();
            }
        }
    }

    pub fn run_for_election(&mut self, now: bool) {
        self.state = State::Candidate {
            election_due: election_timeout(now),
        };
        self.term += 1;
        self.reset();
        debug!("Running for election for term {}.", self.term);
    }

    pub fn become_leader(&mut self) {
        debug!("This node is the new leader for term {}.", self.term);
        self.state = State::Leader {
            follower_tx: self
                .peers
                .iter()
                .filter(|p| p.is_in_shard(self.shard_id))
                .map(|p| start_log_sync(self, p.tx.clone()))
                .collect(),
        };
        self.reset();
    }

    pub fn add_follower(&mut self, peer_id: PeerId) {
        let peer_tx = start_log_sync(self, self.get_peer(peer_id).unwrap().tx.clone());
        if let State::Leader { follower_tx } = &mut self.state {
            follower_tx.push(peer_tx);
        }
    }

    pub fn reset(&mut self) {
        self.peers.iter_mut().for_each(|peer| {
            peer.vote_granted = false;
        });
    }

    pub fn count_vote(&mut self, peer_id: PeerId) -> bool {
        let mut total_peers = 0;
        let shard_id = self.shard_id;
        let mut votes = 1; // Count this node's vote

        self.peers.iter_mut().for_each(|peer| {
            if peer.is_in_shard(shard_id) {
                total_peers += 1;
                if peer.peer_id == peer_id {
                    peer.vote_granted = true;
                    votes += 1;
                } else if peer.vote_granted {
                    votes += 1;
                }
            }
        });

        votes > ((total_peers as f64 + 1.0) / 2.0).floor() as u32
    }

    pub async fn start_election(&mut self, now: bool) {
        // Check if there is enough quorum for an election.
        if self.has_election_quorum() {
            // Assess whether this node could become the leader for the next term.
            if !self.peers.iter().any(|peer| {
                peer.is_in_shard(self.shard_id)
                    && !peer.is_offline()
                    && self.log_is_behind(peer.last_log_term, peer.last_log_index)
            }) {
                // Increase term and start election
                self.run_for_election(now);
                for peer in &self.peers {
                    if peer.is_in_shard(self.shard_id) && !peer.is_offline() {
                        peer.vote_for_me(self.term, self.last_log_index, self.last_log_term)
                            .await;
                    }
                }
            } else {
                // Wait to receive a vote request from a more up-to-date peer.
                self.start_election_timer(now);
            }
        } else {
            self.start_election_timer(false);
            info!(
                "Not enough alive peers in shard {} to start election.",
                self.shard_id
            );
        }
    }

    pub fn handle_vote_request(
        &mut self,
        peer_id: PeerId,
        term: TermId,
        last_log_index: LogIndex,
        last_log_term: TermId,
    ) -> Response {
        if self.term < term {
            self.step_down(term);
        }

        Response::Vote {
            term: self.term,
            vote_granted: if self.term == term
                && self.can_grant_vote(peer_id)
                && self.log_is_behind_or_eq(last_log_term, last_log_index)
            {
                self.vote_for(peer_id);
                true
            } else {
                false
            },
        }
    }

    pub async fn handle_vote_response(
        &mut self,
        peer_id: PeerId,
        term: TermId,
        vote_granted: bool,
    ) {
        if self.term < term {
            self.step_down(term);
            return;
        } else if !self.is_candidate() || !vote_granted || self.term != term {
            return;
        }

        if self.count_vote(peer_id) {
            self.become_leader();
        }
    }

    pub fn handle_follow_leader_request(
        &mut self,
        peer_id: PeerId,
        term: TermId,
        last_log_index: LogIndex,
        last_log_term: TermId,
    ) -> Response {
        if self.term < term {
            self.term = term;
        }

        Response::FollowLeader {
            term: self.term,
            success: if self.term == term && self.log_is_behind_or_eq(last_log_term, last_log_index)
            {
                self.follow_leader(peer_id);
                true
            } else {
                false
            },
        }
    }
}

impl Peer {
    pub async fn vote_for_me(&self, term: TermId, last_log_index: LogIndex, last_log_term: TermId) {
        self.dispatch_request(Request::Vote {
            term,
            last_log_index,
            last_log_term,
        })
        .await;
    }
}

pub fn election_timeout(now: bool) -> Instant {
    Instant::now()
        + Duration::from_millis(
            if now { 0 } else { ELECTION_TIMEOUT }
                + rand::thread_rng()
                    .gen_range(ELECTION_TIMEOUT_RAND_FROM..ELECTION_TIMEOUT_RAND_TO),
        )
}
