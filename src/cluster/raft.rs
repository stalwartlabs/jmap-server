use std::time::{Duration, Instant};

use rand::Rng;
use store::Store;
use tokio::sync::mpsc;
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
    Leader,
    Wait(Instant),
    Candidate(Instant),
    VotedFor((PeerId, Instant)),
    Follower((PeerId, mpsc::Sender<bool>)),
}

impl Default for State {
    fn default() -> Self {
        State::Wait(election_timeout(false))
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
            State::Candidate(timeout) | State::Wait(timeout) | State::VotedFor((_, timeout))
                if timeout >= Instant::now() =>
            {
                false
            }
            _ => true,
        }
    }

    pub fn time_to_next_election(&self) -> Option<u64> {
        match self.state {
            State::Candidate(timeout) | State::Wait(timeout) | State::VotedFor((_, timeout)) => {
                let now = Instant::now();
                Some(if timeout > now {
                    (timeout - now).as_millis() as u64
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

    pub fn can_grant_vote(&self, peer_id: PeerId) -> bool {
        match self.state {
            State::Wait(_) => true,
            State::VotedFor((voted_for, _)) => voted_for == peer_id,
            State::Leader | State::Follower(_) | State::Candidate(_) => false,
        }
    }

    pub fn leader_peer_id(&self) -> Option<PeerId> {
        match self.state {
            State::Leader => Some(self.peer_id),
            State::Follower((peer_id, _)) => Some(peer_id),
            _ => None,
        }
    }

    pub fn is_leading(&self) -> bool {
        matches!(self.state, State::Leader)
    }

    pub fn is_candidate(&self) -> bool {
        matches!(self.state, State::Candidate(_))
    }

    pub fn is_following(&self) -> bool {
        matches!(self.state, State::Follower(_))
    }

    pub fn start_election_timer(&mut self, now: bool) {
        self.state = State::Wait(election_timeout(now));
        self.reset();
    }

    pub fn step_down(&mut self, term: TermId) {
        self.reset();
        self.term = term;
        self.state = State::Wait(match self.state {
            State::Wait(timeout) | State::Candidate(timeout) | State::VotedFor((_, timeout))
                if timeout < Instant::now() =>
            {
                timeout
            }
            _ => election_timeout(false),
        });
        debug!("Stepping down for term {}.", self.term);
    }

    pub fn vote_for(&mut self, peer_id: PeerId) {
        self.state = State::VotedFor((peer_id, election_timeout(false)));
        self.reset();
        debug!("Voted for peer {} for term {}.", peer_id, self.term);
    }

    pub fn follow_leader(&mut self, peer_id: PeerId) {
        self.state = State::Follower((
            peer_id,
            start_log_sync(
                self.core.clone(),
                self.get_peer(peer_id).unwrap().tx.clone(),
            ),
        ));
        self.reset();
        debug!("Following peer {} for term {}.", peer_id, self.term);
    }

    pub async fn sync_log(&self) {
        if let State::Follower((peer_id, log_sync_tx)) = &self.state {
            let leader = self.get_peer(*peer_id).unwrap();
            if self.log_is_behind(leader.last_log_term, leader.last_log_index) {
                log_sync_tx.try_send(true).ok();
            }
        }
    }

    pub fn run_for_election(&mut self, now: bool) {
        self.state = State::Candidate(election_timeout(now));
        self.term += 1;
        self.reset();
        debug!("Running for election for term {}.", self.term);
    }

    pub fn become_leader(&mut self) {
        debug!("This node is the new leader for term {}.", self.term);
        self.state = State::Leader;
        self.reset();
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

        /*debug!(
            "Vote: {} {} {:?} {}",
            self.term,
            term,
            self.state,
            self.log_is_behind_or_eq(last_log_term, last_log_index)
        );*/

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
        /*debug!(
            "Vote Response: {}, {}, {:?}, {}",
            self.term, term, self.state, vote_granted
        );*/

        if self.term < term {
            self.step_down(term);
            return;
        } else if !self.is_candidate() || !vote_granted || self.term != term {
            return;
        }

        if self.count_vote(peer_id) {
            self.become_leader();
            for peer in &self.peers {
                if peer.is_in_shard(self.shard_id) && !peer.is_offline() {
                    peer.follow_me(self.term, self.last_log_index, self.last_log_term)
                        .await;
                }
            }
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

    pub fn handle_follow_leader_response(&mut self, term: TermId, success: bool) {
        if self.term < term {
            self.step_down(term);
        } else if !success {
            self.start_election_timer(false);
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

    pub async fn follow_me(&self, term: TermId, last_log_index: LogIndex, last_log_term: TermId) {
        self.dispatch_request(Request::FollowLeader {
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
