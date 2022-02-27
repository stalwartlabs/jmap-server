use std::time::{Duration, Instant};

use actix_web::web;
use rand::Rng;
use store::Store;
use tracing::{debug, info};

use crate::JMAPServer;

use super::{rpc::Request, Cluster, Message, PeerId};

pub type TermId = u64;
pub type LogIndex = u64;

pub const ELECTION_TIMEOUT: u64 = 1000;
pub const ELECTION_TIMEOUT_RAND_FROM: u64 = 150;
pub const ELECTION_TIMEOUT_RAND_TO: u64 = 300;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    Leader,
    Wait(Instant),
    Candidate(Instant),
    VotedFor((PeerId, Instant)),
    Follower(PeerId),
}

impl Default for State {
    fn default() -> Self {
        State::Wait(election_timeout())
    }
}

impl Cluster {
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
            State::Follower(peer_id) => Some(peer_id),
            _ => None,
        }
    }

    pub fn is_leading(&self) -> bool {
        matches!(self.state, State::Leader)
    }

    pub fn is_candidate(&self) -> bool {
        matches!(self.state, State::Candidate(_))
    }

    pub fn start_election_timer(&mut self) {
        self.state = State::Wait(election_timeout());
        self.reset_votes();
    }

    pub fn step_down(&mut self, term: TermId) {
        self.reset_votes();
        self.term = term;
        self.state = State::Wait(match self.state {
            State::Wait(timeout) | State::Candidate(timeout) | State::VotedFor((_, timeout))
                if timeout < Instant::now() =>
            {
                timeout
            }
            _ => election_timeout(),
        });
        debug!("Stepping down for term {}.", self.term);
    }

    pub fn vote_for(&mut self, peer_id: PeerId) {
        self.state = State::VotedFor((peer_id, election_timeout()));
        self.reset_votes();
        debug!("Voted for peer {} for term {}.", peer_id, self.term);
    }

    pub fn follow_leader(&mut self, peer_id: PeerId) {
        self.state = State::Follower(peer_id);
        self.reset_votes();
        debug!("Following peer {} for term {}.", peer_id, self.term);
    }

    pub fn run_for_election(&mut self) {
        self.state = State::Candidate(election_timeout());
        self.term += 1;
        self.reset_votes();
        debug!("Running for election for term {}.", self.term);
    }

    pub fn become_leader(&mut self) {
        debug!("This node is the new leader for term {}.", self.term);
        self.state = State::Leader;
        self.reset_votes();
    }

    pub fn reset_votes(&mut self) {
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
}

pub fn election_timeout() -> Instant {
    Instant::now()
        + Duration::from_millis(
            ELECTION_TIMEOUT
                + rand::thread_rng()
                    .gen_range(ELECTION_TIMEOUT_RAND_FROM..ELECTION_TIMEOUT_RAND_TO),
        )
}

pub fn start_election(cluster: &mut Cluster, requests: &mut Vec<Message>) {
    // Check if there is enough quorum for an election.
    if cluster.has_election_quorum() {
        // Assess whether this node could become the leader for the next term.
        let mut is_up_to_date = true;
        let mut channels = Vec::new();

        for peer in cluster.peers.iter() {
            if peer.is_in_shard(cluster.shard_id) && !peer.is_offline() {
                if cluster.log_is_behind(peer.last_log_term, peer.last_log_index) {
                    is_up_to_date = false;
                    break;
                } else {
                    channels.push(peer.rpc_channel.clone());
                }
            }
        }

        if is_up_to_date {
            // Increase term and start election
            cluster.run_for_election();
            requests.push(Message::new_rpc_many(
                channels,
                Request::VoteRequest {
                    term: cluster.term,
                    last_log_index: cluster.last_log_index,
                    last_log_term: cluster.last_log_term,
                },
            ));
        } else {
            // Query who is the current leader while at the same time wait to
            // receive a vote request from a more up-to-date peer.
            cluster.start_election_timer();
            //requests.push(Message::new_rpc_all(Request::JoinRaftRequest));
        }
    } else {
        cluster.start_election_timer();
        info!(
            "Not enough alive peers in shard {} to start election.",
            cluster.shard_id
        );
    }
}

pub fn handle_vote_request(
    cluster: &mut Cluster,
    peer_id: PeerId,
    term: TermId,
    last_log_index: LogIndex,
    last_log_term: TermId,
) -> Request {
    if cluster.term < term {
        cluster.step_down(term);
    }

    Request::VoteResponse {
        term: cluster.term,
        vote_granted: if cluster.term == term
            && cluster.can_grant_vote(peer_id)
            && cluster.log_is_behind_or_eq(last_log_term, last_log_index)
        {
            cluster.vote_for(peer_id);
            true
        } else {
            false
        },
    }
}

pub async fn handle_vote_response<T>(
    core: &web::Data<JMAPServer<T>>,
    peer_id: PeerId,
    term: TermId,
    vote_granted: bool,
) where
    T: for<'x> Store<'x> + 'static,
{
    let (channels, request) = {
        let mut cluster = core.cluster.lock();

        if cluster.term < term {
            cluster.step_down(term);
            return;
        } else if !cluster.is_candidate() || !vote_granted || cluster.term != term {
            return;
        }

        if cluster.count_vote(peer_id) {
            cluster.become_leader();
            let mut channels = Vec::new();
            for peer in &cluster.peers {
                if peer.is_in_shard(cluster.shard_id) && !peer.is_offline() {
                    channels.push(peer.rpc_channel.clone());
                }
            }
            (
                channels,
                Request::FollowLeaderRequest {
                    term: cluster.term,
                    last_log_index: cluster.last_log_index,
                    last_log_term: cluster.last_log_term,
                },
            )
        } else {
            return;
        }
    };

    for channel in channels {
        channel.send(request.clone()).await.unwrap();
    }
}

pub fn handle_follow_leader_request(
    cluster: &mut Cluster,
    peer_id: PeerId,
    term: TermId,
    last_log_index: LogIndex,
    last_log_term: TermId,
) -> Request {
    if cluster.term < term {
        cluster.term = term;
    }

    Request::FollowLeaderResponse {
        term: cluster.term,
        success: if cluster.term == term
            && cluster.log_is_behind_or_eq(last_log_term, last_log_index)
        {
            cluster.follow_leader(peer_id);
            true
        } else {
            false
        },
    }
}
