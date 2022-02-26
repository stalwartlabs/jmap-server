use std::time::{Duration, Instant};

use rand::Rng;
use tracing::{error, info};

use super::{
    rpc::{Command, Response},
    Cluster, Message, PeerId,
};

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
        let mut urls = Vec::with_capacity(cluster.peers.len());

        for peer in cluster.peers.iter() {
            if peer.is_in_shard(cluster.shard_id) && !peer.is_offline() {
                if is_up_to_date && cluster.log_is_behind(peer.last_log_term, peer.last_log_index) {
                    is_up_to_date = false;
                }
                urls.push(peer.rpc_url.clone());
            }
        }

        if is_up_to_date {
            // Increase term and start election
            cluster.run_for_election();
            requests.push(Message::VoteRequest {
                urls,
                term: cluster.term,
                last_log_index: cluster.last_log_index,
                last_log_term: cluster.last_log_term,
            });
        } else {
            // Query who is the current leader while at the same time wait to
            // receive a vote request from a more up-to-date peer.
            cluster.start_election_timer();
            requests.push(Message::JoinRaftRequest { urls });
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
) -> Command {
    if cluster.term < term {
        cluster.step_down(term);
    }

    Command::VoteResponse {
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

pub fn handle_vote_responses(
    cluster: &mut Cluster,
    responses: Vec<Option<Response>>,
) -> Option<Command> {
    let mut votes = 1; // Count the local node's vote.
    let mut started_election = false;

    for response in responses.into_iter().flatten() {
        if let Command::VoteResponse { term, vote_granted } = response.cmd {
            if cluster.term < term {
                cluster.step_down(term);
                started_election = true;
            } else if vote_granted {
                votes += 1;
            }
        } else {
            error!(
                "Unexpected command {:?} from peer {}.",
                response.cmd, response.peer_id
            );
            return None;
        }
    }

    if cluster.is_candidate() && votes > cluster.quorum() {
        cluster.become_leader();
        Some(Command::FollowLeaderRequest {
            term: cluster.term,
            last_log_index: cluster.last_log_index,
            last_log_term: cluster.last_log_term,
        })
    } else {
        if !started_election {
            cluster.start_election_timer();
        }
        None
    }
}

pub fn handle_follow_leader_request(
    cluster: &mut Cluster,
    peer_id: PeerId,
    term: TermId,
    last_log_index: LogIndex,
    last_log_term: TermId,
) -> Command {
    if cluster.term < term {
        cluster.term = term;
    }

    Command::FollowLeaderResponse {
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

pub fn handle_follow_leader_responses(cluster: &mut Cluster, responses: Vec<Option<Response>>) {
    for response in responses.into_iter().flatten() {
        if let Command::FollowLeaderResponse { term, success } = response.cmd {
            if cluster.term < term {
                cluster.step_down(term);
            } else if !success {
                cluster.start_election_timer();
            }
        } else {
            error!(
                "Unexpected command {:?} from peer {}.",
                response.cmd, response.peer_id
            );
            return;
        }
    }
}

pub fn handle_join_raft_responses(cluster: &mut Cluster, responses: Vec<Option<Response>>) {
    if matches!(cluster.state, State::Wait(_)) {
        for response in responses.into_iter().flatten() {
            if let Command::JoinRaftResponse { term, leader_id } = response.cmd {
                match (&cluster.state, leader_id) {
                    (State::Wait(_), Some(leader_id)) if cluster.is_peer_alive(leader_id) => {
                        cluster.state = State::Follower(leader_id);
                    }
                    (State::Follower(current_leader_id), Some(leader_id))
                        if current_leader_id != &leader_id
                            && cluster.term < term
                            && cluster.is_peer_alive(leader_id) =>
                    {
                        cluster.state = State::Follower(leader_id);
                    }
                    _ => {}
                }

                if cluster.term < term {
                    cluster.term = term;
                }
            } else {
                error!(
                    "Unexpected command {:?} from peer {}.",
                    response.cmd, response.peer_id
                );
                return;
            }
        }
    }
}
