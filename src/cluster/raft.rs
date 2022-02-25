use std::time::Instant;

use tracing::error;

use super::{
    rpc::{Command, Response},
    Cluster, PeerId,
};

pub type TermId = u64;
pub type LogIndex = u64;

pub const ELECTION_TIMEOUT: u128 = 2000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    None,
    Leader,
    Wait(Instant),
    Candidate(Instant),
    VotedFor((PeerId, Instant)),
    Follower(PeerId),
}

impl State {
    pub fn will_grant_vote(&self, peer_id: PeerId) -> bool {
        match self {
            State::None | State::Wait(_) => true,
            State::VotedFor((voted_for, _)) => *voted_for == peer_id,
            State::Leader | State::Follower(_) | State::Candidate(_) => false,
        }
    }
}

impl Default for State {
    fn default() -> Self {
        State::None
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
        cluster.term = term;
        cluster.state = State::Wait(Instant::now());
    }

    let vote_granted = cluster.term == term
        && cluster.state.will_grant_vote(peer_id)
        && (last_log_term > cluster.last_log_term
            || (last_log_term == cluster.last_log_term
                && last_log_index >= cluster.last_log_index));

    if vote_granted {
        cluster.state = State::VotedFor((peer_id, Instant::now()));
    }

    Command::VoteResponse {
        term: cluster.term,
        vote_granted,
    }
}

pub fn handle_vote_responses(
    cluster: &mut Cluster,
    responses: Vec<Option<Response>>,
) -> Option<Command> {
    let mut votes = 1; // Count the local node's vote.

    for response in responses.into_iter().flatten() {
        if let Command::VoteResponse { term, vote_granted } = response.cmd {
            if cluster.term < term {
                cluster.term = term;
                cluster.state = State::None;
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

    if matches!(cluster.state, State::Candidate(_)) && votes > cluster.quorum() {
        cluster.state = State::Leader;
        Some(Command::BecomeLeader { term: cluster.term })
    } else {
        None
    }
}

pub fn handle_become_leader_responses(cluster: &mut Cluster, responses: Vec<Option<Response>>) {
    for response in responses.into_iter().flatten() {
        if let Command::BecomeLeader { term } = response.cmd {
            if cluster.term < term {
                cluster.term = term;
                if matches!(cluster.state, State::Leader) {
                    cluster.state = State::None;
                }
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

pub fn handle_query_leader_responses(cluster: &mut Cluster, responses: Vec<Option<Response>>) {
    if matches!(cluster.state, State::None | State::Wait(_)) {
        for response in responses.into_iter().flatten() {
            if let Command::QueryLeaderResponse { term, leader_id } = response.cmd {
                match (cluster.state, leader_id) {
                    (State::None | State::Wait(_), Some(leader_id)) => {
                        cluster.state = State::Follower(leader_id);
                    }
                    (State::Follower(current_leader_id), Some(leader_id))
                        if current_leader_id != leader_id && cluster.term < term =>
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
