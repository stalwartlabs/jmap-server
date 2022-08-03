use super::Cluster;
use super::State;
use std::time::{Duration, Instant};
use store::log::raft::TermId;
use store::rand::Rng;
use store::tracing::debug;
use store::Store;

pub const ELECTION_TIMEOUT: u64 = 1000;
pub const ELECTION_TIMEOUT_RAND_FROM: u64 = 50;
pub const ELECTION_TIMEOUT_RAND_TO: u64 = 300;

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

    pub async fn start_election_timer(&mut self, now: bool) {
        self.state = State::Wait {
            election_due: election_timeout(now),
        };
        self.reset_votes();
        self.core.set_follower().await;
    }

    pub async fn run_for_election(&mut self, now: bool) {
        self.state = State::Candidate {
            election_due: election_timeout(now),
        };
        self.term += 1;
        self.reset_votes();
        self.core.set_follower().await;
        debug!(
            "[{}] Running for election for term {}.",
            self.addr, self.term
        );
    }

    pub async fn step_down(&mut self, term: TermId) {
        self.reset_votes();
        self.core.set_follower().await;
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
        debug!("[{}] Stepping down for term {}.", self.addr, self.term);
    }

    pub fn is_candidate(&self) -> bool {
        matches!(self.state, State::Candidate { .. })
    }
}

pub fn election_timeout(now: bool) -> Instant {
    Instant::now()
        + Duration::from_millis(
            if now { 0 } else { ELECTION_TIMEOUT }
                + store::rand::thread_rng()
                    .gen_range(ELECTION_TIMEOUT_RAND_FROM..ELECTION_TIMEOUT_RAND_TO),
        )
}
