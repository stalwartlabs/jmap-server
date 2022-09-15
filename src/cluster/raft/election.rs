/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use super::Cluster;
use super::State;
use std::time::{Duration, Instant};
use store::log::raft::TermId;
use store::rand::Rng;
use store::tracing::debug;
use store::Store;

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
            election_due: self.election_timeout(now),
        };
        self.reset_votes();
        self.core.set_follower(None).await;
    }

    pub async fn run_for_election(&mut self, now: bool) {
        self.state = State::Candidate {
            election_due: self.election_timeout(now),
        };
        self.term += 1;
        self.reset_votes();
        self.core.set_follower(None).await;
        debug!(
            "[{}] Running for election for term {}.",
            self.addr, self.term
        );
    }

    pub async fn step_down(&mut self, term: TermId) {
        self.reset_votes();
        self.core.set_follower(None).await;
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
                _ => self.election_timeout(false),
            },
        };
        debug!("[{}] Stepping down for term {}.", self.addr, self.term);
    }

    pub fn election_timeout(&self, now: bool) -> Instant {
        Instant::now()
            + Duration::from_millis(
                if now {
                    0
                } else {
                    self.config.raft_election_timeout
                } + store::rand::thread_rng()
                    .gen_range(ELECTION_TIMEOUT_RAND_FROM..ELECTION_TIMEOUT_RAND_TO),
            )
    }

    pub fn is_candidate(&self) -> bool {
        matches!(self.state, State::Candidate { .. })
    }
}
