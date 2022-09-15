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
