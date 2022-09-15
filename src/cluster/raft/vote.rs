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

use super::{rpc, State};
use super::{
    rpc::{Request, Response},
    Cluster, Peer, PeerId,
};
use store::log::raft::{LogIndex, RaftId, TermId};
use store::tracing::{debug, error, info};
use store::Store;
use tokio::sync::oneshot;

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn can_grant_vote(&self, candidate_peer_id: PeerId) -> bool {
        match self.state {
            State::Wait { .. } => true,
            State::VotedFor { peer_id, .. } => candidate_peer_id == peer_id,
            State::Leader { .. } | State::Follower { .. } | State::Candidate { .. } => false,
        }
    }

    pub async fn vote_for(&mut self, peer_id: PeerId) {
        self.state = State::VotedFor {
            peer_id,
            election_due: self.election_timeout(false),
        };
        self.reset_votes();
        self.core.set_follower(None).await;
        debug!(
            "[{}] Voted for peer {} for term {}.",
            self.addr,
            self.get_peer(peer_id).unwrap(),
            self.term
        );
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

    pub async fn request_votes(&mut self, now: bool) -> store::Result<()> {
        // Check if there is enough quorum for an election.
        if self.has_election_quorum() {
            // Assess whether this node could become the leader for the next term.
            if !self.peers.iter().any(|peer| {
                peer.is_in_shard(self.shard_id)
                    && !peer.is_offline()
                    && self.log_is_behind(peer.last_log_term, peer.last_log_index)
            }) {
                // If this node requires a rollback, it won't be able to become a leader
                // on the next election.
                if !self.core.has_pending_rollback().await? {
                    // Increase term and start election
                    self.run_for_election(now).await;
                    for peer in &self.peers {
                        if peer.is_in_shard(self.shard_id) && !peer.is_offline() {
                            peer.vote_for_me(self.term, self.last_log.index, self.last_log.term)
                                .await;
                        }
                    }
                } else {
                    self.start_election_timer(now).await;
                }
            } else {
                // Wait to receive a vote request from a more up-to-date peer.
                debug!(
                    "[{}] Waiting for a vote request from a more up-to-date peer.",
                    self.addr
                );
                self.start_election_timer(now).await;
            }
        } else {
            self.start_election_timer(false).await;
            info!(
                "Not enough alive peers in shard {} to start election.",
                self.shard_id
            );
        }

        Ok(())
    }

    pub async fn handle_vote_request(
        &mut self,
        peer_id: PeerId,
        response_tx: oneshot::Sender<rpc::Response>,
        term: TermId,
        last: RaftId,
    ) {
        response_tx
            .send(if self.is_known_peer(peer_id) {
                if self.term < term {
                    self.step_down(term).await;
                }
                Response::Vote {
                    term: self.term,
                    vote_granted: if self.term == term
                        && self.can_grant_vote(peer_id)
                        && self.log_is_behind_or_eq(last.term, last.index)
                    {
                        self.vote_for(peer_id).await;
                        true
                    } else {
                        false
                    },
                }
            } else {
                rpc::Response::UnregisteredPeer
            })
            .unwrap_or_else(|_| error!("Oneshot response channel closed."));
    }

    pub async fn handle_vote_response(
        &mut self,
        peer_id: PeerId,
        term: TermId,
        vote_granted: bool,
    ) -> store::Result<()> {
        if self.term < term {
            self.step_down(term).await;
            return Ok(());
        } else if !self.is_candidate() || !vote_granted || self.term != term {
            return Ok(());
        }

        if self.count_vote(peer_id) {
            self.become_leader().await?;
        }

        Ok(())
    }
}

impl Peer {
    pub async fn vote_for_me(&self, term: TermId, last_log_index: LogIndex, last_log_term: TermId) {
        self.dispatch_request(Request::Vote {
            term,
            last: RaftId::new(last_log_term, last_log_index),
        })
        .await;
    }
}
