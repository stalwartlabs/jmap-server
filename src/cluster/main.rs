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

use crate::JMAPServer;

use super::{
    rpc::{self},
    Cluster, Event,
};
use store::tracing::error;
use store::{log::raft::LogIndex, Store};

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn handle_message(&mut self, message: Event) -> store::Result<bool> {
        match message {
            Event::Gossip { request, addr } => match request {
                // Join request, add node and perform full sync.
                crate::cluster::gossip::request::Request::Join { id, port } => {
                    self.handle_join(id, addr, port).await
                }

                // Join reply.
                crate::cluster::gossip::request::Request::JoinReply { id } => {
                    self.handle_join_reply(id).await
                }

                // Hearbeat request, reply with the cluster status.
                crate::cluster::gossip::request::Request::Ping(peer_list) => {
                    self.handle_ping(peer_list, true).await
                }

                // Heartbeat response, update the cluster status if needed.
                crate::cluster::gossip::request::Request::Pong(peer_list) => {
                    self.handle_ping(peer_list, false).await
                }

                // Leave request.
                crate::cluster::gossip::request::Request::Leave(peer_list) => {
                    self.handle_leave(peer_list).await?
                }
            },

            Event::RpcRequest {
                peer_id,
                request,
                response_tx,
            } => match request {
                rpc::Request::UpdatePeers { peers } => {
                    self.handle_update_peers(response_tx, peers).await;
                }
                rpc::Request::Vote { term, last } => {
                    self.handle_vote_request(peer_id, response_tx, term, last)
                        .await;
                }
                rpc::Request::BecomeFollower { term, last_log } => {
                    self.handle_become_follower(peer_id, response_tx, term, last_log)
                        .await?;
                }
                rpc::Request::AppendEntries { term, request } => {
                    self.handle_append_entries(peer_id, response_tx, term, request)
                        .await;
                }
                rpc::Request::Ping => response_tx
                    .send(rpc::Response::Pong)
                    .unwrap_or_else(|_| error!("Oneshot response channel closed.")),
                rpc::Request::Command { command } => {
                    self.handle_command(command, response_tx).await;
                }
                _ => response_tx
                    .send(rpc::Response::None)
                    .unwrap_or_else(|_| error!("Oneshot response channel closed.")),
            },
            Event::RpcResponse { peer_id, response } => match response {
                rpc::Response::UpdatePeers { peers } => {
                    self.sync_peer_info(peers).await;
                }
                rpc::Response::Vote { term, vote_granted } => {
                    self.handle_vote_response(peer_id, term, vote_granted)
                        .await?;
                }
                rpc::Response::UnregisteredPeer => {
                    self.get_peer(peer_id)
                        .unwrap()
                        .dispatch_request(rpc::Request::UpdatePeers {
                            peers: self.build_peer_info(),
                        })
                        .await;
                }
                _ => (),
            },
            Event::StepDown { term } => {
                if term > self.term {
                    self.step_down(term).await;
                } else {
                    self.start_election_timer(false).await;
                }
            }
            Event::UpdateLastLog { last_log } => {
                self.last_log = last_log;
                self.core.update_raft_index(last_log.index);
            }
            Event::AdvanceUncommittedIndex { uncommitted_index } => {
                if uncommitted_index > self.uncommitted_index
                    || self.uncommitted_index == LogIndex::MAX
                {
                    self.uncommitted_index = uncommitted_index;
                    self.send_append_entries();
                }
            }
            Event::AdvanceCommitIndex {
                peer_id,
                commit_index,
            } => {
                self.advance_commit_index(peer_id, commit_index).await?;
            }
            Event::RpcCommand {
                command,
                response_tx,
            } => {
                self.send_command(command, response_tx).await;
            }
            Event::Shutdown => return Ok(false),

            #[cfg(test)]
            Event::SetOffline { .. } => (),
        }
        Ok(true)
    }

    pub fn is_enabled(&self) -> bool {
        !self.config.key.is_empty()
    }

    pub fn shard_status(&self) -> (u32, u32) {
        let mut total = 0;
        let mut healthy = 0;
        for peer in &self.peers {
            if peer.is_in_shard(self.shard_id) {
                if peer.is_healthy() {
                    healthy += 1;
                }
                total += 1;
            }
        }
        (total, healthy)
    }
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn is_in_cluster(&self) -> bool {
        self.cluster.is_some()
    }
}
