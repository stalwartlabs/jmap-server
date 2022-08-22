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
                /*println!(
                    "[{}] Follower updated store to id {}, term {}.",
                    self.addr, last_log.index, last_log.term
                );*/
                self.last_log = last_log;
                self.core.update_raft_index(last_log.index);
            }
            Event::AdvanceUncommittedIndex { uncommitted_index } => {
                /*println!(
                    "[{}] Sending appendEntries request for id {}, term {}.",
                    self.addr, uncommitted_index, self.term
                );*/
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
