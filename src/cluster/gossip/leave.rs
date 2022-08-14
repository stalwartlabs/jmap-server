use super::request::Request;
use super::{Cluster, PeerStatus};
use crate::cluster::gossip::State;
use store::log::raft::LogIndex;
use store::tracing::debug;
use store::Store;

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn broadcast_leave(&self) {
        let mut status: Vec<PeerStatus> = Vec::with_capacity(self.peers.len() + 1);
        status.push(self.into());
        for peer in &self.peers {
            if !peer.is_offline() {
                self.send_gossip(peer.addr, Request::Leave(status.clone()))
                    .await;
            }
        }
    }

    pub async fn handle_leave(&mut self, peers: Vec<PeerStatus>) -> store::Result<()> {
        if let Some(peer) = peers.first() {
            let (is_leader_leaving, is_leading) = match self.state {
                crate::cluster::raft::State::Leader { .. } => (false, true),
                crate::cluster::raft::State::Follower { peer_id, .. } => {
                    (peer.peer_id == peer_id, false)
                }
                _ => (false, false),
            };

            let mut peer_commit_index = LogIndex::MAX;
            for local_peer in self.peers.iter_mut() {
                if local_peer.peer_id == peer.peer_id {
                    debug!(
                        "{} {} is leaving the cluster.",
                        if is_leader_leaving { "Leader" } else { "Peer" },
                        local_peer.addr
                    );

                    if is_leading
                        && local_peer.is_in_shard(self.shard_id)
                        && peer.last_log_index > local_peer.last_log_index
                    {
                        peer_commit_index = peer.last_log_index;
                    }

                    local_peer.state = State::Left;
                    local_peer.epoch = peer.epoch;
                    local_peer.last_log_index = peer.last_log_index;
                    local_peer.last_log_term = peer.last_log_term;

                    break;
                }
            }

            // Advance local commit index
            if peer_commit_index != LogIndex::MAX {
                self.advance_commit_index(peer.peer_id, peer_commit_index)
                    .await?;
            }

            if is_leader_leaving {
                self.request_votes(true).await?;
            }
        }

        Ok(())
    }
}
