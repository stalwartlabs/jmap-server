use super::request::Request;
use super::{Cluster, PeerStatus};
use crate::cluster::gossip::State;
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
            let is_leader_leaving =
                matches!(self.leader_peer_id(), Some(leader_id) if peer.peer_id == leader_id);

            for local_peer in self.peers.iter_mut() {
                if local_peer.peer_id == peer.peer_id {
                    debug!(
                        "{} {} is leaving the cluster.",
                        if is_leader_leaving { "Leader" } else { "Peer" },
                        local_peer.addr
                    );
                    local_peer.state = State::Left;
                    local_peer.epoch = peer.epoch;
                    local_peer.last_log_index = peer.last_log_index;
                    local_peer.last_log_term = peer.last_log_term;
                    //TODO advance local commit index
                    break;
                }
            }

            if is_leader_leaving {
                self.request_votes(true).await?;
            }
        }

        Ok(())
    }
}
