use store::log::raft::{RaftId, TermId};
use store::tracing::error;
use store::Store;
use tokio::sync::oneshot;

use crate::cluster::log::{AppendEntriesRequest, Event};

use super::PeerId;
use super::{
    rpc::{self, Response},
    Cluster,
};

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn handle_become_follower(
        &mut self,
        peer_id: PeerId,
        response_tx: oneshot::Sender<rpc::Response>,
        term: TermId,
        last_log: RaftId,
    ) -> store::Result<()> {
        if self.is_known_peer(peer_id) {
            if self.term < term {
                self.term = term;
            }

            if self.term == term && self.log_is_behind_or_eq(last_log.term, last_log.index) {
                self.follow_leader(peer_id)
                    .await?
                    .send(Event {
                        response_tx,
                        request: AppendEntriesRequest::Match { last_log },
                    })
                    .await
                    .unwrap_or_else(|err| error!("Failed to send event: {}", err));
            } else {
                response_tx
                    .send(Response::StepDown { term: self.term })
                    .unwrap_or_else(|_| error!("Oneshot response channel closed."));
            }
        } else {
            response_tx
                .send(rpc::Response::UnregisteredPeer)
                .unwrap_or_else(|_| error!("Oneshot response channel closed."));
        }
        Ok(())
    }
}
