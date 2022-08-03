use store::log::raft::TermId;
use store::tracing::error;
use store::Store;
use tokio::sync::oneshot;

use crate::cluster::log::{AppendEntriesRequest, Event};

use super::PeerId;
use super::{
    rpc::{self},
    Cluster,
};

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn handle_append_entries(
        &mut self,
        peer_id: PeerId,
        response_tx: oneshot::Sender<rpc::Response>,
        term: TermId,
        request: AppendEntriesRequest,
    ) {
        if term > self.term {
            self.term = term;
        }

        match self.is_following_peer(peer_id) {
            Some(tx) => {
                tx.send(Event {
                    response_tx,
                    request,
                })
                .await
                .unwrap_or_else(|err| error!("Failed to send event: {}", err));
            }
            _ => response_tx
                .send(rpc::Response::StepDown { term: self.term })
                .unwrap_or_else(|_| error!("Oneshot response channel closed.")),
        }
    }
}
