use super::Cluster;
use super::Event;
use futures::poll;
use std::task::Poll;
use store::log::raft::LogIndex;
use store::tracing::{debug, error};
use store::Store;
use tokio::sync::watch;

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn spawn_raft_leader_init(
        &self,
        mut log_index_rx: watch::Receiver<Event>,
    ) -> watch::Receiver<bool> {
        let (tx, rx) = watch::channel(false);

        let term = self.term;
        let last_log_index = self.last_log.index;

        let core = self.core.clone();
        tokio::spawn(async move {
            if let Err(err) = core.commit_leader(LogIndex::MAX, true).await {
                error!("Failed to rollback uncommitted entries: {:?}", err);
                return;
            }
            if let Err(err) = core.commit_follower(LogIndex::MAX, true).await {
                error!("Failed to commit pending updates: {:?}", err);
                return;
            }

            // Poll the receiver to make sure this node is still the leader.
            match poll!(Box::pin(log_index_rx.changed())) {
                Poll::Ready(result) => match result {
                    Ok(_) => (),
                    Err(_) => {
                        debug!("This node was asked to step down during initialization.");
                        return;
                    }
                },
                Poll::Pending => (),
            }

            core.update_raft_index(last_log_index);
            if let Err(err) = core.set_leader_commit_index(last_log_index).await {
                error!("Failed to set leader commit index: {:?}", err);
                return;
            }
            core.set_leader(term).await;

            if tx.send(true).is_err() {
                error!("Failed to send message to raft leader processes.");
            }
        });
        rx
    }
}
