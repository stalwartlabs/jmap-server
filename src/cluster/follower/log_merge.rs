use store::log::raft::RaftId;
use store::tracing::error;
use store::Store;

use crate::JMAPServer;

use super::rpc::Response;
use super::State;

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn handle_merge_log(&self, matched_log: RaftId) -> Option<(State, Response)> {
        if let Err(err) = self.prepare_rollback_changes(matched_log.index, true).await {
            error!("Failed to prepare rollback changes: {:?}", err);
            return None;
        }

        let (account_id, collection, changes) = match self.next_rollback_change().await {
            Ok(Some(rollback_change)) => rollback_change,
            Ok(None) => {
                error!("Failed to prepare rollback changes: No changes found.");
                return None;
            }
            Err(err) => {
                error!("Failed to obtain rollback changes: {:?}", err);
                return None;
            }
        };

        self.handle_rollback_updates(account_id, collection, changes, vec![])
            .await
    }
}
