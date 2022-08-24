use super::rpc::Response;
use super::{RaftIndexes, State};
use crate::cluster::log::AppendEntriesResponse;
use crate::JMAPServer;
use store::log::raft::LogIndex;
use store::tracing::{debug, error};
use store::Store;

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn commit_updates(&self, indexes: &mut RaftIndexes) -> Option<(State, Response)> {
        // Apply changes
        if indexes.leader_commit_index != LogIndex::MAX
            && indexes.uncommitted_index <= indexes.leader_commit_index
        {
            let last_log = match self.commit_follower(indexes.uncommitted_index, false).await {
                Ok(Some(last_log)) => last_log,
                Ok(None) => {
                    error!(
                        "Raft entry {} not found while committing updates.",
                        indexes.uncommitted_index
                    );
                    return None;
                }
                Err(err) => {
                    error!("Failed to apply changes: {:?}", err);
                    return None;
                }
            };

            indexes.commit_index = indexes.uncommitted_index;
            self.update_last_log(last_log).await;

            // Set up to date
            if indexes.commit_index == indexes.leader_commit_index {
                debug!(
                    "This node is now up to date with the leader's commit index {}.",
                    indexes.leader_commit_index
                );
                self.set_up_to_date(true);
            } else {
                debug!(
                    concat!(
                        "This node is still behind the leader's commit index {}, ",
                        "local commit index is {}."
                    ),
                    indexes.leader_commit_index, indexes.commit_index
                );
            }
        } else {
            debug!(
                concat!(
                    "No changes to apply: leader commit index = {}, ",
                    "local uncommitted index: {}, local committed index: {}."
                ),
                indexes.leader_commit_index, indexes.uncommitted_index, indexes.leader_commit_index
            );
        }
        (
            State::Synchronize,
            Response::AppendEntries(AppendEntriesResponse::Done {
                up_to_index: indexes.uncommitted_index,
            }),
        )
            .into()
    }
}
