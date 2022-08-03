use super::rpc::Response;
use super::{RaftIndexes, State};
use crate::cluster::log::Update;
use crate::cluster::log::{AppendEntriesResponse, PendingUpdate, PendingUpdates};
use crate::JMAPServer;
use store::core::bitmap::Bitmap;
use store::core::collection::Collection;
use store::serialize::key::LogKey;
use store::serialize::StoreSerialize;
use store::tracing::error;
use store::{AccountId, ColumnFamily, Store};

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn handle_pending_updates(
        &self,
        indexes: &mut RaftIndexes,
        changed_accounts: Vec<(AccountId, Bitmap<Collection>)>,
        updates: Vec<Update>,
    ) -> Option<(State, Response)> {
        //println!("{:#?}", updates);
        let mut pending_updates = Vec::with_capacity(updates.len());
        let mut is_done = updates.is_empty();

        for update in updates {
            match update {
                Update::Begin {
                    account_id,
                    collection,
                } => {
                    pending_updates.push(PendingUpdate::Begin {
                        account_id,
                        collection,
                    });
                }
                Update::Document { update } => {
                    pending_updates.push(PendingUpdate::Update { update });
                }
                Update::Eof => {
                    is_done = true;
                }
                _ => {
                    debug_assert!(false, "Invalid update: {:?}", update);
                }
            }
        }

        if !pending_updates.is_empty() {
            //println!("Storing update: {:?}", pending_updates);
            let pending_updates_key =
                LogKey::serialize_pending_update(indexes.uncommitted_index, indexes.sequence_id);
            let pending_updates = match PendingUpdates::new(pending_updates).serialize() {
                Some(pending_updates) => pending_updates,
                None => {
                    error!("Failed to serialize pending updates.");
                    return None;
                }
            };
            indexes.sequence_id += 1;

            let store = self.store.clone();
            if let Err(err) = self
                .spawn_worker(move || {
                    store
                        .db
                        .set(ColumnFamily::Logs, &pending_updates_key, &pending_updates)
                })
                .await
            {
                error!("Failed to write pending update: {:?}", err);
                return None;
            }
        }

        if !is_done {
            (
                State::AppendChanges { changed_accounts },
                Response::AppendEntries(AppendEntriesResponse::Continue),
            )
                .into()
        } else {
            self.request_updates(indexes, changed_accounts).await
        }
    }
}
