use super::rpc::Response;
use super::{RaftIndexes, State};
use crate::cluster::log::changes_merge::RaftStoreMerge;
use crate::cluster::log::{AppendEntriesResponse, PendingUpdate, PendingUpdates};
use crate::JMAPServer;
use store::core::bitmap::Bitmap;
use store::core::collection::Collection;
use store::log::raft::LogIndex;
use store::roaring::RoaringBitmap;
use store::serialize::key::LogKey;
use store::serialize::StoreSerialize;
use store::tracing::{debug, error};
use store::{AccountId, ColumnFamily, Store};

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn request_updates(
        &self,
        indexes: &mut RaftIndexes,
        mut changed_accounts: Vec<(AccountId, Bitmap<Collection>)>,
    ) -> Option<(State, Response)> {
        loop {
            let (account_id, collection) =
                if let Some((account_id, collections)) = changed_accounts.last_mut() {
                    if let Some(collection) = collections.pop() {
                        if matches!(collection, Collection::Thread) {
                            continue;
                        }
                        (*account_id, collection)
                    } else {
                        changed_accounts.pop();
                        continue;
                    }
                } else {
                    return self.commit_updates(indexes).await;
                };

            debug!(
                "Merging changes for account {}, collection {:?} from index {} to {}.",
                account_id, collection, indexes.merge_index, indexes.uncommitted_index
            );
            debug_assert!(indexes.merge_index != LogIndex::MAX);
            debug_assert!(indexes.uncommitted_index != LogIndex::MAX);

            let store = self.store.clone();
            let merge_index = indexes.merge_index;
            let uncommitted_index = indexes.uncommitted_index;
            match self
                .spawn_worker(move || {
                    store.merge_changes(account_id, collection, merge_index, uncommitted_index)
                })
                .await
            {
                Ok(mut changes) => {
                    if !changes.deletes.is_empty() {
                        let pending_updates_key = LogKey::serialize_pending_update(
                            indexes.uncommitted_index,
                            indexes.sequence_id,
                        );
                        let pending_updates = match PendingUpdates::new(vec![
                            PendingUpdate::Begin {
                                account_id,
                                collection,
                            },
                            PendingUpdate::Delete {
                                document_ids: changes.deletes.into_iter().collect(),
                            },
                        ])
                        .serialize()
                        {
                            Some(pending_updates) => pending_updates,
                            None => {
                                error!("Failed to serialize pending updates.");
                                return None;
                            }
                        };

                        let store = self.store.clone();
                        if let Err(err) = self
                            .spawn_worker(move || {
                                store.db.set(
                                    ColumnFamily::Logs,
                                    &pending_updates_key,
                                    &pending_updates,
                                )
                            })
                            .await
                        {
                            error!("Failed to write pending update: {:?}", err);
                            return None;
                        }

                        indexes.sequence_id += 1;
                        changes.deletes = RoaringBitmap::new();
                    }

                    if !changes.inserts.is_empty() || !changes.updates.is_empty() {
                        return (
                            State::AppendChanges { changed_accounts },
                            Response::AppendEntries(AppendEntriesResponse::Update {
                                account_id,
                                collection,
                                changes: match changes.serialize() {
                                    Some(changes) => changes,
                                    None => {
                                        error!("Failed to serialize bitmap.");
                                        return None;
                                    }
                                },
                                is_rollback: false,
                            }),
                        )
                            .into();
                    } else {
                        continue;
                    }
                }
                Err(err) => {
                    error!("Error getting raft changes: {:?}", err);
                    return None;
                }
            }
        }
    }
}
