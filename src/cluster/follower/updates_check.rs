use super::rpc::Response;
use super::{RaftIndexes, State};
use crate::cluster::log::AppendEntriesResponse;
use crate::cluster::log::Update;
use crate::JMAPServer;
use jmap::jmap_store::raft::RaftUpdate;
use store::ahash::AHashSet;
use store::core::bitmap::Bitmap;
use store::core::collection::Collection;
use store::tracing::error;
use store::{AccountId, Store};

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn check_pending_updates(
        &self,
        indexes: &mut RaftIndexes,
        changed_accounts: Vec<(AccountId, Bitmap<Collection>)>,
        updates: Vec<Update>,
    ) -> Option<(State, Response)> {
        // Request any missing blobs
        let store = self.store.clone();
        match self
            .spawn_worker(move || {
                let mut missing_blob_ids = AHashSet::default();

                for update in &updates {
                    match update {
                        Update::Document {
                            update:
                                RaftUpdate::Insert {
                                    blobs, term_index, ..
                                },
                        } if !blobs.is_empty() || term_index.is_some() => {
                            for blob in blobs {
                                if !store.blob_exists(blob)? {
                                    missing_blob_ids.insert(blob.clone());
                                }
                            }
                            if let Some(term_index) = term_index {
                                if !store.blob_exists(term_index)? {
                                    missing_blob_ids.insert(term_index.clone());
                                }
                            }
                        }
                        _ => (),
                    }
                }

                Ok((updates, missing_blob_ids.into_iter().collect::<Vec<_>>()))
            })
            .await
        {
            Ok((updates, missing_blob_ids)) => {
                if !missing_blob_ids.is_empty() {
                    Some((
                        State::AppendBlobs {
                            pending_blobs: missing_blob_ids.iter().cloned().collect(),
                            pending_updates: updates,
                            changed_accounts,
                        },
                        Response::AppendEntries(AppendEntriesResponse::FetchBlobs {
                            blob_ids: missing_blob_ids,
                        }),
                    ))
                } else {
                    self.handle_pending_updates(indexes, changed_accounts, updates)
                        .await
                }
            }
            Err(err) => {
                error!("Failed to verify blobs: {:?}", err);
                None
            }
        }
    }
}
