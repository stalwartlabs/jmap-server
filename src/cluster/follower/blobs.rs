use store::ahash::AHashSet;
use store::blob::BlobId;
use store::core::bitmap::Bitmap;
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::tracing::error;
use store::{AccountId, Store};

use crate::cluster::log::AppendEntriesResponse;
use crate::JMAPServer;

use crate::cluster::log::Update;

use super::rpc::Response;
use super::{RaftIndexes, State};

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn handle_missing_blobs(
        &self,
        indexes: &mut RaftIndexes,
        changed_accounts: Vec<(AccountId, Bitmap<Collection>)>,
        mut pending_blobs: AHashSet<BlobId>,
        pending_updates: Vec<Update>,
        updates: Vec<Update>,
    ) -> Option<(State, Response)> {
        let store = self.store.clone();
        match self
            .spawn_worker(move || {
                for update in updates {
                    match update {
                        Update::Blob { blob_id, blob } => {
                            if pending_blobs.remove(&blob_id) {
                                let blob = store::lz4_flex::decompress_size_prepended(&blob)
                                    .map_err(|_| {
                                        StoreError::InternalError(format!(
                                            "Failed to decompress blobId {}.",
                                            blob_id
                                        ))
                                    })?;
                                let saved_blob_id = if blob_id.is_local() {
                                    BlobId::new_local(&blob)
                                } else {
                                    BlobId::new_external(&blob)
                                };
                                if blob_id == saved_blob_id {
                                    store.blob_store(&saved_blob_id, blob)?;
                                } else {
                                    return Err(StoreError::InternalError(format!(
                                        "BlobId {} was saved with Id {}.",
                                        blob_id, saved_blob_id
                                    )));
                                }
                            } else {
                                debug_assert!(
                                    false,
                                    "Received unexpected blobId: {}, pending {}.",
                                    blob_id,
                                    pending_blobs.len()
                                );
                            }
                        }
                        _ => {
                            debug_assert!(false, "Invalid update: {:?}", update);
                        }
                    }
                }
                Ok(pending_blobs)
            })
            .await
        {
            Ok(pending_blobs) => {
                if pending_blobs.is_empty() {
                    self.handle_pending_updates(indexes, changed_accounts, pending_updates)
                        .await
                } else {
                    (
                        State::AppendBlobs {
                            pending_blobs,
                            pending_updates,
                            changed_accounts,
                        },
                        Response::AppendEntries(AppendEntriesResponse::Continue),
                    )
                        .into()
                }
            }
            Err(err) => {
                error!("Failed to write blobs: {:?}", err);
                None
            }
        }
    }
}
