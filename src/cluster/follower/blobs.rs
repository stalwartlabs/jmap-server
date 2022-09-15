/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

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
