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

use super::rpc::Response;
use super::{RaftIndexes, State};
use crate::cluster::log::Update;
use crate::cluster::log::{AppendEntriesResponse, DocumentUpdate};
use crate::JMAPServer;
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
                                DocumentUpdate::Insert {
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
