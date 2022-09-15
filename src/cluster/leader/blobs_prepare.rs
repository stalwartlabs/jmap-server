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

use crate::cluster::log::Update;
use crate::JMAPServer;
use store::blob::BlobId;
use store::core::error::StoreError;
use store::Store;

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn prepare_blobs(
        &self,
        pending_blob_ids: Vec<BlobId>,
        max_batch_size: usize,
    ) -> store::Result<(Vec<Update>, Vec<BlobId>)> {
        let store = self.store.clone();
        self.spawn_worker(move || {
            let mut remaining_blobs = Vec::new();
            let mut updates = Vec::new();
            let mut bytes_sent = 0;

            for pending_blob_id in pending_blob_ids {
                if bytes_sent < max_batch_size {
                    let blob = store::lz4_flex::compress_prepend_size(
                        &store.blob_get(&pending_blob_id)?.ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Blob {} not found.",
                                pending_blob_id
                            ))
                        })?,
                    );
                    bytes_sent += blob.len();
                    updates.push(Update::Blob {
                        blob_id: pending_blob_id,
                        blob,
                    });
                } else {
                    remaining_blobs.push(pending_blob_id);
                }
            }

            Ok((updates, remaining_blobs))
        })
        .await
    }
}
