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

use std::{ops::Range, time::SystemTime};

use roaring::RoaringBitmap;
use tracing::error;

use crate::serialize::leb128::Leb128Reader;
use crate::write::operation::WriteOperation;
use crate::{
    core::collection::Collection,
    serialize::{key::BlobKey, StoreSerialize},
    AccountId, ColumnFamily, Direction, DocumentId, JMAPStore, Store,
};

use super::{BlobId, BlobStore};

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn blob_store(&self, blob_id: &BlobId, bytes: Vec<u8>) -> crate::Result<()> {
        let key = BlobKey::serialize(blob_id);

        // Lock blob hash
        let _lock = self.blob_store.lock.lock_hash(blob_id);

        // Blob already exists, return.
        if self.db.exists(ColumnFamily::Blobs, &key)? {
            return Ok(());
        }

        // Write blob
        let value = if blob_id.is_external() {
            self.blob_store.put(blob_id, &bytes)?;
            Vec::new()
        } else {
            bytes
        };

        // Write blob or blob reference to database
        let mut batch = Vec::with_capacity(2);
        batch.push(WriteOperation::Set {
            cf: ColumnFamily::Blobs,
            key,
            value,
        });
        // Obtain seconds from Unix epoch
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        batch.push(WriteOperation::Set {
            cf: ColumnFamily::Blobs,
            key: BlobKey::serialize_prefix(blob_id, 0),
            value: timestamp.serialize().unwrap(),
        });

        // Store blobId including a timestamp
        if let Err(err) = self.db.write(batch) {
            // There was a problem writing to the store, delete blob.
            if blob_id.is_external() {
                if let Err(err) = self.blob_store.delete(blob_id) {
                    error!("Failed to delete blob {}: {:?}", blob_id, err);
                }
            }
            return Err(err);
        }

        Ok(())
    }

    pub fn blob_exists(&self, blob_id: &BlobId) -> crate::Result<bool> {
        self.db
            .exists(ColumnFamily::Blobs, &BlobKey::serialize(blob_id))
    }

    pub fn blob_link_ephemeral(
        &self,
        blob_id: &BlobId,
        account_id: AccountId,
    ) -> crate::Result<()> {
        // Obtain seconds from Unix epoch
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        self.db.set(
            ColumnFamily::Blobs,
            &BlobKey::serialize_prefix(blob_id, account_id),
            &timestamp.serialize().unwrap(),
        )
    }

    pub fn blob_get(&self, blob_id: &BlobId) -> crate::Result<Option<Vec<u8>>> {
        if !blob_id.is_local() {
            self.blob_store.get(blob_id)
        } else {
            self.db
                .get(ColumnFamily::Blobs, &BlobKey::serialize(blob_id))
        }
    }

    pub fn blob_get_range(
        &self,
        blob_id: &BlobId,
        range: Range<u32>,
    ) -> crate::Result<Option<Vec<u8>>> {
        if !blob_id.is_local() {
            self.blob_store.get_range(blob_id, range)
        } else {
            Ok(self
                .db
                .get::<Vec<u8>>(ColumnFamily::Blobs, &BlobKey::serialize(blob_id))?
                .and_then(|bytes| {
                    bytes
                        .get(range.start as usize..range.end as usize)
                        .map(|bytes| bytes.to_vec())
                }))
        }
    }

    pub fn blob_account_has_access(
        &self,
        blob_id: &BlobId,
        account_ids: &[AccountId],
    ) -> crate::Result<bool> {
        let prefix = BlobKey::serialize_prefix(blob_id, AccountId::MAX);

        for (key, _) in self
            .db
            .iterator(ColumnFamily::Blobs, &prefix, Direction::Forward)?
        {
            if key.starts_with(&prefix) {
                if key.len() > prefix.len() {
                    if let Some((account_id, _)) = (&key[prefix.len()..]).read_leb128() {
                        if account_ids.contains(&account_id) {
                            return Ok(true);
                        }
                    } else {
                        break;
                    }
                }
            } else {
                break;
            }
        }

        Ok(false)
    }

    pub fn blob_document_has_access(
        &self,
        blob_id: &BlobId,
        account_id: AccountId,
        collection: Collection,
        documents: &RoaringBitmap,
    ) -> crate::Result<bool> {
        let prefix = BlobKey::serialize_collection(blob_id, account_id, collection);

        for (key, _) in self
            .db
            .iterator(ColumnFamily::Blobs, &prefix, Direction::Forward)?
        {
            if key.starts_with(&prefix) && key.len() > prefix.len() {
                if let Some((document_id, _)) = (&key[prefix.len()..]).read_leb128() {
                    if documents.contains(document_id) {
                        return Ok(true);
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        Ok(false)
    }

    pub fn blob_any_linked_document(
        &self,
        blob_id: &BlobId,
        account_id: AccountId,
        collection: Collection,
    ) -> crate::Result<Option<DocumentId>> {
        let prefix = BlobKey::serialize_collection(blob_id, account_id, collection);

        if let Some((key, _)) = self
            .db
            .iterator(ColumnFamily::Blobs, &prefix, Direction::Forward)?
            .next()
        {
            if key.starts_with(&prefix) && key.len() > prefix.len() {
                if let Some((document_id, _)) = (&key[prefix.len()..]).read_leb128() {
                    return Ok(Some(document_id));
                }
            }
        }

        Ok(None)
    }
}
