use std::{ops::Range, time::SystemTime};

use roaring::RoaringBitmap;
use tracing::error;

use crate::serialize::leb128::Leb128;
use crate::{
    core::collection::Collection,
    serialize::{key::BlobKey, StoreSerialize},
    AccountId, ColumnFamily, Direction, DocumentId, JMAPStore, Store,
};

use super::{BlobId, BlobStore, BlobStoreType};

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn blob_store(&self, bytes: &[u8]) -> crate::Result<BlobId> {
        let blob_id: BlobId = bytes.into();
        let key = BlobKey::serialize(&blob_id);

        // Lock blob hash
        let _lock = self.blob.lock.lock_hash(&blob_id.hash);

        // Blob already exists, return.
        if self.db.exists(ColumnFamily::Blobs, &key)? {
            return Ok(blob_id);
        }

        match &self.blob.store {
            BlobStoreType::Local(local_store) => local_store.put(&blob_id, bytes)?,
            BlobStoreType::S3(s3_store) => s3_store.put(&blob_id, bytes)?,
        };

        // Obtain seconds from Unix epoch
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Store blobId including a timestamp
        if let Err(err) = self
            .db
            .set(ColumnFamily::Blobs, &key, &timestamp.serialize().unwrap())
        {
            // There was a problem writing to the store, delete blob.
            if let Err(err) = match &self.blob.store {
                BlobStoreType::Local(local_store) => local_store.delete(&blob_id),
                BlobStoreType::S3(s3_store) => s3_store.delete(&blob_id),
            } {
                error!("Failed to delete blob {}: {:?}", blob_id, err);
            }
            return Err(err);
        }

        Ok(blob_id)
    }

    pub fn blob_exists(&self, blob_id: &BlobId) -> crate::Result<bool> {
        self.db
            .exists(ColumnFamily::Blobs, &BlobKey::serialize(blob_id))
    }

    pub fn blob_link_ephimeral(
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
        match &self.blob.store {
            BlobStoreType::Local(local_store) => local_store.get(blob_id),
            BlobStoreType::S3(s3_store) => s3_store.get(blob_id),
        }
    }

    pub fn blob_get_range(
        &self,
        blob_id: &BlobId,
        range: Range<u32>,
    ) -> crate::Result<Option<Vec<u8>>> {
        match &self.blob.store {
            BlobStoreType::Local(local_store) => local_store.get_range(blob_id, range),
            BlobStoreType::S3(s3_store) => s3_store.get_range(blob_id, range),
        }
    }

    pub fn blob_account_has_access(
        &self,
        blob_id: &BlobId,
        account_id: AccountId,
    ) -> crate::Result<bool> {
        let prefix = BlobKey::serialize_prefix(blob_id, account_id);

        if let Some((key, _)) = self
            .db
            .iterator(ColumnFamily::Blobs, &prefix, Direction::Forward)?
            .next()
        {
            if key.starts_with(&prefix) {
                return Ok(true);
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
                if let Some((document_id, _)) = DocumentId::from_leb128_bytes(&key[prefix.len()..])
                {
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
}
