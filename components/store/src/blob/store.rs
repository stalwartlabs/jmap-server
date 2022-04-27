use std::time::SystemTime;

use tracing::error;

use crate::{
    serialize::{BlobKey, StoreSerialize},
    AccountId, ColumnFamily, Direction, JMAPStore, Store,
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
            .set(ColumnFamily::Values, &key, &timestamp.serialize().unwrap())
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

    pub fn blob_has_access(&self, blob_id: &BlobId, account_id: AccountId) -> crate::Result<bool> {
        let key = BlobKey::serialize_prefix(blob_id, account_id);

        if let Some((key, _)) = self
            .db
            .iterator(ColumnFamily::Blobs, &key, Direction::Forward)?
            .next()
        {
            if key.starts_with(&key) {
                return Ok(true);
            }
        }

        Ok(false)
    }
}
