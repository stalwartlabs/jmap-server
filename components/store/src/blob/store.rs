use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    thread,
    time::SystemTime,
};

use tracing::error;

use crate::{
    batch::{Document, WriteBatch},
    field::{DefaultOptions, Options},
    serialize::{StoreSerialize, ValueKey},
    AccountId, Collection, ColumnFamily, DocumentId, JMAPId, JMAPStore, Store, Tag, WriteOperation,
};

use super::{BlobId, BlobStore, BlobStoreType};

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn store_blob(&self, blob_id: BlobId, bytes: &[u8]) -> crate::Result<()> {
        let _lock = self.blob.lock.lock_hash(&blob_id.hash);
        match &self.blob.store {
            BlobStoreType::Local(local_store) => {
                if local_store.put(&blob_id, bytes)? {
                    if let Err(err) = self.db.set(
                        ColumnFamily::Values,
                        &ValueKey::serialize_blob(&blob_id),
                        &0i64.serialize().unwrap(),
                    ) {
                        if let Err(err) = local_store.delete(&blob_id) {
                            error!("Failed to delete blob {}: {:?}", blob_id, err);
                        }
                        return Err(err);
                    }

                    let value = blob_id.serialize().unwrap();
                    let document_id = self.assign_document_id(AccountId::MAX, Collection::Blob)?;

                    let mut document = Document::new(Collection::Blob, document_id);
                    document.binary(0, value.clone(), DefaultOptions::new().store());
                    document.tag(0, Tag::Bytes(value), DefaultOptions::new());

                    let mut batch = WriteBatch::new(AccountId::MAX, self.config.is_in_cluster);
                    batch.insert_document(document);
                    batch.log_insert(Collection::Blob, document_id as JMAPId);

                    self.write(batch)?;
                }
            }
            BlobStoreType::S3(s3_store) => {
                if s3_store.put(&blob_id, bytes)? {
                    if let Err(err) = self.db.set(
                        ColumnFamily::Values,
                        &ValueKey::serialize_blob(&blob_id),
                        &0i64.serialize().unwrap(),
                    ) {
                        if let Err(err) = s3_store.delete(&blob_id) {
                            error!("Failed to delete blob {}: {:?}", blob_id, err);
                        }
                        return Err(err);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn link_temporary_blob(
        &self,
        account_id: AccountId,
        blob_id: &BlobId,
    ) -> crate::Result<(u64, u64)> {
        let mut batch = Vec::with_capacity(2);

        // Obtain second from Unix epoch
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Generate unique id for the temporary blob
        let mut s = DefaultHasher::new();
        thread::current().id().hash(&mut s);
        SystemTime::now().hash(&mut s);
        let hash = s.finish();

        // Increment blob count
        batch.push(WriteOperation::Merge {
            cf: ColumnFamily::Values,
            key: ValueKey::serialize_blob(blob_id),
            value: (1i64).serialize().unwrap(),
        });
        batch.push(WriteOperation::Set {
            cf: ColumnFamily::Values,
            key: ValueKey::serialize_temporary_blob(account_id, hash, timestamp),
            value: blob_id.serialize().unwrap(),
        });

        self.db.write(batch)?;

        Ok((timestamp, hash))
    }

    pub fn get_temporary_blob_id(
        &self,
        account_id: AccountId,
        hash: u64,
        timestamp: u64,
    ) -> crate::Result<Option<BlobId>> {
        self.db.get::<BlobId>(
            ColumnFamily::Values,
            &ValueKey::serialize_temporary_blob(account_id, hash, timestamp),
        )
    }

    pub fn get_owned_blob_id(
        &self,
        account_id: AccountId,
        collection: Collection,
        document: DocumentId,
        index: u32,
    ) -> crate::Result<Option<BlobId>> {
        self.db.get::<BlobId>(
            ColumnFamily::Values,
            &ValueKey::serialize_document_blob(account_id, collection, document, index),
        )
    }
}
