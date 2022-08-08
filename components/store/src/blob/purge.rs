use std::time::SystemTime;

use tracing::error;

use crate::serialize::leb128::skip_leb128_value;
use crate::serialize::StoreDeserialize;
use crate::WriteOperation;
use crate::{ColumnFamily, Direction, JMAPStore, Store, StoreError};

use super::{BlobId, BlobStore, BlobStoreType, BLOB_EXTERNAL, BLOB_HASH_LEN};

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn purge_blobs(&self) -> crate::Result<()> {
        let mut batch = Vec::with_capacity(16);
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| StoreError::InternalError("Failed to get current timestamp".into()))?
            .as_secs();

        let mut blob_id = vec![0u8; BLOB_HASH_LEN + 1];
        let mut blob_link_count = u32::MAX;
        let mut _blob_lock = None;

        for (key, value) in self
            .db
            .iterator(ColumnFamily::Blobs, &[], Direction::Forward)?
        {
            if key.len() < BLOB_HASH_LEN + 1 {
                continue;
            }

            if key[..BLOB_HASH_LEN + 1] != blob_id {
                batch = self.delete_blobs(batch, &blob_id, blob_link_count)?;
                blob_link_count = 0;
                blob_id.copy_from_slice(&key[..BLOB_HASH_LEN + 1]);
                _blob_lock = self.blob.lock.lock_hash(&blob_id).into();
            }

            // Blob link
            if key.len() > BLOB_HASH_LEN + 1 {
                if let Some(bytes_read) = skip_leb128_value(&key[BLOB_HASH_LEN + 1..]) {
                    if key.len() == BLOB_HASH_LEN + 1 + bytes_read {
                        let timestamp = u64::deserialize(&value).ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Failed to deserialize timestamp from key {:?}",
                                key
                            ))
                        })?;

                        if (now >= timestamp && now - timestamp > self.config.blob_temp_ttl)
                            || (now < timestamp && timestamp - now > self.config.blob_temp_ttl)
                        {
                            // Ephimeral link expired, delete reference
                            batch.push(WriteOperation::Delete {
                                cf: ColumnFamily::Blobs,
                                key: key.to_vec(),
                            });
                        } else {
                            blob_link_count += 1;
                        }
                    } else {
                        blob_link_count += 1;
                    }
                }
            }
        }

        self.delete_blobs(batch, &blob_id, blob_link_count)
            .map(|_| ())
    }

    fn delete_blobs(
        &self,
        mut batch: Vec<WriteOperation>,
        blob_id: &[u8],
        blob_link_count: u32,
    ) -> crate::Result<Vec<WriteOperation>> {
        if blob_link_count == 0 {
            // Delete blob
            batch.push(WriteOperation::Delete {
                cf: ColumnFamily::Blobs,
                key: blob_id.to_vec(),
            });

            // Delete external blob
            if blob_id[0] == BLOB_EXTERNAL {
                let blob_id = BlobId::deserialize(blob_id).unwrap();

                if let Err(err) = match &self.blob.store {
                    BlobStoreType::Local(local_store) => local_store.delete(&blob_id),
                    BlobStoreType::S3(s3_store) => s3_store.delete(&blob_id),
                } {
                    error!("Failed to delete blob {}: {:?}", blob_id, err);
                }
            }
        }

        Ok(if !batch.is_empty() {
            self.db.write(batch)?;
            Vec::with_capacity(16)
        } else {
            batch
        })
    }
}
