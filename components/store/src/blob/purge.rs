use std::time::SystemTime;

use crate::blob::BlobId;
use crate::leb128::Leb128;
use crate::serialize::StoreDeserialize;
use crate::serialize::{StoreSerialize, BLOB_KEY_PREFIX};
use crate::WriteOperation;
use crate::{
    serialize::TEMP_BLOB_KEY_PREFIX, ColumnFamily, Direction, JMAPStore, Store, StoreError,
};

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn purge_blobs(&self) -> crate::Result<Vec<BlobId>> {
        let mut batch = Vec::new();
        let current_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| StoreError::InternalError("Failed to get current timestamp".into()))?
            .as_secs();

        for (key, value) in self.db.iterator(
            ColumnFamily::Values,
            TEMP_BLOB_KEY_PREFIX,
            Direction::Forward,
        )? {
            if key.starts_with(TEMP_BLOB_KEY_PREFIX) {
                let (timestamp, _) = u64::from_leb128_bytes(&key[TEMP_BLOB_KEY_PREFIX.len()..])
                    .ok_or_else(|| {
                        StoreError::InternalError(format!(
                            "Failed to deserialize timestamp from key {:?}",
                            key
                        ))
                    })?;
                if (current_time >= timestamp
                    && current_time - timestamp > self.config.blob_temp_ttl)
                    || (current_time < timestamp
                        && timestamp - current_time > self.config.blob_temp_ttl)
                {
                    batch.push(WriteOperation::Delete {
                        cf: ColumnFamily::Values,
                        key: key.into(),
                    });
                    batch.push(WriteOperation::Merge {
                        cf: ColumnFamily::Values,
                        key: value.to_vec(),
                        value: (-1i64).serialize().unwrap(),
                    });
                }
            } else {
                break;
            }
        }

        if !batch.is_empty() {
            self.db.write(batch)?;
        }

        let mut delete_blobs = Vec::new();
        for (key, value) in
            self.db
                .iterator(ColumnFamily::Values, BLOB_KEY_PREFIX, Direction::Forward)?
        {
            if key.starts_with(BLOB_KEY_PREFIX) {
                let value = i64::deserialize(&value).ok_or_else(|| {
                    StoreError::InternalError("Failed to convert blob key to i64".to_string())
                })?;
                debug_assert!(value >= 0);
                if value == 0 {
                    self.db.delete(ColumnFamily::Values, &key)?;
                    delete_blobs.push(
                        BlobId::deserialize(&key[BLOB_KEY_PREFIX.len()..])
                            .ok_or(StoreError::DataCorruption)?,
                    );
                }
            } else {
                break;
            }
        }
        Ok(delete_blobs)
    }
}
