use std::collections::hash_map::DefaultHasher;
use std::convert::TryInto;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::PathBuf;
use std::thread;
use std::time::SystemTime;

use rocksdb::{Direction, IteratorMode};
use sha2::{Digest, Sha256};
use store::leb128::Leb128;
use store::serialize::{serialize_blob_key, serialize_temporary_blob_key};
use store::serialize::{BLOB_KEY, TEMP_BLOB_KEY};
use store::{AccountId, BlobEntry, CollectionId, DocumentId, Store, StoreBlob, StoreError};

use crate::RocksDBStore;
pub struct BlobFile {
    pub path: PathBuf,
    pub is_commited: bool,
}

impl BlobFile {
    pub fn new(
        base_path: PathBuf,
        name: &[u8],
        hash_levels: &[usize],
        create_if_missing: bool,
    ) -> std::io::Result<Self> {
        let mut path = base_path;
        let mut hash_pos = 0;
        for hash_level in hash_levels {
            let mut path_buf = String::with_capacity(10);
            for _ in 0..*hash_level {
                path_buf.push_str(&format!(
                    "{:02x}",
                    name.get(hash_pos).ok_or_else(|| std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Invalid hash"
                    ))?
                ));
                hash_pos += 1;
            }
            path.push(path_buf);
        }

        if create_if_missing {
            std::fs::create_dir_all(&path)?;
        }

        path.push(base32::encode(
            base32::Alphabet::RFC4648 { padding: false },
            name,
        ));

        Ok(Self {
            path,
            is_commited: true,
        })
    }

    pub fn get_path(&self) -> &PathBuf {
        &self.path
    }

    pub fn delete(&mut self) -> std::io::Result<()> {
        self.is_commited = true;
        std::fs::remove_file(&self.path)
    }

    pub fn commit(mut self) -> Self {
        self.is_commited = true;
        self
    }

    pub fn needs_commit(mut self) -> Self {
        self.is_commited = false;
        self
    }
}

impl Drop for BlobFile {
    fn drop(&mut self) {
        if !self.is_commited {
            self.delete().unwrap_or_else(|_| {
                //TODO log error properly
                println!("Failed to remove blob file: {}", self.path.display());
            });
        }
    }
}

pub fn serialize_blob_keys_from_value(bytes: &[u8]) -> Option<Vec<Vec<u8>>> {
    let num_entries = bytes.len() / (32 + std::mem::size_of::<u32>());
    let mut result = Vec::with_capacity(num_entries);

    for pos in 0..num_entries {
        let start_offset = pos * (32 + std::mem::size_of::<u32>());
        let end_offset = (pos + 1) * (32 + std::mem::size_of::<u32>());

        let mut key = Vec::with_capacity(32 + std::mem::size_of::<u32>() + BLOB_KEY.len());
        key.extend_from_slice(BLOB_KEY);
        key.extend_from_slice(bytes.get(start_offset..end_offset)?);

        result.push(key);
    }

    result.into()
}

pub fn deserialize_blob_entry(
    base_path: PathBuf,
    hash_levels: &[usize],
    bytes: &[u8],
    index: usize,
) -> store::Result<(BlobFile, usize)> {
    let start_offset = index * (32 + std::mem::size_of::<u32>());
    let end_offset = (index + 1) * (32 + std::mem::size_of::<u32>());

    Ok((
        BlobFile::new(
            base_path,
            bytes
                .get(start_offset..end_offset)
                .ok_or(StoreError::DataCorruption)?,
            hash_levels,
            false,
        )
        .map_err(|err| StoreError::DeserializeError(err.to_string()))?,
        u32::from_le_bytes(
            bytes
                .get(start_offset + 32..start_offset + 32 + std::mem::size_of::<u32>())
                .ok_or(StoreError::DataCorruption)?
                .try_into()
                .map_err(|e| StoreError::DeserializeError(format!("{:}", e)))?,
        ) as usize,
    ))
}

impl StoreBlob for RocksDBStore {
    fn get_blobs(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        entries: impl Iterator<Item = BlobEntry<Option<Range<usize>>>>,
    ) -> store::Result<Vec<BlobEntry<Vec<u8>>>> {
        let mut result = Vec::with_capacity(entries.size_hint().0);

        let blob_entries = if let Some(blob_entries) = self
            .db
            .get_cf(
                &self.get_handle("values")?,
                &serialize_blob_key(account, collection, document),
            )
            .map_err(|e| StoreError::InternalError(e.into_string()))?
        {
            blob_entries
        } else {
            return Ok(result);
        };

        for entry in entries {
            let (blob_entry, blob_len) = deserialize_blob_entry(
                self.blob_path.clone(),
                &self.config.db_options.hash_levels,
                &blob_entries,
                entry.index,
            )?;

            let mut blob = File::open(&blob_entry.get_path()).map_err(|err| {
                StoreError::InternalError(format!(
                    "Failed to open blob file {}: {:}",
                    blob_entry.get_path().display(),
                    err
                ))
            })?;

            result.push(BlobEntry {
                index: entry.index,
                value: if let Some(range) = &entry.value {
                    let from_offset = if range.start < blob_len {
                        range.start
                    } else {
                        0
                    };
                    let buf_len = std::cmp::min(range.end, blob_len) - from_offset;
                    let mut buf = vec![0; buf_len];

                    if from_offset > 0 {
                        blob.seek(SeekFrom::Start(from_offset as u64))
                            .map_err(|err| {
                                StoreError::InternalError(format!(
                                    "Failed to seek blob file {} to offset {}: {:}",
                                    blob_entry.get_path().display(),
                                    from_offset,
                                    err
                                ))
                            })?;
                    }
                    blob.read_exact(&mut buf).map_err(|err| {
                        StoreError::InternalError(format!(
                            "Failed to read blob file {} at offset {}: {:}",
                            blob_entry.get_path().display(),
                            from_offset,
                            err
                        ))
                    })?;
                    buf
                } else {
                    let mut buf = Vec::with_capacity(blob_len);
                    blob.read_to_end(&mut buf).map_err(|err| {
                        StoreError::InternalError(format!(
                            "Failed to read blob file {}: {:}",
                            blob_entry.get_path().display(),
                            err
                        ))
                    })?;
                    buf
                },
            });
        }

        Ok(result)
    }

    fn purge_blobs(&self) -> store::Result<()> {
        let cf_values = self.get_handle("values")?;
        let mut batch = rocksdb::WriteBatch::default();
        let current_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| StoreError::InternalError("Failed to get current timestamp".into()))?
            .as_secs();

        for (key, value) in self.db.iterator_cf(
            &cf_values,
            IteratorMode::From(TEMP_BLOB_KEY, Direction::Forward),
        ) {
            if key.starts_with(TEMP_BLOB_KEY) {
                let (timestamp, _) = u64::from_leb128_bytes(&value[TEMP_BLOB_KEY.len()..])
                    .ok_or_else(|| {
                        StoreError::InternalError(format!(
                            "Failed to deserialize timestamp from key {:?}",
                            key
                        ))
                    })?;
                if (current_time >= timestamp
                    && current_time - timestamp > self.config.db_options.temp_blob_ttl)
                    || (current_time < timestamp
                        && timestamp - current_time > self.config.db_options.temp_blob_ttl)
                {
                    batch.delete_cf(&cf_values, key);
                    let mut blob_key = Vec::with_capacity(value.len() + BLOB_KEY.len());
                    blob_key.extend_from_slice(BLOB_KEY);
                    blob_key.extend_from_slice(&value);
                    batch.merge_cf(&cf_values, &blob_key, &(-1i64).to_le_bytes());
                }
            } else {
                break;
            }
        }

        if !batch.is_empty() {
            self.db
                .write(batch)
                .map_err(|e| StoreError::InternalError(e.to_string()))?;
        }

        for (key, value) in self
            .db
            .iterator_cf(&cf_values, IteratorMode::From(BLOB_KEY, Direction::Forward))
        {
            if key.starts_with(BLOB_KEY) {
                let value = i64::from_le_bytes(value.as_ref().try_into().map_err(|err| {
                    StoreError::InternalError(format!(
                        "Failed to convert blob key to i64: {:}",
                        err
                    ))
                })?);
                debug_assert!(value >= 0);
                if value == 0 {
                    self.db.delete_cf(&cf_values, &key).map_err(|err| {
                        StoreError::InternalError(format!("Failed to delete blob key: {:}", err))
                    })?;

                    BlobFile::new(
                        self.blob_path.clone(),
                        &key[BLOB_KEY.len()..],
                        &self.config.db_options.hash_levels,
                        false,
                    )
                    .map_err(|err| {
                        StoreError::InternalError(format!("Failed to create blob file: {:}", err))
                    })?
                    .delete()
                    .map_err(|err| {
                        StoreError::InternalError(format!("Failed to delete blob file: {:}", err))
                    })?;
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    fn store_temporary_blob(&self, account: AccountId, bytes: &[u8]) -> store::Result<(u64, u32)> {
        let mut batch = rocksdb::WriteBatch::default();
        let blob_key = self.store_blob(bytes)?;
        let cf_values = self.get_handle("values")?;

        // Obtain second from Unix epoch
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Generate unique id for the temporary blob
        let mut s = DefaultHasher::new();
        thread::current().id().hash(&mut s);
        SystemTime::now().hash(&mut s);
        let blob_id = s.finish() as u32;

        // Increment blob count
        batch.merge_cf(&cf_values, &blob_key, (1i64).to_le_bytes());

        batch.put_cf(
            &cf_values,
            &serialize_temporary_blob_key(account, blob_id, timestamp),
            &blob_key[BLOB_KEY.len()..],
        );

        self.db
            .write(batch)
            .map_err(|e| StoreError::InternalError(e.to_string()))?;

        Ok((timestamp, blob_id))
    }

    fn get_temporary_blob(
        &self,
        account: AccountId,
        blob_id: DocumentId,
        timestamp: u64,
    ) -> store::Result<Option<Vec<u8>>> {
        if let Some(blob_key) = self
            .db
            .get_cf(
                &self.get_handle("values")?,
                &serialize_temporary_blob_key(account, blob_id, timestamp),
            )
            .map_err(|e| StoreError::InternalError(e.into_string()))?
        {
            let (blob_entry, blob_len) = deserialize_blob_entry(
                self.blob_path.clone(),
                &self.config.db_options.hash_levels,
                &blob_key,
                0,
            )?;

            let mut blob = File::open(&blob_entry.get_path()).map_err(|err| {
                StoreError::InternalError(format!(
                    "Failed to open blob file {}: {:}",
                    blob_entry.get_path().display(),
                    err
                ))
            })?;

            let mut buf = Vec::with_capacity(blob_len);
            blob.read_to_end(&mut buf).map_err(|err| {
                StoreError::InternalError(format!(
                    "Failed to read blob file {}: {:}",
                    blob_entry.get_path().display(),
                    err
                ))
            })?;

            Ok(Some(buf))
        } else {
            Ok(None)
        }
    }

    fn store_blob(&self, bytes: &[u8]) -> store::Result<Vec<u8>> {
        // Create blob key
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let mut blob_key = Vec::with_capacity(32 + std::mem::size_of::<u32>() + BLOB_KEY.len());
        blob_key.extend_from_slice(BLOB_KEY);
        blob_key.extend_from_slice(&hasher.finalize());
        blob_key.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        let blob_hash = &blob_key[BLOB_KEY.len()..];

        // Lock blob key
        let _blob_lock = self
            .blob_lock
            .lock_hash(blob_hash)
            .map_err(|_| StoreError::InternalError("Failed to obtain mutex".to_string()))?;

        // Check whether the blob is already stored
        let cf_values = self.get_handle("values")?;
        if self
            .db
            .get_cf(&cf_values, &blob_key)
            .map_err(|e| StoreError::InternalError(e.into_string()))?
            .is_none()
        {
            let blob = BlobFile::new(
                self.blob_path.clone(),
                blob_hash,
                &self.get_config().db_options.hash_levels,
                true,
            )
            .map_err(|err| {
                StoreError::InternalError(format!("Failed to create blob file: {:?}", err))
            })?
            .needs_commit();
            let mut blob_file = File::create(blob.get_path()).map_err(|err| {
                StoreError::InternalError(format!(
                    "Failed to create blob file {:?}: {:?}",
                    blob.get_path().display(),
                    err
                ))
            })?;
            blob_file.write_all(bytes).map_err(|err| {
                StoreError::InternalError(format!(
                    "Failed to write blob file {:?}: {:?}",
                    blob.get_path().display(),
                    err
                ))
            })?;
            blob_file.flush().map_err(|err| {
                StoreError::InternalError(format!(
                    "Failed to flush blob file {:?}: {:?}",
                    blob.get_path().display(),
                    err
                ))
            })?;

            // Create blob key
            self.db
                .put_cf(&cf_values, &blob_key, &(0i64).to_le_bytes())
                .map_err(|e| StoreError::InternalError(e.to_string()))?;
            blob.commit();
        }

        Ok(blob_key)
    }
}

#[cfg(test)]
use store::StoreBlobTest;
#[cfg(test)]
impl StoreBlobTest for RocksDBStore {
    fn get_all_blobs(&self) -> store::Result<Vec<(std::path::PathBuf, i64)>> {
        let cf_values = self.get_handle("values")?;
        let mut result = Vec::new();

        for (key, value) in self
            .db
            .iterator_cf(&cf_values, IteratorMode::From(BLOB_KEY, Direction::Forward))
        {
            if key.starts_with(BLOB_KEY) {
                let value = i64::from_le_bytes(value.as_ref().try_into().map_err(|err| {
                    StoreError::InternalError(format!(
                        "Failed to convert blob key to i64: {:}",
                        err
                    ))
                })?);

                result.push((
                    BlobFile::new(
                        self.blob_path.clone(),
                        &key[BLOB_KEY.len()..],
                        &self.config.db_options.hash_levels,
                        false,
                    )
                    .map_err(|err| {
                        StoreError::InternalError(format!("Failed to create blob file: {:}", err))
                    })?
                    .path
                    .clone(),
                    value,
                ));
            } else {
                break;
            }
        }

        Ok(result)
    }
}
