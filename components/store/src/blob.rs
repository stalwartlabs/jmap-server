use std::{
    collections::hash_map::DefaultHasher,
    convert::TryInto,
    fs::{self, File},
    hash::{Hash, Hasher},
    io::{Read, Seek, SeekFrom, Write},
    ops::Range,
    path::PathBuf,
    thread,
    time::SystemTime,
};

use crate::{leb128::Leb128, serialize::ValueKey};
use crate::{
    serialize::{StoreDeserialize, StoreSerialize, BLOB_KEY_PREFIX, TEMP_BLOB_KEY_PREFIX},
    AccountId, Collection, ColumnFamily, Direction, DocumentId, JMAPStore, Store, StoreError,
    WriteOperation,
};
use sha2::Digest;
use sha2::Sha256;

#[derive(Default, Debug, PartialEq, Eq)]
pub struct BlobEntries {
    pub items: Vec<BlobEntry>,
}

impl BlobEntries {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn add(&mut self, entry: BlobEntry) {
        self.items.push(entry);
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct BlobEntry {
    pub hash: Vec<u8>,
    pub size: u32,
}

pub type BlobIndex = u32;
pub const BLOB_HASH_LEN: usize = 32;

impl StoreSerialize for BlobEntries {
    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buf = Vec::with_capacity(self.items.iter().map(|item| item.hash.len()).sum());
        for item in &self.items {
            buf.extend_from_slice(&item.hash);
        }
        Some(buf)
    }
}

impl StoreDeserialize for BlobEntries {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        let num_entries = bytes.len() / (BLOB_HASH_LEN + std::mem::size_of::<u32>());
        let mut items = Vec::with_capacity(num_entries);

        debug_assert!(num_entries != 0);
        debug_assert_eq!(
            bytes.len(),
            num_entries * (BLOB_HASH_LEN + std::mem::size_of::<u32>()),
            "bytes: {:?}",
            bytes
        );

        for pos in 0..num_entries {
            let start_offset = pos * (BLOB_HASH_LEN + std::mem::size_of::<u32>());
            let end_offset = (pos + 1) * (BLOB_HASH_LEN + std::mem::size_of::<u32>());

            items.push(BlobEntry {
                hash: bytes.get(start_offset..end_offset)?.to_vec(),
                size: u32::from_le_bytes(
                    bytes
                        .get(
                            (start_offset + BLOB_HASH_LEN)
                                ..(start_offset + BLOB_HASH_LEN + std::mem::size_of::<u32>()),
                        )?
                        .try_into()
                        .ok()?,
                ),
            });
        }

        BlobEntries { items }.into()
    }
}

impl BlobEntry {
    pub fn as_key(&self) -> Vec<u8> {
        ValueKey::serialize_blob(&self.hash)
    }

    pub fn as_path(&self, base_path: PathBuf, hash_levels: &[usize]) -> crate::Result<PathBuf> {
        let mut path = base_path;
        let mut hash_pos = 0;
        for hash_level in hash_levels {
            let mut path_buf = String::with_capacity(10);
            for _ in 0..*hash_level {
                path_buf.push_str(&format!(
                    "{:02x}",
                    self.hash
                        .get(hash_pos)
                        .ok_or_else(|| StoreError::InternalError("Invalid hash".to_string()))?
                ));
                hash_pos += 1;
            }
            path.push(path_buf);
        }

        path.push(base32::encode(
            base32::Alphabet::RFC4648 { padding: false },
            &self.hash,
        ));

        Ok(path)
    }

    pub fn size(&self) -> u32 {
        self.size
    }
}

impl From<&[u8]> for BlobEntry {
    fn from(bytes: &[u8]) -> Self {
        // Create blob key
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let mut hash = Vec::with_capacity(BLOB_HASH_LEN + std::mem::size_of::<u32>());
        hash.extend_from_slice(&hasher.finalize());
        hash.extend_from_slice(&(bytes.len() as u32).to_le_bytes());

        debug_assert_eq!(hash.len(), BLOB_HASH_LEN + std::mem::size_of::<u32>());

        BlobEntry {
            hash,
            size: bytes.len() as u32,
        }
    }
}

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn get_blob(
        &self,
        account: AccountId,
        collection: Collection,
        document: DocumentId,
        blob_index: BlobIndex,
    ) -> crate::Result<Option<Vec<u8>>> {
        self.get_blob_range(account, collection, document, blob_index, 0..u32::MAX)
    }

    pub fn get_blob_range(
        &self,
        account: AccountId,
        collection: Collection,
        document: DocumentId,
        blob_index: BlobIndex,
        blob_range: Range<u32>,
    ) -> crate::Result<Option<Vec<u8>>> {
        Ok(self
            .get_blobs(
                account,
                collection,
                document,
                vec![(blob_index, blob_range)],
            )?
            .pop()
            .map(|(_, blob)| blob))
    }

    pub fn get_blobs(
        &self,
        account: AccountId,
        collection: Collection,
        document: DocumentId,
        items: Vec<(BlobIndex, Range<u32>)>,
    ) -> crate::Result<Vec<(BlobIndex, Vec<u8>)>> {
        let mut result = Vec::with_capacity(items.len());

        let blob_entries = if let Some(blob_entries) = self.db.get::<BlobEntries>(
            ColumnFamily::Values,
            &ValueKey::serialize_document_blob(account, collection, document),
        )? {
            blob_entries
        } else {
            return Ok(result);
        };

        for (item_idx, item_range) in items {
            let blob_entry = blob_entries.items.get(item_idx as usize).ok_or_else(|| {
                StoreError::InternalError(format!("Blob entry {} not found", item_idx))
            })?;

            let blob_path = blob_entry.as_path(
                self.config.blob_base_path.clone(),
                &self.config.blob_hash_levels,
            )?;

            let mut blob = File::open(&blob_path).map_err(|err| {
                StoreError::InternalError(format!(
                    "Failed to open blob file {}: {:}",
                    blob_path.display(),
                    err
                ))
            })?;
            result.push((
                item_idx,
                if item_range.start != 0 || item_range.end != u32::MAX {
                    let from_offset = if item_range.start < blob_entry.size() {
                        item_range.start
                    } else {
                        0
                    };
                    let mut buf = vec![
                        0;
                        (std::cmp::min(item_range.end, blob_entry.size()) - from_offset)
                            as usize
                    ];

                    if from_offset > 0 {
                        blob.seek(SeekFrom::Start(from_offset as u64))
                            .map_err(|err| {
                                StoreError::InternalError(format!(
                                    "Failed to seek blob file {} to offset {}: {:}",
                                    blob_path.display(),
                                    from_offset,
                                    err
                                ))
                            })?;
                    }
                    blob.read_exact(&mut buf).map_err(|err| {
                        StoreError::InternalError(format!(
                            "Failed to read blob file {} at offset {}: {:}",
                            blob_path.display(),
                            from_offset,
                            err
                        ))
                    })?;
                    buf
                } else {
                    let mut buf = Vec::with_capacity(blob_entry.size() as usize);
                    blob.read_to_end(&mut buf).map_err(|err| {
                        StoreError::InternalError(format!(
                            "Failed to read blob file {}: {:}",
                            blob_path.display(),
                            err
                        ))
                    })?;
                    buf
                },
            ));
        }

        Ok(result)
    }

    pub fn purge_blobs(&self) -> crate::Result<()> {
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
                let (timestamp, _) = u64::from_leb128_bytes(&value[TEMP_BLOB_KEY_PREFIX.len()..])
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
                        key: ValueKey::serialize_blob(&value),
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
                    std::fs::remove_file(
                        &BlobEntries::deserialize(&key[BLOB_KEY_PREFIX.len()..])
                            .ok_or(StoreError::DataCorruption)?
                            .items
                            .get(0)
                            .ok_or(StoreError::DataCorruption)?
                            .as_path(
                                self.config.blob_base_path.clone(),
                                &self.config.blob_hash_levels,
                            )?,
                    )
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

    pub fn store_temporary_blob(
        &self,
        account: AccountId,
        bytes: &[u8],
    ) -> crate::Result<(u64, u64)> {
        let blob_entry = self.store_blob(bytes)?;
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
            key: blob_entry.as_key(),
            value: (1i64).serialize().unwrap(),
        });
        batch.push(WriteOperation::Set {
            cf: ColumnFamily::Values,
            key: ValueKey::serialize_temporary_blob(account, hash, timestamp),
            value: blob_entry.hash,
        });

        self.db.write(batch)?;

        Ok((timestamp, hash))
    }

    pub fn get_temporary_blob(
        &self,
        account: AccountId,
        hash: u64,
        timestamp: u64,
    ) -> crate::Result<Option<Vec<u8>>> {
        if let Some(blob_entries) = self.db.get::<BlobEntries>(
            ColumnFamily::Values,
            &ValueKey::serialize_temporary_blob(account, hash, timestamp),
        )? {
            let blob_entry = blob_entries.items.get(0).ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to get blob entry for account {} and hash {}",
                    account, hash
                ))
            })?;
            let blob_path = blob_entry.as_path(
                self.config.blob_base_path.clone(),
                &self.config.blob_hash_levels,
            )?;

            let mut buf = Vec::with_capacity(blob_entry.size() as usize);
            File::open(&blob_path)
                .map_err(|err| {
                    StoreError::InternalError(format!(
                        "Failed to open blob file {}: {:}",
                        blob_path.display(),
                        err
                    ))
                })?
                .read_to_end(&mut buf)
                .map_err(|err| {
                    StoreError::InternalError(format!(
                        "Failed to read blob file {}: {:}",
                        blob_path.display(),
                        err
                    ))
                })?;

            Ok(Some(buf))
        } else {
            Ok(None)
        }
    }

    pub fn store_blob(&self, bytes: &[u8]) -> crate::Result<BlobEntry> {
        let blob_entry: BlobEntry = bytes.into();

        // Lock blob key
        let _blob_lock = self.blob_lock.lock_hash(&blob_entry.hash);

        // Check whether the blob is already stored
        let blob_key = blob_entry.as_key();
        if !self.db.exists(ColumnFamily::Values, &blob_key)? {
            let blob_path = blob_entry.as_path(
                self.config.blob_base_path.clone(),
                &self.config.blob_hash_levels,
            )?;

            fs::create_dir_all(blob_path.parent().unwrap()).map_err(|err| {
                StoreError::InternalError(format!(
                    "Failed to create blob directory {}: {:}",
                    blob_path.display(),
                    err
                ))
            })?;
            let mut blob_file = File::create(&blob_path).map_err(|err| {
                StoreError::InternalError(format!(
                    "Failed to create blob file {:?}: {:?}",
                    blob_path.display(),
                    err
                ))
            })?;
            blob_file.write_all(bytes).map_err(|err| {
                StoreError::InternalError(format!(
                    "Failed to write blob file {:?}: {:?}",
                    blob_path.display(),
                    err
                ))
            })?;
            blob_file.flush().map_err(|err| {
                StoreError::InternalError(format!(
                    "Failed to flush blob file {:?}: {:?}",
                    blob_path.display(),
                    err
                ))
            })?;

            // Create blob key
            self.db.set(
                ColumnFamily::Values,
                &blob_key,
                &(0i64).serialize().unwrap(),
            )?;
        }

        Ok(blob_entry)
    }
}
