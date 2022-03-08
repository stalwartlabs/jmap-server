use std::{
    collections::hash_map::DefaultHasher,
    convert::TryInto,
    hash::{Hash, Hasher},
    io::SeekFrom,
    ops::Range,
    path::PathBuf,
    thread,
    time::SystemTime,
};

use crate::leb128::Leb128;
use crate::{
    serialize::{
        serialize_blob_key, serialize_temporary_blob_key, StoreDeserialize, StoreSerialize,
        BLOB_KEY, TEMP_BLOB_KEY,
    },
    AccountId, CollectionId, ColumnFamily, Direction, DocumentId, JMAPStore, Store, StoreError,
    WriteOperation,
};
use sha2::Digest;
use sha2::Sha256;
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

#[derive(Default)]
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

pub struct BlobEntry {
    pub hash: Vec<u8>,
    pub size: u32,
}

pub type BlobIndex = usize;

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
        let num_entries = bytes.len() / (32 + std::mem::size_of::<u32>());
        let mut items = Vec::with_capacity(num_entries);

        for pos in 0..num_entries {
            let start_offset = pos * (32 + std::mem::size_of::<u32>());
            let end_offset = (pos + 1) * (32 + std::mem::size_of::<u32>());

            items.push(BlobEntry {
                hash: bytes.get(start_offset..end_offset)?.to_vec(),
                size: u32::from_le_bytes(
                    bytes
                        .get(start_offset + 32..start_offset + 32 + std::mem::size_of::<u32>())?
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
        let mut key = Vec::with_capacity(self.hash.len() + BLOB_KEY.len());
        key.extend_from_slice(BLOB_KEY);
        key.extend_from_slice(&self.hash);
        key
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
        let mut hash = Vec::with_capacity(32 + std::mem::size_of::<u32>());
        hash.extend_from_slice(&hasher.finalize());
        hash.extend_from_slice(&(bytes.len() as u32).to_le_bytes());

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
    pub async fn get_blob(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        blob_index: BlobIndex,
        blob_range: Range<u32>,
    ) -> crate::Result<Option<(BlobIndex, Vec<u8>)>> {
        Ok(self
            .get_blobs(
                account,
                collection,
                document,
                vec![(blob_index, blob_range)],
            )
            .await?
            .pop())
    }

    pub async fn get_blobs(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        items: Vec<(BlobIndex, Range<u32>)>,
    ) -> crate::Result<Vec<(BlobIndex, Vec<u8>)>> {
        let mut result = Vec::with_capacity(items.len());

        let blob_entries = if let Some(blob_entries) = self
            .get::<BlobEntries>(
                ColumnFamily::Values,
                serialize_blob_key(account, collection, document),
            )
            .await?
        {
            blob_entries
        } else {
            return Ok(result);
        };

        for (item_idx, item_range) in items {
            let blob_entry = blob_entries.items.get(item_idx).ok_or_else(|| {
                StoreError::InternalError(format!("Blob entry {} not found", item_idx))
            })?;

            let blob_path = blob_entry.as_path(
                self.config.blob_base_path.clone(),
                &self.config.blob_hash_levels,
            )?;

            let mut blob = File::open(&blob_path).await.map_err(|err| {
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
                            .await
                            .map_err(|err| {
                                StoreError::InternalError(format!(
                                    "Failed to seek blob file {} to offset {}: {:}",
                                    blob_path.display(),
                                    from_offset,
                                    err
                                ))
                            })?;
                    }
                    blob.read_exact(&mut buf).await.map_err(|err| {
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
                    blob.read_to_end(&mut buf).await.map_err(|err| {
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

    pub async fn purge_blobs(&self) -> crate::Result<()> {
        let db = self.db.clone();
        let blob_temp_ttl = self.config.blob_temp_ttl;
        let blob_base_path = self.config.blob_base_path.clone();
        let blob_hash_levels = self.config.blob_hash_levels.clone();

        self.spawn_worker(move || {
            let mut batch = Vec::new();
            let current_time = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|_| StoreError::InternalError("Failed to get current timestamp".into()))?
                .as_secs();

            for (key, value) in db.iterator(
                ColumnFamily::Values,
                TEMP_BLOB_KEY.to_vec(),
                Direction::Forward,
            )? {
                if key.starts_with(TEMP_BLOB_KEY) {
                    let (timestamp, _) = u64::from_leb128_bytes(&value[TEMP_BLOB_KEY.len()..])
                        .ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Failed to deserialize timestamp from key {:?}",
                                key
                            ))
                        })?;
                    if (current_time >= timestamp && current_time - timestamp > blob_temp_ttl)
                        || (current_time < timestamp && timestamp - current_time > blob_temp_ttl)
                    {
                        batch.push(WriteOperation::Delete {
                            cf: ColumnFamily::Values,
                            key: key.into(),
                        });
                        let mut blob_key = Vec::with_capacity(value.len() + BLOB_KEY.len());
                        blob_key.extend_from_slice(BLOB_KEY);
                        blob_key.extend_from_slice(&value);
                        batch.push(WriteOperation::Merge {
                            cf: ColumnFamily::Values,
                            key: blob_key,
                            value: (-1i64).serialize().unwrap(),
                        });
                    }
                } else {
                    break;
                }
            }

            if !batch.is_empty() {
                db.write(batch)?;
            }

            for (key, value) in
                db.iterator(ColumnFamily::Values, BLOB_KEY.to_vec(), Direction::Forward)?
            {
                if key.starts_with(BLOB_KEY) {
                    let value = i64::deserialize(&value).ok_or_else(|| {
                        StoreError::InternalError("Failed to convert blob key to i64".to_string())
                    })?;
                    debug_assert!(value >= 0);
                    if value == 0 {
                        db.delete(ColumnFamily::Values, key.to_vec())?;
                        std::fs::remove_file(
                            &BlobEntries::deserialize(&key[BLOB_KEY.len()..])
                                .ok_or(StoreError::DataCorruption)?
                                .items
                                .get(0)
                                .ok_or(StoreError::DataCorruption)?
                                .as_path(blob_base_path.clone(), &blob_hash_levels)?,
                        )
                        .map_err(|err| {
                            StoreError::InternalError(format!(
                                "Failed to delete blob file: {:}",
                                err
                            ))
                        })?;
                    }
                } else {
                    break;
                }
            }
            Ok(())
        })
        .await
    }

    pub async fn store_temporary_blob(
        &self,
        account: AccountId,
        bytes: &[u8],
    ) -> crate::Result<(u64, u64)> {
        let blob_entry = self.store_blob(bytes).await?;
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
            key: serialize_temporary_blob_key(account, hash, timestamp),
            value: blob_entry.hash,
        });

        self.write(batch).await?;

        Ok((timestamp, hash))
    }

    pub async fn get_temporary_blob(
        &self,
        account: AccountId,
        hash: u64,
        timestamp: u64,
    ) -> crate::Result<Option<Vec<u8>>> {
        if let Some(blob_entries) = self
            .get::<BlobEntries>(
                ColumnFamily::Values,
                serialize_temporary_blob_key(account, hash, timestamp),
            )
            .await?
        {
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
                .await
                .map_err(|err| {
                    StoreError::InternalError(format!(
                        "Failed to open blob file {}: {:}",
                        blob_path.display(),
                        err
                    ))
                })?
                .read_to_end(&mut buf)
                .await
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

    pub async fn store_blob(&self, bytes: &[u8]) -> crate::Result<BlobEntry> {
        let blob_entry: BlobEntry = bytes.into();

        // Lock blob key
        let _blob_lock = self.blob_lock.lock_hash(&blob_entry.hash).await;

        // Check whether the blob is already stored
        if !self
            .exists(ColumnFamily::Values, blob_entry.as_key())
            .await?
        {
            let blob_path = blob_entry.as_path(
                self.config.blob_base_path.clone(),
                &self.config.blob_hash_levels,
            )?;

            fs::create_dir_all(blob_path.parent().unwrap())
                .await
                .map_err(|err| {
                    StoreError::InternalError(format!(
                        "Failed to create blob directory {}: {:}",
                        blob_path.display(),
                        err
                    ))
                })?;
            let mut blob_file = File::create(&blob_path).await.map_err(|err| {
                StoreError::InternalError(format!(
                    "Failed to create blob file {:?}: {:?}",
                    blob_path.display(),
                    err
                ))
            })?;
            blob_file.write_all(bytes).await.map_err(|err| {
                StoreError::InternalError(format!(
                    "Failed to write blob file {:?}: {:?}",
                    blob_path.display(),
                    err
                ))
            })?;
            blob_file.flush().await.map_err(|err| {
                StoreError::InternalError(format!(
                    "Failed to flush blob file {:?}: {:?}",
                    blob_path.display(),
                    err
                ))
            })?;

            // Create blob key
            self.set(
                ColumnFamily::Values,
                blob_entry.as_key(),
                (0i64).serialize().unwrap(),
            )
            .await?;
        }

        Ok(blob_entry)
    }
}
