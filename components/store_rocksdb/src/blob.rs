use std::convert::TryInto;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::PathBuf;

use rocksdb::{Direction, IteratorMode};
use store::leb128::Leb128;
use store::serialize::serialize_blob_key;
use store::{leb128::skip_leb128_value, serialize::BLOB_KEY};
use store::{AccountId, BlobEntry, CollectionId, DocumentId, StoreBlob, StoreError};

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

pub struct BlobEntries {
    pub file: BlobFile,
    pub index: Vec<usize>,
}

pub fn serialize_blob_key_from_value(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut key = Vec::with_capacity(32 + std::mem::size_of::<usize>() + BLOB_KEY.len());
    key.extend_from_slice(BLOB_KEY);
    key.extend_from_slice(bytes.get(0..32 + skip_leb128_value(bytes.get(32..)?)?)?);
    key.into()
}

pub fn deserialize_blob_entries(
    base_path: PathBuf,
    hash_levels: &[usize],
    bytes: &[u8],
) -> store::Result<BlobEntries> {
    let (mut num_entries, bytes_read) =
        usize::from_leb128_bytes(bytes.get(32..).ok_or(StoreError::DataCorruption)?)
            .ok_or(StoreError::DataCorruption)?;
    let blob_name = bytes
        .get(0..32 + bytes_read)
        .ok_or(StoreError::DataCorruption)?;
    let mut index = Vec::with_capacity(num_entries);
    let mut bytes_it = bytes
        .get(32 + bytes_read..)
        .ok_or(StoreError::DataCorruption)?
        .iter();

    while num_entries > 0 {
        index.push(usize::from_leb128_it(&mut bytes_it).ok_or_else(|| {
            StoreError::DeserializeError(format!(
                "Failed to deserialize blob entry from bytes: {:?}",
                bytes
            ))
        })?);
        num_entries -= 1;
    }

    if num_entries > 0 {
        return Err(StoreError::DeserializeError(
            "Failed to deserialize blob index".into(),
        ));
    }

    Ok(BlobEntries {
        file: BlobFile::new(base_path, blob_name, hash_levels, false)
            .map_err(|err| StoreError::DeserializeError(err.to_string()))?,
        index,
    })
}

impl StoreBlob for RocksDBStore {
    fn get_document_blob_entries(
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
            deserialize_blob_entries(
                self.blob_path.clone(),
                &self.config.db_options.hash_levels,
                &blob_entries,
            )?
        } else {
            return Ok(result);
        };

        let mut blob = File::open(&blob_entries.file.get_path()).map_err(|err| {
            StoreError::InternalError(format!(
                "Failed to open blob file {}: {:}",
                blob_entries.file.get_path().display(),
                err
            ))
        })?;

        for entry in entries {
            if entry.index >= blob_entries.index.len() {
                return Err(StoreError::InternalError(format!(
                    "Blob index out of bounds: {}",
                    entry.index
                )));
            }
            let (from_offset, buf_len) = if let Some(range) = &entry.value {
                (
                    if entry.index > 0 {
                        blob_entries.index[entry.index - 1]
                    } else {
                        0
                    } + range.start,
                    std::cmp::min(
                        range.end - range.start,
                        blob_entries.index[entry.index] - blob_entries.index[entry.index - 1],
                    ),
                )
            } else if entry.index > 0 {
                (
                    blob_entries.index[entry.index - 1],
                    blob_entries.index[entry.index] - blob_entries.index[entry.index - 1],
                )
            } else {
                (0, blob_entries.index[entry.index])
            };

            let mut buf = vec![0; buf_len];
            blob.seek(SeekFrom::Start(from_offset as u64))
                .map_err(|err| {
                    StoreError::InternalError(format!(
                        "Failed to seek blob file {} to offset {}: {:}",
                        blob_entries.file.get_path().display(),
                        from_offset,
                        err
                    ))
                })?;
            blob.read_exact(&mut buf).map_err(|err| {
                StoreError::InternalError(format!(
                    "Failed to read blob file {} at offset {}: {:}",
                    blob_entries.file.get_path().display(),
                    from_offset,
                    err
                ))
            })?;
            result.push(BlobEntry {
                index: entry.index,
                value: buf,
            });
        }

        Ok(result)
    }

    fn purge_deleted_blobs(&self) -> store::Result<()> {
        let cf_values = self.get_handle("values")?;

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
                debug_assert!(value >= 0);
                if value == 0 {
                    result.push((
                        BlobFile::new(
                            self.blob_path.clone(),
                            &key[BLOB_KEY.len()..],
                            &self.config.db_options.hash_levels,
                            false,
                        )
                        .map_err(|err| {
                            StoreError::InternalError(format!(
                                "Failed to create blob file: {:}",
                                err
                            ))
                        })?
                        .path
                        .clone(),
                        value,
                    ));
                }
            } else {
                break;
            }
        }

        Ok(result)
    }
}
