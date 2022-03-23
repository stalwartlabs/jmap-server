use std::{convert::TryInto, path::PathBuf, sync::Arc};

use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBIteratorWithThreadMode, DBWithThreadMode,
    MergeOperands, MultiThreaded, Options,
};
use store::{
    bitmap::{deserialize_bitlist, deserialize_bitmap, IS_BITLIST, IS_BITMAP},
    config::EnvSettings,
    roaring::RoaringBitmap,
    serialize::{StoreDeserialize, LAST_TERM_ID_KEY},
    Result, Store, StoreError,
};

pub struct RocksDB {
    db: DBWithThreadMode<MultiThreaded>,
}

impl<'x> Store<'x> for RocksDB {
    type Iterator = DBIteratorWithThreadMode<'x, DBWithThreadMode<MultiThreaded>>;

    #[inline(always)]
    fn delete(&self, cf: store::ColumnFamily, key: &[u8]) -> Result<()> {
        self.db
            .delete_cf(&self.cf_handle(cf)?, key)
            .map_err(|err| StoreError::InternalError(format!("delete_cf failed: {}", err)))
    }

    #[inline(always)]
    fn set(&self, cf: store::ColumnFamily, key: &[u8], value: &[u8]) -> Result<()> {
        self.db
            .put_cf(&self.cf_handle(cf)?, key, value)
            .map_err(|err| StoreError::InternalError(format!("put_cf failed: {}", err)))
    }

    #[inline(always)]
    fn get<U>(&self, cf: store::ColumnFamily, key: &[u8]) -> Result<Option<U>>
    where
        U: StoreDeserialize,
    {
        if let Some(bytes) = self
            .db
            .get_pinned_cf(&self.cf_handle(cf)?, &key)
            .map_err(|err| StoreError::InternalError(format!("get_cf failed: {}", err)))?
        {
            Ok(Some(U::deserialize(&bytes).ok_or_else(|| {
                StoreError::DeserializeError(format!("Failed to deserialize key: {:?}", key))
            })?))
        } else {
            Ok(None)
        }
    }

    #[inline(always)]
    fn merge(&self, cf: store::ColumnFamily, key: &[u8], value: &[u8]) -> Result<()> {
        self.db
            .merge_cf(&self.cf_handle(cf)?, key, value)
            .map_err(|err| StoreError::InternalError(format!("merge_cf failed: {}", err)))
    }

    #[inline(always)]
    fn write(&self, batch: Vec<store::WriteOperation>) -> Result<()> {
        let mut rocks_batch = rocksdb::WriteBatch::default();
        let cf_bitmaps = self.cf_handle(store::ColumnFamily::Bitmaps)?;
        let cf_values = self.cf_handle(store::ColumnFamily::Values)?;
        let cf_indexes = self.cf_handle(store::ColumnFamily::Indexes)?;
        let cf_terms = self.cf_handle(store::ColumnFamily::Terms)?;
        let cf_logs = self.cf_handle(store::ColumnFamily::Logs)?;

        for op in batch {
            match op {
                store::WriteOperation::Set { cf, key, value } => {
                    rocks_batch.put_cf(
                        match cf {
                            store::ColumnFamily::Bitmaps => &cf_bitmaps,
                            store::ColumnFamily::Values => &cf_values,
                            store::ColumnFamily::Indexes => &cf_indexes,
                            store::ColumnFamily::Terms => &cf_terms,
                            store::ColumnFamily::Logs => &cf_logs,
                        },
                        key,
                        value,
                    );
                }
                store::WriteOperation::Delete { cf, key } => {
                    rocks_batch.delete_cf(
                        match cf {
                            store::ColumnFamily::Bitmaps => &cf_bitmaps,
                            store::ColumnFamily::Values => &cf_values,
                            store::ColumnFamily::Indexes => &cf_indexes,
                            store::ColumnFamily::Terms => &cf_terms,
                            store::ColumnFamily::Logs => &cf_logs,
                        },
                        key,
                    );
                }
                store::WriteOperation::Merge { cf, key, value } => {
                    rocks_batch.merge_cf(
                        match cf {
                            store::ColumnFamily::Bitmaps => &cf_bitmaps,
                            store::ColumnFamily::Values => &cf_values,
                            store::ColumnFamily::Indexes => &cf_indexes,
                            store::ColumnFamily::Terms => &cf_terms,
                            store::ColumnFamily::Logs => &cf_logs,
                        },
                        key,
                        value,
                    );
                }
            }
        }
        self.db
            .write(rocks_batch)
            .map_err(|err| StoreError::InternalError(format!("batch write failed: {}", err)))
    }

    #[inline(always)]
    fn exists(&self, cf: store::ColumnFamily, key: &[u8]) -> Result<bool> {
        Ok(self
            .db
            .get_pinned_cf(&self.cf_handle(cf)?, &key)
            .map_err(|err| StoreError::InternalError(format!("get_cf failed: {}", err)))?
            .is_some())
    }

    #[inline(always)]
    fn multi_get<T, U>(&self, cf: store::ColumnFamily, keys: Vec<U>) -> Result<Vec<Option<T>>>
    where
        T: StoreDeserialize,
        U: AsRef<[u8]>,
    {
        let cf_handle = self.cf_handle(cf)?;
        let mut results = Vec::with_capacity(keys.len());
        for value in self
            .db
            .multi_get_cf(keys.iter().map(|key| (&cf_handle, key)).collect::<Vec<_>>())
        {
            results.push(
                if let Some(bytes) = value.map_err(|err| {
                    StoreError::InternalError(format!("multi_get_cf failed: {}", err))
                })? {
                    T::deserialize(&bytes)
                        .ok_or_else(|| {
                            StoreError::DeserializeError("Failed to deserialize keys.".to_string())
                        })?
                        .into()
                } else {
                    None
                },
            );
        }

        Ok(results)
    }

    #[inline(always)]
    fn iterator<'y: 'x>(
        &'y self,
        cf: store::ColumnFamily,
        start: &[u8],
        direction: store::Direction,
    ) -> Result<DBIteratorWithThreadMode<'x, DBWithThreadMode<MultiThreaded>>> {
        Ok(self.db.iterator_cf(
            &self.cf_handle(cf)?,
            rocksdb::IteratorMode::From(
                start,
                match direction {
                    store::Direction::Forward => rocksdb::Direction::Forward,
                    store::Direction::Backward => rocksdb::Direction::Reverse,
                },
            ),
        ))
    }

    fn compact(&self, cf: store::ColumnFamily) -> Result<()> {
        self.db
            .compact_range_cf(&self.cf_handle(cf)?, None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }

    fn open(settings: &EnvSettings) -> Result<Self> {
        // Create the database directory if it doesn't exist
        let path = PathBuf::from(
            &settings
                .get("db-path")
                .unwrap_or_else(|| "stalwart-jmap".to_string()),
        );
        let mut blob_path = path.clone();
        let mut idx_path = path;
        blob_path.push("blobs");
        idx_path.push("idx");
        std::fs::create_dir_all(&blob_path).map_err(|err| {
            StoreError::InternalError(format!(
                "Failed to create blob directory {}: {:?}",
                blob_path.display(),
                err
            ))
        })?;
        std::fs::create_dir_all(&idx_path).map_err(|err| {
            StoreError::InternalError(format!(
                "Failed to create index directory {}: {:?}",
                idx_path.display(),
                err
            ))
        })?;

        // Bitmaps
        let cf_bitmaps = {
            let mut cf_opts = Options::default();
            //cf_opts.set_max_write_buffer_number(16);
            cf_opts.set_merge_operator_associative("merge", bitmap_merge);
            cf_opts.set_compaction_filter("compact", bitmap_compact);
            ColumnFamilyDescriptor::new("bitmaps", cf_opts)
        };

        // Stored values
        let cf_values = {
            let mut cf_opts = Options::default();
            cf_opts.set_merge_operator_associative("merge", numeric_value_merge);
            ColumnFamilyDescriptor::new("values", cf_opts)
        };

        // Secondary indexes
        let cf_indexes = {
            let cf_opts = Options::default();
            ColumnFamilyDescriptor::new("indexes", cf_opts)
        };

        // Term index
        let cf_terms = {
            let cf_opts = Options::default();
            ColumnFamilyDescriptor::new("terms", cf_opts)
        };

        // Raft log and change log
        let cf_log = {
            let cf_opts = Options::default();
            ColumnFamilyDescriptor::new("logs", cf_opts)
        };

        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        Ok(RocksDB {
            db: DBWithThreadMode::open_cf_descriptors(
                &db_opts,
                idx_path,
                vec![cf_bitmaps, cf_values, cf_indexes, cf_terms, cf_log],
            )
            .map_err(|e| StoreError::InternalError(e.into_string()))?,
        })
    }
}

impl RocksDB {
    #[inline(always)]
    fn cf_handle(&self, cf: store::ColumnFamily) -> Result<Arc<BoundColumnFamily>> {
        self.db
            .cf_handle(match cf {
                store::ColumnFamily::Bitmaps => "bitmaps",
                store::ColumnFamily::Values => "values",
                store::ColumnFamily::Indexes => "indexes",
                store::ColumnFamily::Terms => "terms",
                store::ColumnFamily::Logs => "logs",
            })
            .ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to get handle for '{:?}' column family.",
                    cf
                ))
            })
    }
}

pub fn numeric_value_merge(
    key: &[u8],
    value: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    if key == LAST_TERM_ID_KEY {
        let mut value = if let Some(value) = value {
            usize::from_le_bytes(value.try_into().ok()?)
        } else {
            0
        };
        for op in operands.iter() {
            value += usize::from_le_bytes(op.try_into().ok()?);
        }
        //println!("Merging last term: {}", value);
        let mut bytes = Vec::with_capacity(std::mem::size_of::<usize>());
        bytes.extend_from_slice(&value.to_le_bytes());
        Some(bytes)
    } else {
        let mut value = if let Some(value) = value {
            i64::from_le_bytes(value.try_into().ok()?)
        } else {
            0
        };
        for op in operands.iter() {
            value += i64::from_le_bytes(op.try_into().ok()?);
        }
        //println!("Merging key {:?}: {}", key, value);
        let mut bytes = Vec::with_capacity(std::mem::size_of::<i64>());
        bytes.extend_from_slice(&value.to_le_bytes());
        Some(bytes)
    }
}

pub fn bitmap_merge(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    /*print!(
        "Merge operands {:?}, has val {} -> ",
        operands.size_hint().0,
        existing_val.is_some(),
    );*/

    let mut bm = match existing_val {
        Some(existing_val) => RoaringBitmap::deserialize(existing_val)?,
        None if operands.len() == 1 => {
            //println!("return unserialized");
            return Some(Vec::from(operands.into_iter().next().unwrap()));
        }
        _ => RoaringBitmap::new(),
    };

    for op in operands.iter() {
        match *op.get(0)? {
            IS_BITMAP => {
                if let Some(union_bm) = deserialize_bitmap(op) {
                    //print!("Bitmap union");
                    if !bm.is_empty() {
                        bm |= union_bm;
                    } else {
                        bm = union_bm;
                    }
                } else {
                    return None;
                }
            }
            IS_BITLIST => {
                deserialize_bitlist(&mut bm, op);
            }
            _ => {
                return None;
            }
        }
    }

    //println!(" -> {}", bm.len());

    let mut bytes = Vec::with_capacity(bm.serialized_size() + 1);
    bytes.push(IS_BITMAP);
    bm.serialize_into(&mut bytes).ok()?;
    Some(bytes)
}

pub fn bitmap_compact(
    _level: u32,
    _key: &[u8],
    value: &[u8],
) -> rocksdb::compaction_filter::Decision {
    //println!("Compact entry with {:?} bytes.", value.len());
    match RoaringBitmap::deserialize(value) {
        Some(bm) if bm.is_empty() => rocksdb::compaction_filter::Decision::Remove,
        _ => rocksdb::compaction_filter::Decision::Keep,
    }
}
