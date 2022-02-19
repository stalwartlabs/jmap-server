pub mod bitmaps;
pub mod blob;
pub mod changelog;
pub mod delete;
pub mod document_id;
pub mod get;
pub mod insert;
pub mod iterator;
pub mod query;
pub mod tag;
pub mod term;

use std::{
    convert::TryInto,
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc, Mutex},
    time::Duration,
};

use bitmaps::{bitmap_compact, bitmap_merge};
use document_id::{DocumentIdAssigner, DocumentIdCacheKey};
use moka::sync::Cache;
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, MergeOperands, MultiThreaded,
    Options,
};
use store::{mutex_map::MutexMap, serialize::LAST_TERM_ID_KEY, Result, Store, StoreError, TermId};

pub struct RocksDBStoreConfig {
    pub path: String,
    pub hash_levels: Vec<usize>,
    pub temp_blob_ttl: u64,
    pub term_cache_ttl: u64,
    pub term_cache_size: u64,
    pub doc_id_cache_size: u64,
}

impl RocksDBStoreConfig {
    pub fn default_config(path: &str) -> RocksDBStoreConfig {
        RocksDBStoreConfig {
            path: path.to_string(),
            hash_levels: vec![1],
            temp_blob_ttl: 3600,
            term_cache_ttl: 10 * 60 * 1000,
            term_cache_size: 32 * 1024 * 1024,
            doc_id_cache_size: 32 * 1024 * 1024,
        }
    }
}

pub struct RocksDBStore {
    db: DBWithThreadMode<MultiThreaded>,
    blob_path: PathBuf,
    blob_lock: MutexMap<()>,
    doc_id_cache: Cache<DocumentIdCacheKey, Arc<Mutex<DocumentIdAssigner>>>,
    term_id_cache: Cache<String, TermId>,
    term_id_lock: MutexMap<()>,
    term_id_last: AtomicU64,
    config: RocksDBStoreConfig,
}

impl<'x> Store<'x> for RocksDBStore {
    type Config = RocksDBStoreConfig;

    fn open(config: Self::Config) -> Result<Self> {
        // Create the database directory if it doesn't exist
        let path = PathBuf::from(&config.path);
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
            ColumnFamilyDescriptor::new("log", cf_opts)
        };

        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_cf_descriptors(
            &db_opts,
            idx_path,
            vec![cf_bitmaps, cf_values, cf_indexes, cf_terms, cf_log],
        )
        .map_err(|e| StoreError::InternalError(e.into_string()))?;

        Ok(Self {
            term_id_last: get_last_id(&db, LAST_TERM_ID_KEY)?.into(),
            term_id_cache: Cache::builder()
                .initial_capacity(1024)
                .max_capacity(config.term_cache_size)
                .time_to_idle(Duration::from_millis(config.term_cache_ttl))
                .build(),
            doc_id_cache: Cache::builder()
                .initial_capacity(128)
                .max_capacity(config.doc_id_cache_size)
                .time_to_idle(Duration::from_secs(60 * 60))
                .build(),
            term_id_lock: MutexMap::with_capacity(1024),
            blob_lock: MutexMap::with_capacity(1024),
            blob_path,
            db,
            config,
        })
    }
}

impl RocksDBStore {
    #[inline(always)]
    fn get_handle(&self, name: &str) -> Result<Arc<BoundColumnFamily>> {
        self.db.cf_handle(name).ok_or_else(|| {
            StoreError::InternalError(format!(
                "Failed to get handle for '{}' column family.",
                name
            ))
        })
    }

    pub fn get_db(&self) -> &DBWithThreadMode<MultiThreaded> {
        &self.db
    }

    pub fn compact(&self) -> Result<()> {
        for cf in [
            self.get_handle("values")?,
            self.get_handle("indexes")?,
            self.get_handle("bitmaps")?,
            self.get_handle("terms")?,
            self.get_handle("log")?,
        ] {
            self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        }
        Ok(())
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

pub fn get_last_id(db: &DBWithThreadMode<MultiThreaded>, key: &[u8]) -> crate::Result<u64> {
    Ok(db
        .get_cf(
            &db.cf_handle("values")
                .ok_or_else(|| StoreError::InternalError("No terms column family found.".into()))?,
            key,
        )
        .map_err(|e| StoreError::InternalError(e.into_string()))?
        .map(|v| u64::from_le_bytes(v.try_into().unwrap()))
        .unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use jmap_store::{local_store::JMAPLocalStore, JMAPStoreConfig};
    use store::Store;

    use crate::{RocksDBStore, RocksDBStoreConfig};

    #[test]
    fn test_insert_filter_sort() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_filter_test");

        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        store_test::insert_filter_sort::test_insert_filter_sort(
            RocksDBStore::open(RocksDBStoreConfig::default_config(
                temp_dir.to_str().unwrap(),
            ))
            .unwrap(),
            true,
        );

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_tombstones() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_tombstones_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        store_test::tombstones::test_tombstones(
            RocksDBStore::open(RocksDBStoreConfig::default_config(
                temp_dir.to_str().unwrap(),
            ))
            .unwrap(),
        );

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_blobs() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_blobs_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        store_test::blobs::test_blobs(
            RocksDBStore::open(RocksDBStoreConfig::default_config(
                temp_dir.to_str().unwrap(),
            ))
            .unwrap(),
        );

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_jmap_mail_merge_threads() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_threads_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        store_test::jmap_mail_merge_threads::test_jmap_mail_merge_threads(
            JMAPLocalStore::open(
                RocksDBStore::open(RocksDBStoreConfig::default_config(
                    temp_dir.to_str().unwrap(),
                ))
                .unwrap(),
                JMAPStoreConfig::new(),
            )
            .unwrap(),
        );

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_jmap_mail_query() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_mail_query_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        store_test::jmap_mail_query::test_jmap_mail_query(
            JMAPLocalStore::open(
                RocksDBStore::open(RocksDBStoreConfig::default_config(
                    temp_dir.to_str().unwrap(),
                ))
                .unwrap(),
                JMAPStoreConfig::new(),
            )
            .unwrap(),
            true,
        );

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_jmap_changes() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_changes_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        store_test::jmap_changes::test_jmap_changes(
            JMAPLocalStore::open(
                RocksDBStore::open(RocksDBStoreConfig::default_config(
                    temp_dir.to_str().unwrap(),
                ))
                .unwrap(),
                JMAPStoreConfig::new(),
            )
            .unwrap(),
        );

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_jmap_mail_query_changes() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_query_changes_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        store_test::jmap_mail_query_changes::test_jmap_mail_query_changes(
            JMAPLocalStore::open(
                RocksDBStore::open(RocksDBStoreConfig::default_config(
                    temp_dir.to_str().unwrap(),
                ))
                .unwrap(),
                JMAPStoreConfig::new(),
            )
            .unwrap(),
        );

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_jmap_mail_get() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_mail_get_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        store_test::jmap_mail_get::test_jmap_mail_get(
            JMAPLocalStore::open(
                RocksDBStore::open(RocksDBStoreConfig::default_config(
                    temp_dir.to_str().unwrap(),
                ))
                .unwrap(),
                JMAPStoreConfig::new(),
            )
            .unwrap(),
        );

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_jmap_mail_set() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_mail_set_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        store_test::jmap_mail_set::test_jmap_mail_set(
            JMAPLocalStore::open(
                RocksDBStore::open(RocksDBStoreConfig::default_config(
                    temp_dir.to_str().unwrap(),
                ))
                .unwrap(),
                JMAPStoreConfig::new(),
            )
            .unwrap(),
        );

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_jmap_mail_parse() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_mail_parse_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        store_test::jmap_mail_parse::test_jmap_mail_parse(
            JMAPLocalStore::open(
                RocksDBStore::open(RocksDBStoreConfig::default_config(
                    temp_dir.to_str().unwrap(),
                ))
                .unwrap(),
                JMAPStoreConfig::new(),
            )
            .unwrap(),
        );

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_jmap_mail_thread() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_mail_thread_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        store_test::jmap_mail_thread::test_jmap_mail_thread(
            JMAPLocalStore::open(
                RocksDBStore::open(RocksDBStoreConfig::default_config(
                    temp_dir.to_str().unwrap(),
                ))
                .unwrap(),
                JMAPStoreConfig::new(),
            )
            .unwrap(),
        );

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_jmap_mailbox() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_mailbox_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        store_test::jmap_mailbox::test_jmap_mailbox(
            JMAPLocalStore::open(
                RocksDBStore::open(RocksDBStoreConfig::default_config(
                    temp_dir.to_str().unwrap(),
                ))
                .unwrap(),
                JMAPStoreConfig::new(),
            )
            .unwrap(),
        );

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }
}
