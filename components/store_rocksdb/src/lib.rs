pub mod bitmaps;
pub mod changelog;
pub mod delete;
pub mod document_id;
pub mod get;
pub mod insert;
pub mod iterator;
pub mod query;
pub mod tag;
pub mod term;

use std::sync::{Arc, Mutex, MutexGuard};

use bitmaps::{bitmap_compact, bitmap_merge};
use dashmap::DashMap;
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options,
};
use store::{mutex_map::MutexMap, AccountId, CollectionId, Result, Store, StoreError};
use term::{get_last_term_id, TermLock};

pub struct RocksDBStore {
    db: DBWithThreadMode<MultiThreaded>,
    account_lock: MutexMap,
    term_id_lock: DashMap<String, TermLock>,
    term_id_last: Mutex<u64>,
}

impl RocksDBStore {
    pub fn open(path: &str) -> Result<RocksDBStore> {
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
            let cf_opts = Options::default();
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
            path,
            vec![cf_bitmaps, cf_values, cf_indexes, cf_terms, cf_log],
        )
        .map_err(|e| StoreError::InternalError(e.into_string()))?;

        Ok(Self {
            account_lock: MutexMap::with_capacity(1024),
            term_id_lock: DashMap::with_capacity(1024),
            term_id_last: get_last_term_id(&db)?.into(),
            db,
        })
    }

    #[inline(always)]
    fn get_handle(&self, name: &str) -> Result<Arc<BoundColumnFamily>> {
        self.db.cf_handle(name).ok_or_else(|| {
            StoreError::InternalError(format!(
                "Failed to get handle for '{}' column family.",
                name
            ))
        })
    }

    pub fn lock_collection(
        &self,
        account: AccountId,
        collection: CollectionId,
    ) -> store::Result<MutexGuard<usize>> {
        self.account_lock
            .lock(
                ((account as u64) << (8 * std::mem::size_of::<CollectionId>())) | collection as u64,
            )
            .map_err(|_| StoreError::InternalError("Failed to obtain mutex".to_string()))
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

impl<'x> Store<'x> for RocksDBStore where RocksDBStore: store::StoreQuery<'x> {}

#[cfg(test)]
mod tests {
    use crate::RocksDBStore;

    #[test]
    fn test_insert_filter_sort() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_filter_test");

        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        store_test::insert_filter_sort::test_insert_filter_sort(
            RocksDBStore::open(temp_dir.to_str().unwrap()).unwrap(),
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
            RocksDBStore::open(temp_dir.to_str().unwrap()).unwrap(),
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
            RocksDBStore::open(temp_dir.to_str().unwrap()).unwrap(),
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
            RocksDBStore::open(temp_dir.to_str().unwrap()).unwrap(),
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
            RocksDBStore::open(temp_dir.to_str().unwrap()).unwrap(),
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
            RocksDBStore::open(temp_dir.to_str().unwrap()).unwrap(),
        );

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }
}
