pub mod bitmaps;
pub mod delete;
pub mod document_id;
pub mod get;
pub mod insert;
pub mod iterator;
pub mod query;
pub mod tag;
pub mod term;

use std::sync::{Arc, Mutex};

use bitmaps::{bitmap_compact, bitmap_merge};
use dashmap::DashMap;
use document_id::DocumentIdAssigner;
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options,
};
use store::{BaseId, Result, Store, StoreError};
use term::{get_last_term_id, TermLock};

pub struct RocksDBStore {
    db: DBWithThreadMode<MultiThreaded>,
    id_assigner: DashMap<BaseId, DocumentIdAssigner>,
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

        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_cf_descriptors(
            &db_opts,
            path,
            vec![cf_bitmaps, cf_values, cf_indexes, cf_terms],
        )
        .map_err(|e| StoreError::InternalError(e.into_string()))?;

        Ok(Self {
            id_assigner: DashMap::with_capacity(1024),
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

    pub fn compact(&self) -> Result<()> {
        for cf in [
            self.get_handle("values")?,
            self.get_handle("indexes")?,
            self.get_handle("bitmaps")?,
            self.get_handle("terms")?,
        ] {
            self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        }
        Ok(())
    }
}

impl<'x> Store<'x> for RocksDBStore where RocksDBStore: store::StoreQuery<'x> {}

#[cfg(test)]
mod tests {

    use store_test::{
        test_artworks::{filter_artworks, insert_artworks, sort_artworks},
        test_mail_threads::test_mail_threads,
    };

    use crate::RocksDBStore;

    #[test]
    fn test_artworks() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_query_test");
        /*if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }*/

        let db = RocksDBStore::open(temp_dir.to_str().unwrap()).unwrap();
        //db.compact().unwrap();
        //insert_artworks(&db);
        filter_artworks(&db);
        sort_artworks(&db);
    }

    #[test]
    fn test_message_threads() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_threads_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        {
            let db = RocksDBStore::open(temp_dir.to_str().unwrap()).unwrap();
            test_mail_threads(&db);
        }

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }
}
