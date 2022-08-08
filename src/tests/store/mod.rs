pub mod blobs;
pub mod log;
pub mod query;
pub mod utils;

use std::{path::PathBuf, sync::Arc};

use store::{config::jmap::JMAPConfig, JMAPStore, Store};
use store_rocksdb::RocksDB;

use self::utils::{destroy_temp_dir, init_settings};

pub fn init_db<T>(name: &str, delete_if_exists: bool) -> (JMAPStore<T>, PathBuf)
where
    T: for<'x> Store<'x> + 'static,
{
    init_db_params(name, 1, 1, delete_if_exists)
}

pub fn init_db_params<T>(
    name: &str,
    peer_num: u32,
    total_peers: u32,
    delete_if_exists: bool,
) -> (JMAPStore<T>, PathBuf)
where
    T: for<'x> Store<'x> + 'static,
{
    let (settings, temp_dir) = init_settings(name, peer_num, total_peers, delete_if_exists);

    (
        JMAPStore::new(
            T::open(&settings).unwrap(),
            JMAPConfig::from(&settings),
            &settings,
        ),
        temp_dir,
    )
}

#[test]
fn store_tests() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_store", true);
    let db = Arc::new(db);

    //blobs::test(db.clone());
    log::test(db.clone());
    //query::test(db, true);

    destroy_temp_dir(&temp_dir);
}
