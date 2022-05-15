pub mod blobs;
pub mod log;
pub mod query;
pub mod utils;

use std::path::PathBuf;

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
fn store_query() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_filter_test", true);
    query::test(db, true);
    destroy_temp_dir(temp_dir);
}

#[test]
fn store_blobs() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_blobs", true);

    blobs::test(db);

    destroy_temp_dir(temp_dir);
}

#[test]
fn store_compact_log() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_log", true);

    log::test(db);

    destroy_temp_dir(temp_dir);
}
