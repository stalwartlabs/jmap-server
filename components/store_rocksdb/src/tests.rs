use std::path::PathBuf;

use store::{tokio, JMAPStore};
use store_test::{destroy_temp_dir, init_temp_dir};

use crate::RocksDB;

fn init_db(name: &str, delete_if_exists: bool) -> (JMAPStore<RocksDB>, PathBuf) {
    let (settings, temp_dir) = init_temp_dir(name, delete_if_exists);

    (
        JMAPStore::new(RocksDB::open(&settings).unwrap(), &settings),
        temp_dir,
    )
}

#[tokio::test]
async fn insert_filter_sort() {
    let (db, temp_dir) = init_db("strdb_filter_test", true);

    store_test::db_insert_filter_sort::insert_filter_sort(db, true).await;

    destroy_temp_dir(temp_dir);
}

#[tokio::test]
async fn term_id() {
    let (db, temp_dir) = init_db("strdb_term_id", true);

    store_test::db_term_id::term_id(db).await;

    destroy_temp_dir(temp_dir);
}

#[tokio::test]
async fn tombstones() {
    let (db, temp_dir) = init_db("strdb_tombstones", true);

    store_test::db_tombstones::tombstones(db).await;

    destroy_temp_dir(temp_dir);
}

#[tokio::test]
async fn blobs() {
    let (db, temp_dir) = init_db("strdb_blobs", true);

    store_test::db_blobs::blobs(db).await;

    destroy_temp_dir(temp_dir);
}

/*

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
*/
