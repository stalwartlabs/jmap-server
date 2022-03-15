use std::path::PathBuf;

use store::JMAPStore;
use store_test::{destroy_temp_dir, init_settings};

use crate::RocksDB;

fn init_db(name: &str, delete_if_exists: bool) -> (JMAPStore<RocksDB>, PathBuf) {
    let (settings, temp_dir) = init_settings(name, 1, 1, delete_if_exists);

    (
        JMAPStore::new(RocksDB::open(&settings).unwrap(), &settings),
        temp_dir,
    )
}

#[test]
fn insert_filter_sort() {
    let (db, temp_dir) = init_db("strdb_filter_test", true);

    store_test::db_insert_filter_sort::insert_filter_sort(db, true);

    destroy_temp_dir(temp_dir);
}

#[test]
fn term_id() {
    let (db, temp_dir) = init_db("strdb_term_id", true);

    store_test::db_term_id::term_id(db);

    destroy_temp_dir(temp_dir);
}

#[test]
fn tombstones() {
    let (db, temp_dir) = init_db("strdb_tombstones", true);

    store_test::db_tombstones::tombstones(db);

    destroy_temp_dir(temp_dir);
}

#[test]
fn blobs() {
    let (db, temp_dir) = init_db("strdb_blobs", true);

    store_test::db_blobs::blobs(db);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_mail_merge_threads() {
    let (db, temp_dir) = init_db("strdb_threads_test", true);

    store_test::jmap_mail_merge_threads::jmap_mail_merge_threads(&db);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_mail_query() {
    let (db, temp_dir) = init_db("strdb_mail_query_test", true);

    store_test::jmap_mail_query::jmap_mail_query_prepare(&db, 1);
    store_test::jmap_mail_query::jmap_mail_query(&db, 1);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_changes() {
    let (db, temp_dir) = init_db("strdb_changes_test", true);

    store_test::jmap_changes::jmap_changes(&db, 1);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_mail_query_changes() {
    let (db, temp_dir) = init_db("strdb_query_changes_test", true);

    store_test::jmap_mail_query_changes::jmap_mail_query_changes(&db, 1);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_mail_get() {
    let (db, temp_dir) = init_db("strdb_mail_get_test", true);

    store_test::jmap_mail_get::jmap_mail_get(&db, 1);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_mail_set() {
    let (db, temp_dir) = init_db("strdb_mail_set_test", true);

    store_test::jmap_mail_set::jmap_mail_set(&db, 1);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_mail_parse() {
    let (db, temp_dir) = init_db("strdb_mail_parse_test", true);

    store_test::jmap_mail_parse::jmap_mail_parse(&db, 1);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_mail_thread() {
    let (db, temp_dir) = init_db("strdb_mail_thread_test", true);

    store_test::jmap_mail_thread::jmap_mail_thread(&db, 1);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_mailbox() {
    let (db, temp_dir) = init_db("strdb_mailbox_test", true);

    store_test::jmap_mailbox::jmap_mailbox(&db, 1);

    destroy_temp_dir(temp_dir);
}
