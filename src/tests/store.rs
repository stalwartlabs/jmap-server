use std::{fs, path::PathBuf};

use store::{JMAPConfig, JMAPStore, Store};
use store_rocksdb::RocksDB;
use store_test::{
    destroy_temp_dir, init_settings, jmap_mail_set::insert_email, jmap_mailbox::insert_mailbox,
};

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
fn insert_filter_sort() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_filter_test", true);

    store_test::db_insert_filter_sort::insert_filter_sort(db, true);

    destroy_temp_dir(temp_dir);
}

#[test]
fn term_id() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_term_id", true);

    store_test::db_term_id::term_id(db);

    destroy_temp_dir(temp_dir);
}

#[test]
fn blobs() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_blobs", true);

    store_test::db_blobs::blobs(db);

    destroy_temp_dir(temp_dir);
}

#[test]
fn compact_log() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_log", true);

    store_test::db_log::compact_log(db);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_mail_merge_threads() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_threads_test", true);

    store_test::jmap_mail_merge_threads::jmap_mail_merge_threads(&db);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_mail_query() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_mail_query_test", true);

    store_test::jmap_mail_query::jmap_mail_query_prepare(&db, 1);
    store_test::jmap_mail_query::jmap_mail_query(&db, 1);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_changes() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_changes_test", true);

    store_test::jmap_changes::jmap_changes(&db, 1);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_mail_query_changes() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_query_changes_test", true);

    store_test::jmap_mail_query_changes::jmap_mail_query_changes(&db, 1);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_mail_get() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_mail_get_test", true);

    store_test::jmap_mail_get::jmap_mail_get(&db, 1);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_mail_set() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_mail_set_test", true);

    store_test::jmap_mail_set::jmap_mail_set(&db, 1);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_mail_parse() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_mail_parse_test", true);

    store_test::jmap_mail_parse::jmap_mail_parse(&db, 1);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_mail_thread() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_mail_thread_test", true);

    store_test::jmap_mail_thread::jmap_mail_thread(&db, 1);

    destroy_temp_dir(temp_dir);
}

#[test]
fn jmap_mailbox() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_mailbox_test", true);

    store_test::jmap_mailbox::jmap_mailbox(&db, 1);

    destroy_temp_dir(temp_dir);
}

#[test]
#[ignore]
fn insert_test_data() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_jmap_test", true);

    let inbox_id = insert_mailbox(&db, 1, "Inbox", "INBOX");
    let mut test_dir = PathBuf::from(
        "/home/vagrant/code/jmap-server/components/store_test/resources/jmap_mail_get/",
    ); //env!("CARGO_MANIFEST_DIR"));
       //test_dir.push("resources");
       //test_dir.push("jmap_mail_get");

    /*for (pos, file_name) in fs::read_dir(&test_dir).unwrap().into_iter().enumerate() {
        let file_name = file_name.as_ref().unwrap().path();
        if file_name.extension().map_or(false, |e| e == "eml") {
            insert_email(
                &db,
                1,
                fs::read(&file_name).unwrap(),
                vec![inbox_id],
                vec![match pos % 5 {
                    0 => "$seen",
                    1 => "$flagged",
                    2 => "$answered",
                    3 => "$draft",
                    _ => "$junk",
                }],
                None,
            );
        }
    }*/

    //store_test::jmap_mailbox::jmap_mailbox(&db, 1);

    //destroy_temp_dir(temp_dir);
}
