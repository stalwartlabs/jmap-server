use crate::tests::store::utils::StoreCompareWith;
use store_rocksdb::RocksDB;

pub mod crud;
pub mod election;
pub mod fuzz;
pub mod log_conflict;
pub mod mail_thread_merge;
pub mod utils;

#[test]
//#[ignore]
fn postmortem() {
    let dbs = (1..=6)
        .map(|n| super::store::init_db_params::<RocksDB>("st_cluster", n, 5, false).0)
        .collect::<Vec<_>>();

    for (pos1, db1) in dbs.iter().enumerate() {
        for (pos2, db2) in dbs.iter().enumerate() {
            if pos1 != pos2 {
                print!("{}/{} -> ", pos1, pos2);
                println!("{:?}", db1.compare_with(db2));
            }
        }
    }
}

#[actix_web::test]
//#[cfg_attr(not(feature = "test_cluster"), ignore)]
async fn test_cluster() {
    tracing_subscriber::fmt::init();
    crud::test::<RocksDB>().await;
    election::test::<RocksDB>().await;
    log_conflict::test::<RocksDB>().await;
    mail_thread_merge::test::<RocksDB>().await;
}

#[actix_web::test]
//#[cfg_attr(not(feature = "fuzz_cluster"), ignore)]
async fn fuzz_cluster() {
    tracing_subscriber::fmt::init();
    fuzz::test::<RocksDB>(vec![]).await;
}
