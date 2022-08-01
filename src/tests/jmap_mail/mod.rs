use store_rocksdb::RocksDB;

use crate::api::ingest::Dsn;

use super::{jmap::init_jmap_tests, store::utils::destroy_temp_dir};

pub mod email_changes;
pub mod email_copy;
pub mod email_get;
pub mod email_ingest;
pub mod email_parse;
pub mod email_query;
pub mod email_query_changes;
pub mod email_set;
pub mod email_submission;
pub mod email_thread;
pub mod email_thread_merge;
pub mod mailbox;
pub mod search_snippet;
pub mod vacation_response;

pub async fn ingest_message(raw_message: Vec<u8>, recipients: &[&str]) -> Vec<Dsn> {
    serde_json::from_slice(
        &reqwest::Client::builder()
            .build()
            .unwrap_or_default()
            .post(&format!(
                "http://127.0.0.1:8001/ingest?api_key=SECRET_API_KEY&to={}",
                recipients.join(",")
            ))
            .body(raw_message)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap()
}

#[actix_web::test]
async fn jmap_mail_tests() {
    let (server, mut client, temp_dir) = init_jmap_tests::<RocksDB>("jmap_mail_tests").await;

    // Run tests
    email_changes::test(server.clone(), &mut client).await;
    email_query_changes::test(server.clone(), &mut client).await;
    email_thread::test(server.clone(), &mut client).await;
    email_thread_merge::test(server.clone(), &mut client).await;
    email_get::test(server.clone(), &mut client).await;
    email_parse::test(server.clone(), &mut client).await;
    email_set::test(server.clone(), &mut client).await;
    email_query::test(server.clone(), &mut client).await;
    email_copy::test(server.clone(), &mut client).await;
    email_submission::test(server.clone(), &mut client).await;
    email_ingest::test(server.clone(), &mut client).await;
    vacation_response::test(server.clone(), &mut client).await;
    mailbox::test(server.clone(), &mut client).await;
    search_snippet::test(server.clone(), &mut client).await;

    destroy_temp_dir(temp_dir);
}
