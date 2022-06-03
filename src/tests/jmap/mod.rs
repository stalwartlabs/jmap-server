use std::time::Duration;

use jmap_client::client::Client;
use store_rocksdb::RocksDB;

use crate::{
    api::ingest::Dsn,
    server::http::{init_jmap_server, start_jmap_server},
};

use super::store::utils::{destroy_temp_dir, init_settings};

pub mod email_changes;
pub mod email_copy;
pub mod email_get;
pub mod email_parse;
pub mod email_query;
pub mod email_query_changes;
pub mod email_set;
pub mod email_submission;
pub mod email_thread;
pub mod email_thread_merge;
pub mod event_source;
pub mod mailbox;
pub mod push_subscription;
pub mod references;
pub mod vacation_response;
pub mod websocket;

#[actix_web::test]
async fn jmap_tests() {
    tracing_subscriber::fmt::init();

    let (settings, temp_dir) = init_settings("jmap_tests", 1, 1, true);
    let server = init_jmap_server::<RocksDB>(&settings, None);
    let session_url = format!(
        "http://{}/.well-known/jmap",
        settings.get("hostname").unwrap()
    );

    // Start web server
    let _server = server.clone();
    actix_web::rt::spawn(async move {
        start_jmap_server(_server, settings).await.unwrap();
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create client
    let mut client = Client::connect(&session_url).await.unwrap();

    // Run tests
    /*email_changes::test(server.clone(), &mut client).await;
    email_query_changes::test(server.clone(), &mut client).await;
    email_thread::test(server.clone(), &mut client).await;
    email_thread_merge::test(server.clone(), &mut client).await;
    email_get::test(server.clone(), &mut client).await;
    email_parse::test(server.clone(), &mut client).await;
    email_set::test(server.clone(), &mut client).await;
    email_query::test(server.clone(), &mut client).await;*/
    email_copy::test(server.clone(), &mut client).await;
    //email_submission::test(server.clone(), &mut client).await;
    //vacation_response::test(server.clone(), &mut client).await;
    //mailbox::test(server.clone(), &mut client).await;
    //event_source::test(server.clone(), &mut client).await;
    //push_subscription::test(server.clone(), &mut client).await;
    //websocket::test(server.clone(), &mut client).await;

    destroy_temp_dir(temp_dir);
}

pub async fn ingest_message(raw_message: Vec<u8>, recipients: &[&str]) -> Vec<Dsn> {
    let mut url = "http://127.0.0.1:8001/ingest?api_key=SECRET_API_KEY".to_string();
    for to in recipients {
        url.push_str(&format!("&to={}", to));
    }

    serde_json::from_slice(
        &reqwest::Client::builder()
            .build()
            .unwrap_or_default()
            .post(&url)
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
