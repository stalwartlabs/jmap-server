use std::time::Duration;

use jmap_client::client::Client;
use store_rocksdb::RocksDB;

use crate::server::http::{init_jmap_server, start_jmap_server};

use super::store::utils::{destroy_temp_dir, init_settings};

pub mod email_changes;
pub mod email_get;
pub mod email_parse;
pub mod email_query;
pub mod email_query_changes;
pub mod email_set;
pub mod email_thread;
pub mod email_thread_merge;
pub mod mailbox;

#[actix_web::test]
async fn jmap_tests() {
    tracing_subscriber::fmt::init();

    let (settings, temp_dir) = init_settings("jmap_tests", 1, 1, true);
    let server = init_jmap_server::<RocksDB>(&settings, None);
    let session_url = format!("{}/.well-known/jmap", settings.get("jmap-url").unwrap());

    // Start web server
    let _server = server.clone();
    actix_web::rt::spawn(async move {
        start_jmap_server(_server, settings).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create client
    let mut client = Client::connect(&session_url).await.unwrap();

    // Run tests
    email_changes::test(server.clone(), &mut client).await;
    email_query_changes::test(server.clone(), &mut client).await;
    email_thread::test(server.clone(), &mut client).await;
    email_thread_merge::test(server.clone(), &mut client).await;
    email_get::test(server.clone(), &mut client).await;
    email_parse::test(server.clone(), &mut client).await;
    email_set::test(server.clone(), &mut client).await;
    email_query::test(server.clone(), &mut client).await;
    mailbox::test(server.clone(), &mut client).await;

    destroy_temp_dir(temp_dir);
}
