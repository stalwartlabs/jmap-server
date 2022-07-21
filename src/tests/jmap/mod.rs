use std::{path::PathBuf, sync::Arc, time::Duration};

use actix_web::web;
use jmap::{types::jmap::JMAPId, SUPERUSER_ID};
use jmap_client::client::{Client, Credentials};
use store::{core::acl::ACLToken, Store};
use store_rocksdb::RocksDB;

use crate::{
    authorization::{auth::RemoteAddress, rate_limit::RateLimiter, Session},
    server::http::{init_jmap_server, start_jmap_server},
    JMAPServer,
};

use super::store::utils::{destroy_temp_dir, init_settings};

pub mod acl;
pub mod authorization;
pub mod event_source;
pub mod push_subscription;
pub mod references;
pub mod websocket;

pub async fn init_jmap_tests<T>(test_name: &str) -> (web::Data<JMAPServer<T>>, Client, PathBuf)
where
    T: for<'x> Store<'x> + 'static,
{
    tracing_subscriber::fmt::init();

    let (settings, temp_dir) = init_settings(test_name, 1, 1, true);
    let server = init_jmap_server::<T>(&settings, None);
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

    // Bypass authentication for the main client
    let acl_token = Arc::new(ACLToken {
        member_of: vec![SUPERUSER_ID, 1],
        access_to: vec![],
    });
    server
        .sessions
        .insert(
            "DO_NOT_ATTEMPT_THIS_AT_HOME".to_string(),
            Session::new(SUPERUSER_ID, acl_token.as_ref()),
        )
        .await;
    server.store.acl_tokens.insert(SUPERUSER_ID, acl_token);
    server
        .rate_limiters
        .insert(
            RemoteAddress::AccountId(SUPERUSER_ID),
            Arc::new(RateLimiter::new(1000, 1000)),
        )
        .await;

    // Create client
    let mut client = Client::connect(
        &session_url,
        Credentials::bearer("DO_NOT_ATTEMPT_THIS_AT_HOME"),
    )
    .await
    .unwrap();
    client.set_default_account_id(JMAPId::new(1));

    (server, client, temp_dir)
}

#[actix_web::test]
async fn jmap_core_tests() {
    let (server, mut client, temp_dir) = init_jmap_tests::<RocksDB>("jmap_tests").await;

    // Run tests
    acl::test(server.clone(), &mut client).await;
    /*authorization::test(server.clone(), &mut client).await;
    event_source::test(server.clone(), &mut client).await;
    push_subscription::test(server.clone(), &mut client).await;
    websocket::test(server.clone(), &mut client).await;*/

    destroy_temp_dir(temp_dir);
}
