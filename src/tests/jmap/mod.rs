use std::{path::PathBuf, sync::Arc, time::Duration};

use actix_web::{dev::ServerHandle, web};
use jmap::{types::jmap::JMAPId, SUPERUSER_ID};
use jmap_client::client::{Client, Credentials};
use store::{core::acl::ACLToken, Store};
use store_rocksdb::RocksDB;
use tokio::sync::oneshot;

use crate::{
    authorization::{auth::RemoteAddress, rate_limit::RateLimiter, Session},
    server::http::{build_jmap_server, init_jmap_server},
    JMAPServer,
};

use super::store::utils::{destroy_temp_dir, init_settings};

pub mod acl;
pub mod authorization;
pub mod event_source;
pub mod oauth;
pub mod push_subscription;
pub mod references;
pub mod stress_test;
pub mod websocket;

pub async fn init_jmap_tests_opts<T>(
    test_name: &str,
    peer_num: u32,
    total_peers: u32,
    delete_if_exists: bool,
) -> (web::Data<JMAPServer<T>>, Client, PathBuf, ServerHandle)
where
    T: for<'x> Store<'x> + 'static,
{
    let (settings, temp_dir) = init_settings(test_name, peer_num, total_peers, delete_if_exists);
    let server = init_jmap_server::<T>(&settings, None);
    let session_url = format!(
        "http://{}/.well-known/jmap",
        settings.get("hostname").unwrap()
    );

    // Start web server
    let _server = server.clone();
    let (tx, rx) = oneshot::channel();
    actix_web::rt::spawn(async move {
        let server = build_jmap_server(_server, settings).await.unwrap();
        tx.send(server.handle()).unwrap();
        server.await
    });
    let handle = rx.await.unwrap();

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Bypass authentication for the main client
    bypass_authentication(&server).await;

    // Create client
    let mut client = Client::new()
        .credentials(Credentials::bearer("DO_NOT_ATTEMPT_THIS_AT_HOME"))
        .connect(&session_url)
        .await
        .unwrap();
    client.set_default_account_id(JMAPId::new(1));

    (server, client, temp_dir, handle)
}

pub async fn init_jmap_tests<T>(test_name: &str) -> (web::Data<JMAPServer<T>>, Client, PathBuf)
where
    T: for<'x> Store<'x> + 'static,
{
    tracing_subscriber::fmt::init();

    let (server, client, tmp_dir, _) = init_jmap_tests_opts::<T>(test_name, 1, 1, true).await;
    (server, client, tmp_dir)
}

pub async fn bypass_authentication<T>(server: &JMAPServer<T>)
where
    T: for<'x> Store<'x>,
{
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
}

#[actix_web::test]
async fn jmap_core_tests() {
    let (server, mut client, temp_dir) = init_jmap_tests::<RocksDB>("jmap_tests").await;

    // Run tests
    oauth::test(server.clone(), &mut client).await;
    //acl::test(server.clone(), &mut client).await;
    //authorization::test(server.clone(), &mut client).await;
    //event_source::test(server.clone(), &mut client).await;
    //push_subscription::test(server.clone(), &mut client).await;
    //websocket::test(server.clone(), &mut client).await;

    destroy_temp_dir(&temp_dir);
}
