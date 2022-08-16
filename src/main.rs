#![warn(clippy::disallowed_types)]

pub mod api;
pub mod authorization;
pub mod cluster;
pub mod server;
pub mod services;

#[cfg(test)]
pub mod tests;

use std::{sync::Arc, time::Duration};

use cluster::{
    init::{init_cluster, start_cluster},
    ClusterIpc,
};

use authorization::{auth::RemoteAddress, oauth, rate_limit::RateLimiter};
use futures::StreamExt;
use server::http::{build_jmap_server, init_jmap_server};
use services::{email_delivery, housekeeper, state_change};
use signal_hook::consts::{SIGHUP, SIGINT, SIGQUIT, SIGTERM};
use signal_hook_tokio::Signals;
use store::{
    config::env_settings::EnvSettings, moka::future::Cache, tracing::info, JMAPStore, Store,
};
use store_rocksdb::RocksDB;
use tokio::sync::mpsc;

pub const DEFAULT_HTTP_PORT: u16 = 8080;
pub const DEFAULT_RPC_PORT: u16 = 7911;

pub struct JMAPServer<T> {
    pub store: Arc<JMAPStore<T>>,
    pub worker_pool: rayon::ThreadPool,
    pub base_session: api::session::Session,
    pub cluster: Option<ClusterIpc>,
    pub state_change: mpsc::Sender<state_change::Event>,
    pub email_delivery: mpsc::Sender<email_delivery::Event>,
    pub housekeeper: mpsc::Sender<housekeeper::Event>,

    pub oauth: Box<oauth::OAuth>,
    pub oauth_status: Cache<String, Arc<oauth::OAuthStatus>>,

    pub sessions: Cache<String, authorization::Session>,
    pub rate_limiters: Cache<RemoteAddress, Arc<RateLimiter>>,

    #[cfg(test)]
    pub is_offline: std::sync::atomic::AtomicBool,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    //TODO logging
    tracing_subscriber::fmt::init();
    //tracing::subscriber::set_global_default(tracing_subscriber::FmtSubscriber::new())
    //    .expect("Failed to set global default subscriber");

    // Read configuration parameters
    let mut settings = EnvSettings::new();
    if !settings.contains_key("hostname") {
        let default_hostname = format!(
            "{}:{}",
            settings.parse_ipaddr("advertise-addr", "127.0.0.1"),
            settings.parse("http-port").unwrap_or(DEFAULT_HTTP_PORT)
        );
        info!(
            "Warning: Hostname parameter 'hostname' was not specified, using default '{}'.",
            default_hostname
        );
        settings.set_value("hostname".to_string(), default_hostname);
    }

    // Init JMAP server
    let core = if let Some((cluster_ipc, cluster_init)) = init_cluster(&settings) {
        let core = init_jmap_server::<RocksDB>(&settings, cluster_ipc.into());
        start_cluster(cluster_init, core.clone(), &settings).await;
        core
    } else {
        init_jmap_server::<RocksDB>(&settings, None)
    };
    let server = build_jmap_server(core.clone(), settings)
        .await
        .expect("Failed to start JMAP server");
    let server_handle = server.handle();

    // Start web server
    actix_web::rt::spawn(async move { server.await });

    // Wait for shutdown signal
    let mut signals = Signals::new(&[SIGHUP, SIGTERM, SIGINT, SIGQUIT])?;

    while let Some(signal) = signals.next().await {
        match signal {
            SIGHUP => {
                // Reload configuration
            }
            SIGTERM | SIGINT | SIGQUIT => {
                // Shutdown the system
                info!(
                    "Shutting down Stalwart JMAP server v{}...",
                    env!("CARGO_PKG_VERSION")
                );

                // Stop web server
                server_handle.stop(true).await;

                // Stop services
                core.shutdown().await;

                // Wait for services to finish
                tokio::time::sleep(Duration::from_secs(1)).await;

                // Flush DB
                core.store.db.close().expect("Failed to close database");

                break;
            }
            _ => unreachable!(),
        }
    }

    Ok(())
}
