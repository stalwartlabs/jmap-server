pub mod api;
pub mod authorization;
pub mod cluster;
pub mod server;
pub mod services;

#[cfg(test)]
pub mod tests;

use std::sync::Arc;

use cluster::{
    main::{init_cluster, start_cluster},
    ClusterIpc,
};

use authorization::{auth::RemoteAddress, rate_limit::RateLimiter};
use server::http::{init_jmap_server, start_jmap_server};
use services::{email_delivery, state_change};
use store::{
    config::env_settings::EnvSettings, moka::future::Cache, parking_lot::Mutex, tracing::info,
    AccountId, JMAPStore,
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

    pub sessions: Cache<AccountId, Arc<authorization::Session>>,
    pub session_tokens: Cache<String, AccountId>,
    pub rate_limiters: Cache<RemoteAddress, Arc<Mutex<RateLimiter>>>,
    pub emails: Cache<String, AccountId>,

    #[cfg(test)]
    pub is_offline: std::sync::atomic::AtomicBool,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

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

    // Start JMAP server
    start_jmap_server(
        if let Some((cluster_ipc, cluster_init)) = init_cluster(&settings) {
            let server = init_jmap_server::<RocksDB>(&settings, cluster_ipc.into());
            start_cluster(cluster_init, server.clone(), &settings).await;
            server
        } else {
            init_jmap_server::<RocksDB>(&settings, None)
        },
        settings,
    )
    .await
}
