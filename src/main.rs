/*pub mod api;
pub mod blob;
pub mod cluster;
pub mod server;
pub mod state;

#[cfg(test)]
pub mod tests;

use std::sync::Arc;

use api::session::Session;
use cluster::{
    main::{init_cluster, start_cluster},
    ClusterIpc,
};
use server::http::{init_jmap_server, start_jmap_server};
use store::{config::env_settings::EnvSettings, tracing::info, JMAPStore};
use store_rocksdb::RocksDB;
use tokio::sync::mpsc;

pub const DEFAULT_HTTP_PORT: u16 = 8080;
pub const DEFAULT_RPC_PORT: u16 = 7911;

pub struct JMAPServer<T> {
    pub store: Arc<JMAPStore<T>>,
    pub worker_pool: rayon::ThreadPool,
    pub base_session: Session,
    pub cluster: Option<ClusterIpc>,
    pub state_change: mpsc::Sender<state::Event>,

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
*/

#[tokio::main]
async fn main() -> std::io::Result<()> {
    Ok(())
}
