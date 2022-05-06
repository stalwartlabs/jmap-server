#[cfg(test)]
pub mod tests;

use core::{
    cluster::main::{init_cluster, start_cluster},
    server::http::{init_jmap_server, start_jmap_server},
    tokio, DEFAULT_HTTP_PORT,
};

use store::{config::env_settings::EnvSettings, tracing::info};
use store_rocksdb::RocksDB;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    // Read configuration parameters
    let mut settings = EnvSettings::new();
    if !settings.contains_key("jmap-url") {
        let default_url = format!(
            "http://{}:{}",
            settings.parse_ipaddr("advertise-addr", "127.0.0.1"),
            settings.parse("http-port").unwrap_or(DEFAULT_HTTP_PORT)
        );
        info!(
            "Warning: JMAP base URL parameter 'jmap-url' was not specified, using default '{}'.",
            default_url
        );
        settings.set_value("jmap-url".to_string(), default_url);
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
