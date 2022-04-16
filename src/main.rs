pub mod cluster;
pub mod jmap;
#[cfg(test)]
pub mod tests;

use crate::jmap::server::{init_jmap_server, start_jmap_server, JMAPServer, DEFAULT_HTTP_PORT};
use store::{config::EnvSettings, tracing::info};
use store_rocksdb::RocksDB;

use crate::cluster::main::start_cluster;

#[actix_web::main]
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

    let (jmap_server, cluster) = init_jmap_server::<RocksDB>(&settings);

    // Start cluster
    if let Some((cluster_tx, cluster_rx)) = cluster {
        start_cluster(jmap_server.clone(), &settings, cluster_rx, cluster_tx).await;
    }

    // Start JMAP server
    start_jmap_server(jmap_server, settings).await
}
