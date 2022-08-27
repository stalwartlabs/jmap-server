#![warn(clippy::disallowed_types)]
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use jmap_server::{
    cluster::init::{init_cluster, start_cluster},
    server::{
        http::{build_jmap_server, init_jmap_server},
        UnwrapFailure,
    },
};

use std::time::Duration;

use futures::StreamExt;
use signal_hook::consts::{SIGHUP, SIGINT, SIGQUIT, SIGTERM};
use signal_hook_tokio::Signals;
use store::{
    config::env_settings::EnvSettings,
    tracing::{self, info, warn, Level},
    Store,
};
use store_rocksdb::RocksDB;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Read configuration parameters
    let mut settings = EnvSettings::new();

    // Enable logging
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(settings.parse("log-level").unwrap_or(Level::INFO))
            .finish(),
    )
    .failed_to("set default subscriber");

    // Set base URL if missing
    if !settings.contains_key("jmap-url") {
        let jmap_url = if settings.contains_key("jmap-cert-path") {
            "https://localhost"
        } else {
            "http://localhost"
        }
        .to_string();
        warn!(
            "Warning: Hostname parameter 'jmap-url' was not specified, using '{}'.",
            jmap_url
        );
        settings.set_value("jmap-url".to_string(), jmap_url);
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
        .failed_to("start JMAP server");
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
                core.store.db.close().failed_to("close database");

                break;
            }
            _ => unreachable!(),
        }
    }

    Ok(())
}
