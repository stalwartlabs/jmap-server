pub mod cluster;
pub mod error;
pub mod jmap;
#[cfg(test)]
pub mod tests;

use std::{
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc},
};

use actix_web::{middleware, web, App, HttpServer};
use store::{config::EnvSettings, tracing::info, JMAPStore};
use store_rocksdb::RocksDB;
use tokio::sync::mpsc;

use crate::{
    cluster::{main::start_cluster, IPC_CHANNEL_BUFFER},
    jmap::jmap_request,
};

pub struct JMAPServer<T> {
    pub store: Arc<JMAPStore<T>>,
    pub cluster_tx: mpsc::Sender<cluster::Event>,
    pub worker_pool: rayon::ThreadPool,
    pub is_cluster: bool,
    pub is_leader: AtomicBool,
    pub is_up_to_date: AtomicBool,

    #[cfg(test)]
    pub is_offline: AtomicBool,
}

pub const DEFAULT_HTTP_PORT: u16 = 8080;
pub const DEFAULT_RPC_PORT: u16 = 7911;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    // Read configuration parameters
    let settings = EnvSettings::new();
    let is_cluster = settings.get("cluster").is_some();

    // Build the JMAP store
    let (cluster_tx, cluster_rx) = mpsc::channel::<cluster::Event>(IPC_CHANNEL_BUFFER);
    let jmap_server = web::Data::new(JMAPServer {
        store: JMAPStore::new(RocksDB::open(&settings).unwrap(), &settings).into(),
        worker_pool: rayon::ThreadPoolBuilder::new()
            .num_threads(
                settings
                    .parse("worker-pool-size")
                    .filter(|v| *v > 0)
                    .unwrap_or_else(num_cpus::get),
            )
            .build()
            .unwrap(),
        cluster_tx: cluster_tx.clone(),
        is_cluster,
        is_leader: (!is_cluster).into(),
        is_up_to_date: (!is_cluster).into(),
        #[cfg(test)]
        is_offline: false.into(),
    });

    // Start cluster
    if is_cluster {
        start_cluster(jmap_server.clone(), &settings, cluster_rx, cluster_tx).await;
    }

    // Start HTTP server
    let http_addr = SocketAddr::from((
        settings.parse_ipaddr("bind-addr", "127.0.0.1"),
        settings.parse("http-port").unwrap_or(DEFAULT_HTTP_PORT),
    ));
    info!("Starting HTTP server at {} (TCP)...", http_addr);
    HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .app_data(web::JsonConfig::default().limit(10000000))
            .app_data(jmap_server.clone())
            .route("/jmap", web::post().to(jmap_request))
            .route("/.well-known/jmap", web::post().to(jmap_request))
    })
    .bind(http_addr)?
    .run()
    .await
}
