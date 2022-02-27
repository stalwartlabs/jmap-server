pub mod cluster;
pub mod config;
pub mod error;
pub mod jmap;

use std::net::SocketAddr;

use actix_web::{middleware, web, App, HttpServer};
use cluster::Cluster;
use config::EnvSettings;
use jmap_store::{local_store::JMAPLocalStore, JMAPStoreConfig};
use store::Store;
use store_rocksdb::RocksDBStore;
use tracing::info;

use crate::{cluster::main::start_cluster, jmap::jmap_request};

pub struct JMAPServer<T> {
    pub jmap_store: JMAPLocalStore<T>,
    pub worker_pool: rayon::ThreadPool,
    pub cluster: parking_lot::Mutex<Cluster>,
}

pub const DEFAULT_HTTP_PORT: u16 = 8080;
pub const DEFAULT_RPC_PORT: u16 = 7911;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    // Read configuration parameters
    let settings = EnvSettings::new();
    let bind_addr = settings.parse_ipaddr("bind-addr", "127.0.0.1");

    // Build the JMAP store
    let jmap_server = web::Data::new(JMAPServer {
        jmap_store: JMAPLocalStore::open(
            RocksDBStore::open((&settings).into()).unwrap(),
            JMAPStoreConfig::new(),
        )
        .unwrap(),
        worker_pool: rayon::ThreadPoolBuilder::new()
            .num_threads(
                settings
                    .parse("worker-pool-size")
                    .filter(|v| *v > 0)
                    .unwrap_or_else(num_cpus::get),
            )
            .build()
            .unwrap(),
        cluster: (&settings).into(),
    });

    // Start cluster
    if jmap_server.cluster.lock().is_enabled() {
        let rpc_addr = SocketAddr::from((
            bind_addr,
            settings.parse("rpc-port").unwrap_or(DEFAULT_RPC_PORT),
        ));
        info!("Starting RPC server at {} (UDP/TCP)...", rpc_addr);
        start_cluster(jmap_server.clone(), rpc_addr).await;
    }

    // Start HTTP server
    let http_addr = SocketAddr::from((
        bind_addr,
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
