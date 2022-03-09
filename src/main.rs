pub mod cluster;
pub mod error;
pub mod jmap;

use std::{
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc},
};

use actix_web::{middleware, web, App, HttpServer};
use store::{config::EnvSettings, tracing::info, JMAPStore};
use store_rocksdb::RocksDB;

use crate::{cluster::main::start_cluster, jmap::jmap_request};

pub struct JMAPServer<T> {
    pub jmap_store: Arc<JMAPStore<T>>,
    pub is_raft_leader: AtomicBool,
    pub worker_pool: rayon::ThreadPool,
}

pub const DEFAULT_HTTP_PORT: u16 = 8080;
pub const DEFAULT_RPC_PORT: u16 = 7911;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    // Read configuration parameters
    let settings = EnvSettings::new();

    // Build the JMAP store
    let jmap_server = web::Data::new(JMAPServer {
        jmap_store: JMAPStore::new(RocksDB::open(&settings).unwrap(), &settings).into(),
        worker_pool: rayon::ThreadPoolBuilder::new()
            .num_threads(
                settings
                    .parse("worker-pool-size")
                    .filter(|v| *v > 0)
                    .unwrap_or_else(num_cpus::get),
            )
            .build()
            .unwrap(),
        is_raft_leader: false.into(),
    });

    // Start cluster
    start_cluster(jmap_server.clone(), &settings).await;

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
