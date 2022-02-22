pub mod cluster;
pub mod config;
pub mod error;
pub mod jmap;

use std::net::SocketAddr;

use actix_web::{middleware, post, web, App, HttpResponse, HttpServer};
use cluster::{
    swim::{start_swim, swim_http_sync, DEFAULT_SWIM_PORT},
    JMAPCluster,
};
use config::EnvSettings;
use jmap_store::{json::JSONValue, local_store::JMAPLocalStore, JMAPStoreConfig};
use store::Store;
use store_rocksdb::RocksDBStore;

pub struct JMAPServer<T> {
    pub jmap_store: JMAPLocalStore<T>,
    pub worker_pool: rayon::ThreadPool,
    pub cluster: Option<JMAPCluster>,
}

pub const DEFAULT_HTTP_PORT: u16 = 8080;

//#[post("/.well-known/jmap")]
//#[get("/.well-known/jmap")]

#[post("/api")]
async fn index(
    request: web::Json<JSONValue>,
    _server: web::Data<JMAPServer<RocksDBStore>>,
) -> HttpResponse {
    HttpResponse::Ok().json(request.0)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=info");
    tracing_subscriber::fmt::init();

    let settings = EnvSettings::new();

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

    let bind_addr = settings.parse_ipaddr("bind-addr", "127.0.0.1");

    // Start cluster
    if jmap_server.cluster.is_some() {
        start_swim(
            jmap_server.clone(),
            SocketAddr::from((
                bind_addr,
                settings.parse("swim-port").unwrap_or(DEFAULT_SWIM_PORT),
            )),
        )
        .await;
    }

    HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .app_data(web::JsonConfig::default().limit(10000000))
            .app_data(jmap_server.clone())
            .service(index)
            .service(swim_http_sync)
    })
    .bind(SocketAddr::from((
        bind_addr,
        settings.parse("http-port").unwrap_or(DEFAULT_HTTP_PORT),
    )))?
    .run()
    .await
}
