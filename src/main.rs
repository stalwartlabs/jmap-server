use actix_web::{get, middleware, post, web, App, HttpResponse, HttpServer, Responder};
use jmap_store::{json::JSONValue, local_store::JMAPLocalStore, JMAPStoreConfig};
use store::Store;
use store_rocksdb::{RocksDBStore, RocksDBStoreConfig};

struct JMAPServer<T> {
    pub jmap_store: JMAPLocalStore<T>,
    pub worker_pool: rayon::ThreadPool,
}

#[post("/test")]
async fn index(
    request: web::Json<JSONValue>,
    server: web::Data<JMAPServer<RocksDBStore>>,
) -> HttpResponse {
    server.worker_pool.spawn(|| {
        println!("Started a thread!");
    });
    println!("{:?}", request);
    HttpResponse::Ok().json(request.0)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let worker_pool_size = 0;
    let jmap_server = web::Data::new(JMAPServer {
        jmap_store: JMAPLocalStore::open(
            RocksDBStore::open(RocksDBStoreConfig::default_config("/tmp/jmap_store_test")).unwrap(),
            JMAPStoreConfig::new(),
        )
        .unwrap(),
        worker_pool: rayon::ThreadPoolBuilder::new()
            .num_threads(if worker_pool_size > 0 {
                worker_pool_size
            } else {
                num_cpus::get()
            })
            .build()
            .unwrap(),
    });

    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();

    HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .app_data(jmap_server.clone())
            .service(index)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
