use std::net::SocketAddr;

use actix_web::{middleware, web, App, HttpServer};
use store::{
    config::{env_settings::EnvSettings, jmap::JMAPConfig},
    tracing::info,
    JMAPStore, Store,
};

use crate::{
    api::{
        request::handle_jmap_request,
        session::{handle_jmap_session, Session},
    },
    blob::{download::handle_jmap_download, upload::handle_jmap_upload},
    cluster::ClusterIpc,
    state::{event_source::handle_jmap_event_source, manager::spawn_state_manager},
    JMAPServer, DEFAULT_HTTP_PORT,
};

pub fn init_jmap_server<T>(
    settings: &EnvSettings,
    cluster: Option<ClusterIpc>,
) -> web::Data<JMAPServer<T>>
where
    T: for<'x> Store<'x> + 'static,
{
    // Build the JMAP server.
    let config = JMAPConfig::from(settings);
    let base_session = Session::new(settings, &config);
    let store = JMAPStore::new(T::open(settings).unwrap(), config, settings).into();

    web::Data::new(JMAPServer {
        base_session,
        store,
        worker_pool: rayon::ThreadPoolBuilder::new()
            .num_threads(
                settings
                    .parse("worker-pool-size")
                    .filter(|v| *v > 0)
                    .unwrap_or_else(num_cpus::get),
            )
            .build()
            .unwrap(),
        state_change: spawn_state_manager(cluster.is_none()),
        cluster,
        #[cfg(test)]
        is_offline: false.into(),
    })
}

pub async fn start_jmap_server<T>(
    jmap_server: web::Data<JMAPServer<T>>,
    settings: EnvSettings,
) -> std::io::Result<()>
where
    T: for<'x> Store<'x> + 'static,
{
    // Start JMAP server
    let http_addr = SocketAddr::from((
        settings.parse_ipaddr("bind-addr", "127.0.0.1"),
        settings.parse("http-port").unwrap_or(DEFAULT_HTTP_PORT),
    ));

    info!("Starting JMAP server at {} (TCP)...", http_addr);

    HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .wrap(middleware::NormalizePath::trim())
            .app_data(jmap_server.clone())
            .route("/.well-known/jmap", web::get().to(handle_jmap_session::<T>))
            .route("/jmap", web::post().to(handle_jmap_request::<T>))
            .route(
                "/jmap/upload/{accountId}",
                web::post().to(handle_jmap_upload::<T>),
            )
            .route(
                "/jmap/download/{accountId}/{blobId}/{name}",
                web::get().to(handle_jmap_download::<T>),
            )
            .route(
                "/jmap/eventsource",
                web::get().to(handle_jmap_event_source::<T>),
            )
    })
    .bind(http_addr)?
    .run()
    .await
}
