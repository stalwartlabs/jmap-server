use std::{net::SocketAddr, time::Duration};

use actix_cors::Cors;
use actix_web::{middleware, web, App, HttpServer};
use store::{
    config::{env_settings::EnvSettings, jmap::JMAPConfig},
    moka::future::Cache,
    tracing::info,
    JMAPStore, Store,
};

use crate::{
    api::{
        blob::{handle_jmap_download, handle_jmap_upload},
        ingest::handle_ingest,
        request::handle_jmap_request,
        session::{handle_jmap_session, Session},
    },
    authorization::auth::SessionFactory,
    cluster::ClusterIpc,
    server::{event_source::handle_jmap_event_source, tls::load_tls_config, websocket::handle_ws},
    services::{
        email_delivery::{init_email_delivery, spawn_email_delivery},
        state_change::spawn_state_manager,
    },
    JMAPServer, DEFAULT_HTTP_PORT,
};

const ONE_DAY_EXPIRY: Duration = Duration::from_secs(60 * 60 * 24);
const ONE_HOUR_EXPIRY: Duration = Duration::from_secs(60 * 60);

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
    let (email_tx, email_rx) = init_email_delivery();

    let server = web::Data::new(JMAPServer {
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
        email_delivery: email_tx.clone(),
        sessions: Cache::builder()
            .initial_capacity(128)
            .time_to_idle(ONE_DAY_EXPIRY)
            .build(),
        rate_limiters: Cache::builder()
            .initial_capacity(128)
            .time_to_idle(ONE_DAY_EXPIRY)
            .build(),
        emails: Cache::builder()
            .initial_capacity(128)
            .time_to_idle(ONE_DAY_EXPIRY)
            .build(),
        session_tokens: Cache::builder()
            .initial_capacity(128)
            .time_to_idle(ONE_HOUR_EXPIRY)
            .build(),

        cluster,
        #[cfg(test)]
        is_offline: false.into(),
    });

    // Spawn email delivery service
    spawn_email_delivery(server.clone(), settings, email_tx, email_rx);

    server
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

    // Obtain TLS path
    let tls_config = if let Some(cert_path) = settings.get("cert-path") {
        load_tls_config(
            &cert_path,
            &settings
                .get("key-path")
                .expect("Missing 'key-path' argument."),
        )
        .into()
    } else {
        None
    };

    info!(
        "Starting JMAP server at {} ({})...",
        http_addr,
        if tls_config.is_some() {
            "https"
        } else {
            "http"
        }
    );

    let server = HttpServer::new(move || {
        App::new()
            .wrap(SessionFactory::new(jmap_server.clone()))
            .wrap(
                Cors::default()
                    .allow_any_origin()
                    .allowed_methods(vec!["GET", "POST", "OPTIONS"]),
            )
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
            .route("/jmap/ws", web::get().to(handle_ws::<T>))
            .route("/ingest", web::post().to(handle_ingest::<T>))
    });

    if let Some(tls_config) = tls_config {
        server.bind_rustls(http_addr, tls_config)?.run().await
    } else {
        server.bind(http_addr)?.run().await
    }
}
