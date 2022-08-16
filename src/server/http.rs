use std::{net::SocketAddr, sync::Arc, time::Duration};

use actix_cors::Cors;
use actix_web::{dev::Server, middleware, web, App, HttpServer};
use jmap::{
    orm::{serialize::JMAPOrm, TinyORM},
    principal::schema::Principal,
    SUPERUSER_ID,
};
use jmap_sharing::principal::CreateAccount;
use store::{
    config::{env_settings::EnvSettings, jmap::JMAPConfig},
    core::{collection::Collection, document::Document},
    moka::future::Cache,
    rand::{distributions::Alphanumeric, thread_rng, Rng},
    tracing::info,
    write::batch::WriteBatch,
    JMAPStore, Store,
};

use crate::{
    api::{
        blob::{handle_jmap_download, handle_jmap_upload},
        ingest::handle_ingest,
        request::handle_jmap_request,
        session::{handle_jmap_session, Session},
    },
    authorization::{
        auth::SessionFactory,
        oauth::{
            handle_client_auth, handle_client_auth_post, handle_device_auth, handle_oauth_metadata,
            handle_token_request, OAuth, OAuthMetadata,
        },
    },
    cluster::{rpc::tls::load_tls_server_config, ClusterIpc},
    server::{event_source::handle_jmap_event_source, websocket::handle_ws},
    services::{
        email_delivery::{init_email_delivery, spawn_email_delivery},
        housekeeper::{init_housekeeper, spawn_housekeeper},
        state_change::{init_state_manager, spawn_state_manager},
    },
    JMAPServer, DEFAULT_HTTP_PORT,
};

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
    let store: Arc<JMAPStore<T>> =
        JMAPStore::new(T::open(settings).unwrap(), config, settings).into();

    // Create admin user on first run.
    if store
        .get_document_ids(SUPERUSER_ID, Collection::Principal)
        .unwrap()
        .map_or(true, |ids| !ids.contains(SUPERUSER_ID))
    {
        #[cfg(not(test))]
        {
            let mut batch = WriteBatch::new(SUPERUSER_ID);
            for (pos, (id, name)) in [("admin", "Administrator"), ("ingest", "Ingest account")]
                .into_iter()
                .enumerate()
            {
                let account_id = store
                    .assign_document_id(SUPERUSER_ID, Collection::Principal)
                    .expect("Failed to generate account id.");
                if account_id != pos as u32 {
                    panic!(
                        "Failed to generate account id, expected id {} but got {}.",
                        pos, account_id
                    );
                }
                let mut document = Document::new(Collection::Principal, account_id);
                TinyORM::<Principal>::new_account(
                    id,
                    &settings
                        .get(&format!("set-{}-password", id))
                        .unwrap_or_else(|| "changeme".to_string()),
                    name,
                )
                .insert(&mut document)
                .unwrap();
                batch.insert_document(document);
            }
            store.write(batch).expect("Failed to write to database.");
        }
    } else if let Some(secret) = settings.get("set-admin-password") {
        // Reset admin password
        let mut batch = WriteBatch::new(SUPERUSER_ID);
        let mut document = Document::new(Collection::Principal, SUPERUSER_ID);
        let admin = store
            .get_orm::<Principal>(SUPERUSER_ID, SUPERUSER_ID)
            .unwrap()
            .unwrap();
        let changes = TinyORM::track_changes(&admin).change_secret(&secret);
        admin.merge(&mut document, changes).unwrap();
        batch.update_document(document);
        batch.log_update(Collection::Principal, SUPERUSER_ID);
        store.write(batch).unwrap();
        println!("Admin password successfully changed.");
        std::process::exit(0);
    }

    let (email_tx, email_rx) = init_email_delivery();
    let (housekeeper_tx, housekeeper_rx) = init_housekeeper();
    let (change_tx, change_rx) = init_state_manager();
    let is_in_cluster = cluster.is_some();

    let server = web::Data::new(JMAPServer {
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
        state_change: change_tx,
        email_delivery: email_tx.clone(),
        housekeeper: housekeeper_tx,
        oauth: Box::new(OAuth {
            key: settings
                .get("rpc-key")
                .or_else(|| settings.get("oauth-key"))
                .unwrap_or_else(|| {
                    thread_rng()
                        .sample_iter(Alphanumeric)
                        .take(40)
                        .map(char::from)
                        .collect::<String>()
                }),
            expiry_user_code: settings.parse("oauth-user-code-expiry").unwrap_or(1800),
            expiry_token: settings.parse("oauth-token-expiry").unwrap_or(3600),
            expiry_refresh_token: settings
                .parse("oauth-refresh-token-expiry")
                .unwrap_or(30 * 86400),
            expiry_refresh_token_renew: settings
                .parse("oauth-refresh-token-renew")
                .unwrap_or(4 * 86400),
            max_auth_attempts: settings.parse("oauth-max-attempts").unwrap_or(3),
            metadata: serde_json::to_string(&OAuthMetadata::new(base_session.base_url()))
                .expect("Failed to serialize OAuth metadata."),
        }),
        sessions: Cache::builder()
            .initial_capacity(128)
            .time_to_live(ONE_HOUR_EXPIRY)
            .build(),
        rate_limiters: Cache::builder()
            .initial_capacity(128)
            .time_to_idle(ONE_HOUR_EXPIRY)
            .build(),
        oauth_status: Cache::builder().time_to_live(ONE_HOUR_EXPIRY).build(),
        cluster,
        base_session,
        #[cfg(test)]
        is_offline: false.into(),
    });

    // Spawn TypeState manager
    spawn_state_manager(server.clone(), settings, !is_in_cluster, change_rx);

    // Spawn email delivery service
    spawn_email_delivery(server.clone(), settings, email_tx, email_rx);

    // Spawn housekeeper
    spawn_housekeeper(server.clone(), settings, housekeeper_rx);

    server
}

pub async fn build_jmap_server<T>(
    jmap_server: web::Data<JMAPServer<T>>,
    settings: EnvSettings,
) -> std::io::Result<Server>
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
        load_tls_server_config(
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
        "Starting Stalwart JMAP server v{} at {} ({})...",
        env!("CARGO_PKG_VERSION"),
        http_addr,
        if tls_config.is_some() {
            "https"
        } else {
            "http"
        }
    );

    let strict_cors = settings.parse("strict-cors").unwrap_or(false);
    let server = HttpServer::new(move || {
        App::new()
            .wrap(SessionFactory::new(jmap_server.clone()))
            .wrap(if strict_cors {
                Cors::default()
                    .allow_any_origin()
                    .allowed_methods(vec!["GET", "POST", "OPTIONS"])
            } else {
                Cors::permissive()
            })
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
            .route("/auth", web::get().to(handle_client_auth::<T>))
            .route("/auth", web::post().to(handle_client_auth_post::<T>))
            .route("/auth/device", web::post().to(handle_device_auth::<T>))
            .route("/auth/token", web::post().to(handle_token_request::<T>))
            .route(
                "/.well-known/oauth-authorization-server",
                web::get().to(handle_oauth_metadata::<T>),
            )
    });
    if let Some(tls_config) = tls_config {
        server.bind_rustls(http_addr, tls_config)
    } else {
        server.bind(http_addr)
    }
    .map(|s| s.run())
}
