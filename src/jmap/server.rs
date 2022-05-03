use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use actix_web::{middleware, web, App, HttpServer};

use store::config::env_settings::EnvSettings;
use store::config::jmap::JMAPConfig;
use store::core::error::StoreError;
use store::tracing::{error, info};
use store::{
    serialize::{StoreDeserialize, StoreSerialize},
    Store,
};
use store::{ColumnFamily, JMAPStore};
use tokio::sync::{mpsc, oneshot};

use crate::cluster::{self, Event, IPC_CHANNEL_BUFFER};
use crate::jmap::api::handle_jmap_request;
use crate::jmap::download::handle_jmap_download;
use crate::jmap::event_source::handle_jmap_event_source;
use crate::jmap::session::handle_jmap_session;
use crate::jmap::upload::handle_jmap_upload;

use super::session::Session;

pub const DEFAULT_HTTP_PORT: u16 = 8080;
pub const DEFAULT_RPC_PORT: u16 = 7911;

pub struct JMAPServer<T> {
    pub store: Arc<JMAPStore<T>>,
    pub cluster_tx: mpsc::Sender<cluster::Event>,
    pub worker_pool: rayon::ThreadPool,
    pub is_leader: AtomicBool,
    pub is_up_to_date: AtomicBool,
    pub base_session: Session,

    #[cfg(test)]
    pub is_offline: AtomicBool,
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn get_key<U>(&self, key: &'static str) -> store::Result<Option<U>>
    where
        U: StoreDeserialize + Send + Sync + 'static,
    {
        let store = self.store.clone();
        self.spawn_worker(move || store.db.get(ColumnFamily::Values, key.as_bytes()))
            .await
    }

    pub async fn set_key<U>(&self, key: &'static str, value: U) -> store::Result<()>
    where
        U: StoreSerialize + Send + Sync + 'static,
    {
        let store = self.store.clone();
        self.spawn_worker(move || {
            store.db.set(
                ColumnFamily::Values,
                key.as_bytes(),
                &value.serialize().ok_or_else(|| {
                    StoreError::SerializeError(format!("Failed to serialize value for key {}", key))
                })?,
            )
        })
        .await
    }

    pub fn queue_set_key<U>(&self, key: &'static str, value: U)
    where
        U: StoreSerialize + Send + Sync + 'static,
    {
        let store = self.store.clone();

        self.worker_pool.spawn(move || {
            let bytes = match value.serialize() {
                Some(bytes) => bytes,
                None => {
                    error!("Failed to serialize value for key {}", key);
                    return;
                }
            };

            if let Err(err) = store.db.set(ColumnFamily::Values, key.as_bytes(), &bytes) {
                error!("Failed to set key: {:?}", err);
            }
        });
    }

    pub async fn spawn_worker<U, V>(&self, f: U) -> store::Result<V>
    where
        U: FnOnce() -> store::Result<V> + Send + 'static,
        V: Sync + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        self.worker_pool.spawn(move || {
            tx.send(f()).ok();
        });

        rx.await
            .map_err(|e| StoreError::InternalError(format!("Await error: {}", e)))?
    }

    pub async fn spawn_jmap_request<U, V>(&self, f: U) -> jmap::Result<V>
    where
        U: FnOnce() -> jmap::Result<V> + Send + 'static,
        V: Sync + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        self.worker_pool.spawn(move || {
            tx.send(f()).ok();
        });

        rx.await
            .map_err(|e| StoreError::InternalError(format!("Await error: {}", e)))?
    }

    pub async fn shutdown(&self) {
        if self.store.config.is_in_cluster && self.cluster_tx.send(Event::Shutdown).await.is_err() {
            error!("Failed to send shutdown event to cluster.");
        }
    }

    #[cfg(test)]
    pub async fn set_offline(&self, is_offline: bool, notify_peers: bool) {
        self.is_offline
            .store(is_offline, std::sync::atomic::Ordering::Relaxed);
        self.set_follower();
        if self
            .cluster_tx
            .send(Event::SetOffline {
                is_offline,
                notify_peers,
            })
            .await
            .is_err()
        {
            error!("Failed to send offline event to cluster.");
        }
    }

    #[cfg(test)]
    pub fn is_offline(&self) -> bool {
        self.is_offline.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[allow(clippy::type_complexity)]
pub fn init_jmap_server<T>(
    settings: &EnvSettings,
) -> (
    web::Data<JMAPServer<T>>,
    Option<(mpsc::Sender<cluster::Event>, mpsc::Receiver<cluster::Event>)>,
)
where
    T: for<'x> Store<'x> + 'static,
{
    let is_cluster = settings.get("cluster").is_some();

    // Build the JMAP server.
    let (cluster_tx, cluster_rx) = mpsc::channel::<cluster::Event>(IPC_CHANNEL_BUFFER);
    let config = JMAPConfig::from(settings);
    let base_session = Session::new(settings, &config);
    let store = JMAPStore::new(T::open(settings).unwrap(), config, settings).into();
    let jmap_server = web::Data::new(JMAPServer {
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
        cluster_tx: cluster_tx.clone(),
        is_leader: (!is_cluster).into(),
        is_up_to_date: (!is_cluster).into(),
        #[cfg(test)]
        is_offline: false.into(),
    });

    (
        jmap_server,
        if is_cluster {
            Some((cluster_tx, cluster_rx))
        } else {
            None
        },
    )
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
