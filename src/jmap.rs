use actix_web::{web, HttpResponse};

use jmap::json::JSONValue;
use store::tracing::error;
use store::ColumnFamily;
use store::{
    serialize::{StoreDeserialize, StoreSerialize},
    Store, StoreError,
};
use store_rocksdb::RocksDB;
use tokio::sync::oneshot;

use crate::cluster::Event;
use crate::JMAPServer;

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

        rx.await.map_err(|e| {
            StoreError::InternalError(format!("Failed to write batch: Await error: {}", e))
        })?
    }

    pub async fn shutdown(&self) {
        if self.store.config.is_in_cluster && self.cluster_tx.send(Event::Shutdown).await.is_err() {
            error!("Failed to send shutdown event to cluster.");
        }
    }

    #[cfg(test)]
    pub async fn set_offline(&self, is_offline: bool) {
        self.is_offline
            .store(is_offline, std::sync::atomic::Ordering::Relaxed);
        self.set_follower();
        if self
            .cluster_tx
            .send(Event::IsOffline(is_offline))
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

pub async fn jmap_request(
    request: web::Json<JSONValue>,
    _server: web::Data<JMAPServer<RocksDB>>,
) -> HttpResponse {
    HttpResponse::Ok().json(request.0)
}
