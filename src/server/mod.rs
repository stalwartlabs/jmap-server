pub mod event_source;
pub mod http;
pub mod websocket;

use crate::cluster::Event;
use crate::JMAPServer;
use store::core::error::StoreError;
use store::tracing::error;
use store::ColumnFamily;
use store::{
    serialize::{StoreDeserialize, StoreSerialize},
    Store,
};
use tokio::sync::oneshot;

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
        if let Some(cluster) = &self.cluster {
            if cluster.tx.send(Event::Shutdown).await.is_err() {
                error!("Failed to send shutdown event to cluster.");
            }
        }
    }

    #[cfg(test)]
    pub async fn set_offline(&self, is_offline: bool, notify_peers: bool) {
        self.is_offline
            .store(is_offline, std::sync::atomic::Ordering::Relaxed);
        self.set_follower(None).await;
        if self
            .cluster
            .as_ref()
            .unwrap()
            .tx
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
