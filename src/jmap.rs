use std::sync::atomic::Ordering;

use actix_web::{web, HttpResponse};

use jmap_store::json::JSONValue;
use store::tracing::error;
use store::ColumnFamily;
use store::{
    changelog::ChangeLogId,
    serialize::{StoreDeserialize, StoreSerialize},
    Store, StoreError,
};
use store_rocksdb::RocksDB;
use tokio::sync::oneshot;

use crate::JMAPServer;

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn get_key<U>(&self, key: &'static str) -> store::Result<Option<U>>
    where
        U: StoreDeserialize + Send + Sync + 'static,
    {
        let jmap_store = self.jmap_store.clone();
        self.spawn_worker(move || jmap_store.db.get(ColumnFamily::Values, key.as_bytes()))
            .await
    }

    pub async fn set_key<U>(&self, key: &'static str, value: U) -> store::Result<()>
    where
        U: StoreSerialize + Send + Sync + 'static,
    {
        let jmap_store = self.jmap_store.clone();
        self.spawn_worker(move || {
            jmap_store.db.set(
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
        let jmap_store = self.jmap_store.clone();

        self.worker_pool.spawn(move || {
            let bytes = match value.serialize() {
                Some(bytes) => bytes,
                None => {
                    error!("Failed to serialize value for key {}", key);
                    return;
                }
            };

            if let Err(err) = jmap_store
                .db
                .set(ColumnFamily::Values, key.as_bytes(), &bytes)
            {
                error!("Failed to set key: {:?}", err);
            }
        });
    }

    pub fn set_raft_leader(&self, term: ChangeLogId) {
        self.is_raft_leader.store(true, Ordering::Relaxed);
        self.jmap_store.raft_log_term.store(term, Ordering::Relaxed);
    }

    pub fn set_raft_follower(&self, term: ChangeLogId) {
        self.is_raft_leader.store(false, Ordering::Relaxed);
        self.jmap_store.raft_log_term.store(term, Ordering::Relaxed);
    }

    pub fn last_log_index(&self) -> ChangeLogId {
        self.jmap_store.raft_log_index.load(Ordering::Relaxed)
    }

    pub fn last_log_term(&self) -> ChangeLogId {
        self.jmap_store.raft_log_term.load(Ordering::Relaxed)
    }

    pub fn is_leader(&self) -> bool {
        self.is_raft_leader.load(Ordering::Relaxed)
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
}

pub async fn jmap_request(
    request: web::Json<JSONValue>,
    _server: web::Data<JMAPServer<RocksDB>>,
) -> HttpResponse {
    HttpResponse::Ok().json(request.0)
}
