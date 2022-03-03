use std::sync::atomic::Ordering;

use actix_web::{web, HttpResponse};

use jmap_store::json::JSONValue;
use store::{
    changelog::ChangeLogId,
    serialize::{StoreDeserialize, StoreSerialize},
    Store, StoreError,
};
use store_rocksdb::RocksDBStore;
use tokio::sync::oneshot;
use tracing::error;

use crate::JMAPServer;

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn get_key<U>(&self, key: &'static str) -> store::Result<Option<U>>
    where
        U: StoreDeserialize + Send + Sync + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let jmap_store = self.jmap_store.clone();

        self.worker_pool.spawn(move || {
            tx.send(jmap_store.store.get_key(key)).ok();
        });

        rx.await.map_err(|e| {
            StoreError::InternalError(format!("Failed to get key: Await error: {}", e))
        })?
    }

    pub async fn set_key<U>(&self, key: &'static str, value: U) -> store::Result<()>
    where
        U: StoreSerialize + Send + Sync + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let jmap_store = self.jmap_store.clone();

        self.worker_pool.spawn(move || {
            tx.send(jmap_store.store.set_key(key, value)).ok();
        });

        rx.await.map_err(|e| {
            StoreError::InternalError(format!("Failed to set key: Await error: {}", e))
        })?
    }

    pub fn queue_set_key<U>(&self, key: &'static str, value: U)
    where
        U: StoreSerialize + Send + Sync + 'static,
    {
        let jmap_store = self.jmap_store.clone();

        self.worker_pool.spawn(move || {
            if let Err(err) = jmap_store.store.set_key(key, value) {
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
}

pub async fn jmap_request(
    request: web::Json<JSONValue>,
    _server: web::Data<JMAPServer<RocksDBStore>>,
) -> HttpResponse {
    HttpResponse::Ok().json(request.0)
}
