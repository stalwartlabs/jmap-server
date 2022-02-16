use actix_web::web;
use jmap_mail::{
    import::{JMAPMailImportRequest, JMAPMailImportResponse},
    JMAPMailImport, JMAPMailSet,
};
use jmap_store::{JMAPSet, JMAPSetResponse};
use store::StoreError;
use store_rocksdb::RocksDBStore;
use tokio::sync::oneshot;

use crate::JMAPServer;

async fn mail_import(
    core: &web::Data<JMAPServer<RocksDBStore>>,
    request: JMAPMailImportRequest,
) -> jmap_store::Result<JMAPMailImportResponse> {
    let (tx, rx) = oneshot::channel();
    let _core = core.clone();

    core.worker_pool.spawn(move || {
        tx.send(_core.jmap_store.mail_import(request)).ok();
    });

    rx.await
        .map_err(|e| StoreError::InternalError(format!("Await error: {}", e)))?
}

async fn mail_set(
    core: &web::Data<JMAPServer<RocksDBStore>>,
    request: JMAPSet<()>,
) -> jmap_store::Result<JMAPSetResponse> {
    let (tx, rx) = oneshot::channel();
    let _core = core.clone();

    core.worker_pool.spawn(move || {
        tx.send(_core.jmap_store.mail_set(request)).ok();
    });

    rx.await
        .map_err(|e| StoreError::InternalError(format!("Await error: {}", e)))?
}
