use std::time::Duration;

use actix_web::web;
use store::Store;
use tokio::{sync::mpsc, time};
use tracing::debug;

use crate::JMAPServer;

use super::rpc;

const KEEPALIVE_MS: u64 = 1000;

/*

mail:
 - blob
 - tags (keyword, mailbox, thread_id)



*/

pub fn start_log_sync<T>(
    core: web::Data<JMAPServer<T>>,
    peer_tx: mpsc::Sender<rpc::RpcMessage>,
) -> mpsc::Sender<bool>
where
    T: for<'x> Store<'x> + 'static,
{
    let (tx, mut rx) = mpsc::channel::<bool>(1);

    tokio::spawn(async move {
        debug!("Started log sync process!");
        loop {
            match time::timeout(Duration::from_millis(KEEPALIVE_MS), rx.recv()).await {
                Ok(Some(_)) => {
                    debug!("Received nudge.");
                }
                Ok(None) => {
                    debug!("Log sync process exiting.");
                    break;
                }
                Err(_) => {
                    debug!("Sending keepalive.");
                }
            }
        }
    });

    tx
}
