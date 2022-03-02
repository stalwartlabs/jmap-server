use std::time::Duration;

use store::Store;
use tokio::{
    sync::{mpsc, oneshot, watch},
    time,
};
use tracing::{debug, error};

use super::{
    raft::LogIndex,
    rpc::{self, Request, Response, RpcMessage},
    Cluster, Message,
};

/*

mail:
 - blob
 - tags (keyword, mailbox, thread_id)



*/

const RETRY_MS: u64 = 30 * 1000;

pub fn start_log_sync<T>(
    cluster: &Cluster<T>,
    peer_tx: mpsc::Sender<rpc::RpcMessage>,
) -> watch::Sender<LogIndex>
where
    T: for<'x> Store<'x> + 'static,
{
    let term = cluster.term;
    let mut last_log_index = cluster.last_log_index;
    let last_log_term = cluster.last_log_term;
    let main_tx = cluster.tx.clone();
    let core = cluster.core.clone();

    let (tx, mut rx) = watch::channel(last_log_index);

    tokio::spawn(async move {
        debug!("Started log sync process!");

        loop {
            if let Response::FollowLeader {
                term: peer_term,
                success,
            } = send_request(
                &peer_tx,
                Request::FollowLeader {
                    term,
                    last_log_index,
                    last_log_term,
                },
            )
            .await
            {
                if !success || peer_term > term {
                    if let Err(err) = main_tx.send(Message::StepDown { term: peer_term }).await {
                        error!("Error sending step down message: {}", err);
                    }
                    break;
                }
            } else {
                match time::timeout(Duration::from_millis(RETRY_MS), rx.changed()).await {
                    Ok(Ok(())) => {
                        debug!("Received new log index.");
                        last_log_index = *rx.borrow();
                    }
                    Ok(Err(_)) => {
                        debug!("Log sync process exiting.");
                        break;
                    }
                    Err(_) => (),
                }
                continue;
            }

            if rx.changed().await.is_ok() {
                debug!("Received new log index.");
                last_log_index = *rx.borrow();
            } else {
                debug!("Log sync process exiting.");
                break;
            }
        }
    });

    tx
}

async fn send_request(peer_tx: &mpsc::Sender<rpc::RpcMessage>, request: Request) -> Response {
    let (response_tx, rx) = oneshot::channel();
    if let Err(err) = peer_tx
        .send(RpcMessage::NeedResponse {
            request,
            response_tx,
        })
        .await
    {
        error!("Channel failed: {}", err);
        return Response::None;
    }
    rx.await.unwrap_or(Response::None)
}
