use std::time::Duration;

use serde::{Deserialize, Serialize};
use store::raft::{LogIndex, RaftId};
use store::tracing::{debug, error};
use store::Store;
use tokio::{
    sync::{mpsc, oneshot, watch},
    time,
};

use super::Peer;
use super::{
    rpc::{self, Request, Response, RpcEvent},
    Cluster, Event,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Entry {
    id: usize,
}

const RETRY_MS: u64 = 30 * 1000;

pub fn spawn_append_entries<T>(cluster: &Cluster<T>, peer: &Peer, mut rx: watch::Receiver<LogIndex>)
where
    T: for<'x> Store<'x> + 'static,
{
    let peer_tx = peer.tx.clone();
    let peer_name = peer.to_string();

    let term = cluster.term;
    let mut last = RaftId::new(cluster.last_log_term, cluster.last_log_index);
    let main_tx = cluster.tx.clone();
    let core = cluster.core.clone();

    let mut did_match = false;

    tokio::spawn(async move {
        debug!("Starting append entries process with peer {}.", peer_name);

        loop {
            let request = if did_match {
                Request::MatchLog {
                    term: term + 1,
                    last,
                }
            } else {
                Request::MatchLog { term, last }
            };

            match send_request(&peer_tx, request).await {
                Response::MatchLog {
                    term: peer_term,
                    success,
                    matched,
                } if !did_match => {
                    if !success || peer_term > term {
                        if let Err(err) = main_tx.send(Event::StepDown { term: peer_term }).await {
                            error!("Error sending step down message: {}", err);
                        }
                        break;
                    } else if !matched.is_null() {
                        let local_match = match core.get_next_raft_id(matched).await {
                            Ok(Some(local_match)) => local_match,
                            Ok(None) => {
                                error!("Log sync failed: local match is null");
                                break;
                            }
                            Err(err) => {
                                error!("Error getting next raft id: {:?}", err);
                                break;
                            }
                        };
                        if local_match != matched {
                            error!(
                                "Failed to match raft logs with {}, local match: {:?}, peer match: {:?}", peer_name,
                                local_match, matched
                            );
                            break;
                        }
                    }
                    did_match = true;
                    continue;
                }
                Response::None => {
                    // There was a problem delivering the message, wait 30 seconds or until
                    // the next change is received.
                    match time::timeout(Duration::from_millis(RETRY_MS), rx.changed()).await {
                        Ok(Ok(())) => {
                            debug!("Received new log index.");
                            last.index = *rx.borrow();
                        }
                        Ok(Err(_)) => {
                            debug!("Log sync process with {} exiting.", peer_name);
                            break;
                        }
                        Err(_) => (),
                    }
                    continue;
                }
                response => {
                    error!(
                        "Unexpected response from peer {}: {:?}",
                        peer_name, response
                    );
                }
            }

            // Wait for the next change
            if rx.changed().await.is_ok() {
                debug!("Received new log index.");
                last.index = *rx.borrow();
            } else {
                debug!("Log sync process with {} exiting.", peer_name);
                break;
            }
        }
    });
}

async fn send_request(peer_tx: &mpsc::Sender<rpc::RpcEvent>, request: Request) -> Response {
    let (response_tx, rx) = oneshot::channel();
    if let Err(err) = peer_tx
        .send(RpcEvent::NeedResponse {
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
