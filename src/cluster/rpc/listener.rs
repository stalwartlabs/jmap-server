use futures::{stream::StreamExt, SinkExt};
use std::{net::SocketAddr, time::Duration};
use store::tracing::{debug, error};
use tokio::sync::watch;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    time::{self},
};
use tokio_util::codec::Framed;

use crate::cluster::Event;

use super::serialize::RpcEncoder;
use super::{Protocol, Request, Response, RPC_TIMEOUT_MS};

pub async fn spawn_rpc(
    bind_addr: SocketAddr,
    mut shutdown_rx: watch::Receiver<bool>,
    main_tx: mpsc::Sender<Event>,
    key: String,
) {
    // Start listener for RPC requests
    let listener = TcpListener::bind(bind_addr).await.unwrap_or_else(|e| {
        panic!("Failed to bind RPC listener to {}: {}", bind_addr, e);
    });

    tokio::spawn(async move {
        loop {
            tokio::select! {
                stream = listener.accept() => {
                    match stream {
                        Ok((stream, _)) => {
                            let main_tx = main_tx.clone();
                            let key = key.clone();
                            let shutdown_rx = shutdown_rx.clone();
                            tokio::spawn(async move {
                                handle_conn(stream, shutdown_rx, main_tx, key).await;
                            });
                        }
                        Err(err) => {
                            error!("Failed to accept TCP connection: {}", err);
                        }
                    }
                },
                _ = shutdown_rx.changed() => {
                    debug!("RPC listener shutting down.");
                    break;
                }
            };
        }
    });
}

async fn handle_conn(
    stream: TcpStream,
    mut shutdown_rx: watch::Receiver<bool>,
    main_tx: mpsc::Sender<Event>,
    auth_key: String,
) {
    let peer_addr = stream.peer_addr().unwrap();
    let mut frames = Framed::new(stream, RpcEncoder::default());

    let peer_id = match time::timeout(Duration::from_millis(RPC_TIMEOUT_MS), frames.next()).await {
        Ok(Some(result)) => match result {
            Ok(Protocol::Request(Request::Auth { peer_id, key })) => {
                if auth_key == key {
                    debug!("Authenticated peer {}.", peer_id);
                    peer_id
                } else {
                    error!("Failed to authenticate peer {}.", peer_id);
                    return;
                }
            }
            Ok(_) => {
                error!("Received unexpected RPC request from {}.", peer_addr);
                return;
            }
            Err(err) => {
                debug!("RPC connection from {} failed: {}", peer_addr, err);
                return;
            }
        },
        Ok(None) => {
            debug!("RPC connection from {} closed before auth.", peer_addr);
            return;
        }
        Err(_) => {
            error!(
                "RPC connection from {} timed out during authentication.",
                peer_addr
            );
            return;
        }
    };

    if let Err(err) = frames.send(Protocol::Response(Response::Pong)).await {
        error!("Failed to send auth response: {}", err);
        return;
    }

    loop {
        tokio::select! {
            frame = frames.next() => {
                match frame {
                    Some(Ok(Protocol::Request(request))) => {
                        let (response_tx, response_rx) = oneshot::channel();

                        if let Err(err) = main_tx
                            .send(Event::RpcRequest {
                                peer_id,
                                response_tx,
                                request,
                            })
                            .await
                        {
                            error!("Failed to send RPC request to core: {}", err);
                            return;
                        }

                        match response_rx.await {
                            Ok(response) => {
                                if let Err(err) = frames.send(Protocol::Response(response)).await {
                                    error!("Failed to send RPC response: {}", err);
                                    return;
                                }
                            }
                            Err(err) => {
                                debug!("Failed to receive RPC response: {}", err);
                                return;
                            }
                        }
                    }
                    Some(Ok(invalid)) => {
                        error!("Received invalid RPC frame from {}: {:?}", peer_id, invalid);
                        return;
                    }
                    Some(Err(err)) => {
                        error!("Failed to read RPC request from {}: {}", peer_addr, err);
                        return;
                    }
                    None => {
                        debug!("RPC connection with peer {} closed.", peer_addr);
                        break;
                    }
                }

            },
            _ = shutdown_rx.changed() => {
                debug!("RPC connection with peer {} shutting down.", peer_addr);
                return;
            }
        };
    }
}
