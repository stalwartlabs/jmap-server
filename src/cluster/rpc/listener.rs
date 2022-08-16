use futures::{stream::StreamExt, SinkExt};
use std::sync::Arc;
use std::{net::SocketAddr, time::Duration};
use store::blake3;
use store::config::env_settings::EnvSettings;
use store::rand::{self, Rng};
use store::tracing::{debug, error};
use tokio::sync::watch;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    time::{self},
};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::Framed;

use crate::cluster::{Config, Event};

use super::serialize::RpcEncoder;
use super::tls::load_tls_server_config;
use super::{Protocol, Request, Response};

pub async fn spawn_rpc(
    bind_addr: SocketAddr,
    mut shutdown_rx: watch::Receiver<bool>,
    main_tx: mpsc::Sender<Event>,
    settings: &EnvSettings,
    config: &Config,
) {
    // Build TLS acceptor
    let (cert_path, key_path) = if let (Some(cert_path), Some(key_path)) =
        (settings.get("rpc-cert-path"), settings.get("rpc-key-path"))
    {
        (cert_path, key_path)
    } else {
        panic!("Missing TLS 'rpc-cert-path' and/or 'rpc-key-path' parameters.");
    };

    let tls_acceptor = Arc::new(TlsAcceptor::from(Arc::new(load_tls_server_config(
        &cert_path, &key_path,
    ))));

    // Start listener for RPC requests
    let listener = TcpListener::bind(bind_addr).await.unwrap_or_else(|e| {
        panic!("Failed to bind RPC listener to {}: {}", bind_addr, e);
    });

    let key = config.key.to_string();
    let rpc_timeout = config.rpc_timeout;

    tokio::spawn(async move {
        loop {
            tokio::select! {
                stream = listener.accept() => {
                    match stream {
                        Ok((stream, _)) => {
                            let main_tx = main_tx.clone();
                            let key = key.clone();
                            let shutdown_rx = shutdown_rx.clone();
                            let tls_acceptor = tls_acceptor.clone();

                            tokio::spawn(async move {
                                let peer_addr = stream.peer_addr().unwrap();
                                let stream = match tls_acceptor.accept(stream).await {
                                    Ok(stream) => stream,
                                    Err(e) => {
                                        debug!("Failed to accept TLS connection: {}", e);
                                        return;
                                    }
                                };

                                handle_conn(stream, peer_addr, shutdown_rx, main_tx, key, rpc_timeout).await;
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
    stream: TlsStream<TcpStream>,
    peer_addr: SocketAddr,
    mut shutdown_rx: watch::Receiver<bool>,
    main_tx: mpsc::Sender<Event>,
    auth_key: String,
    rpc_timeout: u64,
) {
    let mut frames = Framed::new(stream, RpcEncoder::default());

    // Build authentication challenge
    let challenge = rand::thread_rng().gen::<[u8; 12]>();
    let mut hasher = blake3::Hasher::new();
    hasher.update(auth_key.as_bytes());
    hasher.update(&challenge);
    let challenge_response = hasher.finalize();

    let peer_id = match time::timeout(Duration::from_millis(rpc_timeout), async {
        match frames
            .send(Protocol::Response(Response::Auth { challenge }))
            .await
        {
            Ok(_) => frames.next().await,
            Err(_) => None,
        }
    })
    .await
    {
        Ok(Some(result)) => match result {
            Ok(Protocol::Request(Request::Auth { peer_id, response })) => {
                if challenge_response.as_bytes() == &response[..] {
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
