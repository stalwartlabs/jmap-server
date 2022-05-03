use std::time::Instant;
use std::{net::SocketAddr, time::Duration};

use actix_web::web::{self, Buf};
use futures::{stream::StreamExt, SinkExt};
use rand::Rng;
use serde::{Deserialize, Serialize};
use store::bincode;
use store::log::raft::{RaftId, TermId};
use store::serialize::leb128::Leb128;
use store::tracing::{debug, error};
use tokio::sync::watch;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    time::{self},
};
use tokio_util::codec::{Decoder, Encoder, Framed};

use super::log::{AppendEntriesRequest, AppendEntriesResponse};
use super::{gossip::PeerInfo, Event, Peer, PeerId, IPC_CHANNEL_BUFFER};

const RPC_TIMEOUT_MS: u64 = 1000;
const RPC_MAX_BACKOFF_MS: u64 = 3 * 60 * 1000; // 1 minute
const RPC_MAX_CONNECT_ATTEMPTS: u32 = 5;
const RPC_INACTIVITY_TIMEOUT: u64 = 5 * 60 * 1000; //TODO configure
const MAX_FRAME_LENGTH: usize = 50 * 1024 * 1024; //TODO configure

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    UpdatePeers {
        peers: Vec<PeerInfo>,
    },
    Auth {
        peer_id: PeerId,
        key: String,
    },
    Vote {
        term: TermId,
        last: RaftId,
    },
    BecomeFollower {
        term: TermId,
        last_log: RaftId,
    },
    AppendEntries {
        term: TermId,
        request: AppendEntriesRequest,
    },
    Ping,
    None,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    UpdatePeers { peers: Vec<PeerInfo> },
    Vote { term: TermId, vote_granted: bool },
    StepDown { term: TermId },
    AppendEntries(AppendEntriesResponse),
    Pong,
    UnregisteredPeer,
    None,
}

pub enum RpcEvent {
    FireAndForget {
        request: Request,
    },
    NeedResponse {
        request: Request,
        response_tx: oneshot::Sender<Response>,
    },
}
#[derive(Default)]
pub struct RpcEncoder {}

#[derive(Debug, Serialize, Deserialize)]
pub enum Protocol {
    Request(Request),
    Response(Response),
}

impl Protocol {
    pub fn unwrap_request(self) -> Request {
        match self {
            Protocol::Request(req) => req,
            _ => Request::None,
        }
    }

    pub fn unwrap_response(self) -> Response {
        match self {
            Protocol::Response(res) => res,
            _ => Response::None,
        }
    }
}

impl Decoder for RpcEncoder {
    type Item = Protocol;

    type Error = std::io::Error;

    fn decode(&mut self, src: &mut web::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < std::mem::size_of::<u32>() {
            // Not enough data to read length marker.
            return Ok(None);
        }
        let (frame_len, bytes_read) = usize::from_leb128_bytes(src).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Failed to decode frame length.",
            )
        })?;

        if frame_len > MAX_FRAME_LENGTH {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Frame of length {} is too large.", frame_len),
            ));
        } else if src.len() < bytes_read + frame_len {
            src.reserve(bytes_read + frame_len - src.len());
            return Ok(None);
        }

        let result = bincode::deserialize::<Protocol>(&src[bytes_read..bytes_read + frame_len])
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to deserialize RPC request.: {}", e),
                )
            });
        src.advance(bytes_read + frame_len);

        Ok(Some(result?))
    }
}

impl Encoder<Protocol> for RpcEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: Protocol, dst: &mut web::BytesMut) -> Result<(), Self::Error> {
        let bytes = bincode::serialize(&item).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to serialize RPC request.: {}", e),
            )
        })?;
        let mut bytes_len = Vec::with_capacity(std::mem::size_of::<u32>() + 1);
        bytes.len().to_leb128_bytes(&mut bytes_len);

        dst.reserve(bytes_len.len() + bytes.len());
        dst.extend_from_slice(&bytes_len);
        dst.extend_from_slice(&bytes);
        Ok(())
    }
}

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

impl RpcEvent {
    pub fn failed(self) {
        if let RpcEvent::NeedResponse { response_tx, .. } = self {
            if response_tx.send(Response::None).is_err() {
                error!("Channel failed while sending message.");
            }
        }
    }
}

pub fn spawn_peer_rpc(
    main_tx: mpsc::Sender<Event>,
    local_peer_id: PeerId,
    key: String,
    peer_id: PeerId,
    peer_addr: SocketAddr,
) -> (mpsc::Sender<RpcEvent>, watch::Receiver<bool>) {
    let (event_tx, mut event_rx) = mpsc::channel::<RpcEvent>(IPC_CHANNEL_BUFFER);
    let (online_tx, online_rx) = watch::channel(false);

    tokio::spawn(async move {
        let mut conn_ = None;
        let mut is_online = false;

        'main: loop {
            let mut message = match time::timeout(
                Duration::from_millis(RPC_INACTIVITY_TIMEOUT),
                event_rx.recv(),
            )
            .await
            {
                Ok(Some(message)) => message,
                Ok(None) => {
                    debug!("Peer RPC process with {} exiting.", peer_addr);
                    break;
                }
                Err(_) => {
                    // Close connection after the configured inactivity timeout.
                    if conn_.is_some() {
                        debug!("Closing inactive connection to peer {}.", peer_addr);
                        conn_ = None;
                    }
                    continue;
                }
            };

            // Connect to peer if we are not already connected.
            let conn = if let Some(conn) = &mut conn_ {
                conn
            } else {
                let mut connection_attempts = 0;

                'retry: loop {
                    // Connect and authenticate with peer.
                    match connect_peer(
                        peer_addr,
                        Request::Auth {
                            peer_id: local_peer_id,
                            key: key.clone(),
                        },
                    )
                    .await
                    {
                        Ok(conn) => {
                            conn_ = conn.into();

                            // Notify processes that the peer is online.
                            if !is_online {
                                is_online = true;
                                if online_tx.send(true).is_err() {
                                    debug!("Failed to send online status.");
                                }
                            }

                            if connection_attempts < RPC_MAX_CONNECT_ATTEMPTS {
                                // Connection established, send message.
                                break 'retry;
                            } else {
                                // Connection established, but we have already notified the task the current
                                // message was undeliverable. Continue with the next message on the queue.
                                continue 'main;
                            }
                        }
                        Err(err) => {
                            // Keep retrying.
                            connection_attempts += 1;

                            if connection_attempts == RPC_MAX_CONNECT_ATTEMPTS {
                                // Give up trying to deliver the message,
                                // notify task that the message could not be sent.
                                message.failed();
                                message = RpcEvent::FireAndForget {
                                    request: Request::None,
                                };
                            }

                            // Truncated exponential backoff
                            let mut next_attempt_ms = std::cmp::min(
                                2u64.pow(connection_attempts)
                                    + rand::thread_rng().gen_range(0..1000),
                                RPC_MAX_BACKOFF_MS,
                            );

                            error!(
                                "Failed to connect to peer {} ({}), retrying in {} ms.",
                                peer_addr, err, next_attempt_ms
                            );

                            // Reject messages while we wait to reconnect.
                            'wait: loop {
                                let timer = Instant::now();

                                match time::timeout(
                                    Duration::from_millis(next_attempt_ms),
                                    event_rx.recv(),
                                )
                                .await
                                {
                                    Ok(Some(new_message)) => {
                                        match new_message {
                                            new_message @ RpcEvent::FireAndForget {
                                                request: Request::UpdatePeers { .. } | Request::Ping,
                                            } => {
                                                // Peer requested to update peer list via gossip, which means that
                                                // it is probably back online, attempt to reconnect.
                                                message = new_message;
                                                connection_attempts = 0;
                                                continue 'retry;
                                            }
                                            _ => {
                                                // Do not accept new messages until we are able to reconnect.
                                                new_message.failed();
                                            }
                                        }
                                    }
                                    Ok(None) => {
                                        // RPC process was ended.
                                        debug!("Peer RPC process with {} exiting.", peer_addr);
                                        break 'main;
                                    }
                                    Err(_) => {
                                        // Timeout reached, attempt to reconnect.
                                        break 'wait;
                                    }
                                }

                                // Continue waiting to reconnect.
                                let elapsed_ms = timer.elapsed().as_millis() as u64;
                                if next_attempt_ms > elapsed_ms {
                                    next_attempt_ms -= elapsed_ms;
                                } else {
                                    break 'wait;
                                }
                            }

                            continue 'retry;
                        }
                    }
                }

                conn_.as_mut().unwrap()
            };

            let err = match message {
                RpcEvent::NeedResponse {
                    response_tx,
                    request,
                } => match send_rpc(conn, request).await {
                    Ok(response) => {
                        // Send response via oneshot channel
                        if response_tx.send(response).is_err() {
                            error!("Channel failed while sending message.");
                        }
                        continue;
                    }
                    Err(err) => {
                        if response_tx.send(Response::None).is_err() {
                            error!("Channel failed while sending message.");
                        }
                        err
                    }
                },
                RpcEvent::FireAndForget { request } => match send_rpc(conn, request).await {
                    Ok(response) => {
                        // Send response via the main channel
                        if let Err(err) =
                            main_tx.send(Event::RpcResponse { peer_id, response }).await
                        {
                            error!("Channel failed while sending message: {}", err);
                        }
                        continue;
                    }
                    Err(err) => err,
                },
            };

            debug!("Failed to send RPC request to peer {}: {}", peer_addr, err);
            conn_ = None;

            // Notify processes that the peer is offline.
            is_online = false;
            if online_tx.send(false).is_err() {
                debug!("Failed to send online status.");
            }
        }
    });

    (event_tx, online_rx)
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
            Err(_) => todo!(),
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

async fn connect_peer(
    addr: SocketAddr,
    auth_frame: Request,
) -> std::io::Result<Framed<TcpStream, RpcEncoder>> {
    time::timeout(Duration::from_millis(RPC_TIMEOUT_MS), async {
        let mut conn = Framed::new(TcpStream::connect(&addr).await?, RpcEncoder::default());
        if let Response::Pong = send_rpc(&mut conn, auth_frame).await? {
            Ok(conn)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to authenticate peer.",
            ))
        }
    })
    .await
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("RPC connection to {} timed out.", addr),
        )
    })?
}

async fn send_rpc(
    conn: &mut Framed<TcpStream, RpcEncoder>,
    request: Request,
) -> std::io::Result<Response> {
    conn.send(Protocol::Request(request)).await?;
    match conn.next().await {
        Some(Ok(Protocol::Response(response))) => Ok(response),
        Some(Ok(invalid)) => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Received invalid RPC response: {:?}", invalid),
        )),
        Some(Err(err)) => Err(err),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "RPC connection unexpectedly closed.",
        )),
    }
}

impl Peer {
    // Sends a request and "waits" asynchronically until the response is available.
    pub async fn send_request(&self, request: Request) -> Response {
        let (response_tx, rx) = oneshot::channel();
        if let Err(err) = self
            .tx
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

    // Submits a request, the result is returned at a later time via the main channel.
    pub async fn dispatch_request(&self, request: Request) {
        //debug!("OUT: {:?}", request);
        if let Err(err) = self.tx.send(RpcEvent::FireAndForget { request }).await {
            error!("Channel failed: {}", err);
        }
    }
}
