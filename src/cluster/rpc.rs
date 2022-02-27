use std::{net::SocketAddr, time::Duration};

use actix_web::web::{self, Buf};
use futures::{stream::StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use store::leb128::Leb128;
use store::Store;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time,
};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{debug, error};

use crate::JMAPServer;

use super::{
    gossip::{build_peer_info, sync_peer_info, PeerInfo},
    raft::{
        handle_follow_leader_request, handle_vote_request, handle_vote_response, LogIndex, TermId,
    },
    Cluster, PeerId, IPC_CHANNEL_BUFFER,
};

pub const RPC_MAX_PARALLEL: usize = 5;
pub const RPC_TIMEOUT_MS: u64 = 1000;
pub const RPC_INACTIVITY_TIMEOUT: u64 = 5 * 60 * 1000;
const MAX_FRAME_LENGTH: usize = 50 * 1024 * 1024; //TODO configure

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Request {
    SynchronizePeers(Vec<PeerInfo>),
    AuthRequest {
        peer_id: PeerId,
        key: String,
    },
    VoteRequest {
        term: TermId,
        last_log_index: LogIndex,
        last_log_term: TermId,
    },
    VoteResponse {
        term: TermId,
        vote_granted: bool,
    },
    FollowLeaderRequest {
        term: TermId,
        last_log_index: LogIndex,
        last_log_term: TermId,
    },
    FollowLeaderResponse {
        term: TermId,
        success: bool,
    },
}

impl From<&Cluster> for Request {
    fn from(cluster: &Cluster) -> Self {
        Request::SynchronizePeers(build_peer_info(cluster))
    }
}

pub struct RpcProtocol {}

impl Default for RpcProtocol {
    fn default() -> Self {
        Self {}
    }
}

impl Decoder for RpcProtocol {
    type Item = Request;

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

        let result = bincode::deserialize::<Request>(&src[bytes_read..bytes_read + frame_len])
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

impl Encoder<Request> for RpcProtocol {
    type Error = std::io::Error;

    fn encode(&mut self, item: Request, dst: &mut web::BytesMut) -> Result<(), Self::Error> {
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

pub async fn start_rpc<T>(core: web::Data<JMAPServer<T>>, bind_addr: SocketAddr)
where
    T: for<'x> Store<'x> + 'static,
{
    // Start listener for RPC requests
    let listener = TcpListener::bind(bind_addr).await.unwrap_or_else(|e| {
        panic!("Failed to bind RPC listener to {}: {}", bind_addr, e);
    });
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let core = core.clone();
                    tokio::spawn(async move {
                        handle_conn(core, stream).await;
                    });
                }
                Err(err) => {
                    error!("Failed to accept TCP connection: {}", err);
                }
            }
        }
    });
}

pub fn new_rpc_channel<T>(
    core: web::Data<JMAPServer<T>>,
    peer_id: PeerId,
    peer_addr: SocketAddr,
) -> mpsc::Sender<Request>
where
    T: for<'x> Store<'x> + 'static,
{
    let (tx, mut rx) = mpsc::channel::<Request>(IPC_CHANNEL_BUFFER);

    tokio::spawn(async move {
        let auth_frame = {
            let cluster = core.cluster.lock();
            Request::AuthRequest {
                peer_id: cluster.peer_id,
                key: cluster.key.clone(),
            }
        };

        let mut conn_ = None;

        loop {
            let request =
                match time::timeout(Duration::from_millis(RPC_INACTIVITY_TIMEOUT), rx.recv()).await
                {
                    Ok(Some(request)) => request,
                    Ok(None) => {
                        debug!("Peer RPC process exiting.");
                        break;
                    }
                    Err(_) => {
                        // Close connection after 5 minutes of inactivity
                        if conn_.is_some() {
                            conn_ = None;
                        }
                        continue;
                    }
                };
            debug!("Received RPC request: {:?}", request);

            // Connect to peer if not already connected
            let conn = if let Some(conn) = &mut conn_ {
                conn
            } else {
                match connect_peer(peer_addr, auth_frame.clone()).await {
                    Ok(conn) => {
                        conn_ = conn.into();
                        conn_.as_mut().unwrap()
                    }
                    Err(err) => {
                        error!(
                            "Failed to connect to peer {} at {}: {}",
                            peer_id, peer_addr, err
                        );
                        continue;
                    }
                }
            };

            match send_rpc(conn, request).await {
                Ok(response) => {
                    debug!("Peer response: {:?}", response);
                    match response {
                        Request::SynchronizePeers(peers) => {
                            debug!("Successful full sync, received {} peers.", peers.len());
                            sync_peer_info(&core, peers, false);
                        }
                        Request::VoteResponse { term, vote_granted } => {
                            handle_vote_response(&core, peer_id, term, vote_granted).await;
                        }
                        Request::FollowLeaderResponse { term, success } => {
                            let mut cluster = core.cluster.lock();
                            if cluster.term < term {
                                cluster.step_down(term);
                            } else if !success {
                                cluster.start_election_timer();
                            }
                        }
                        _ => {
                            error!("Unexpected response from peer {}: {:?}", peer_id, response);
                        }
                    }
                }
                Err(err) => {
                    error!(
                        "Failed to send RPC request to peer {} at {}: {}",
                        peer_id, peer_addr, err
                    );
                    conn_ = None;
                }
            }
        }
    });

    tx
}

async fn handle_conn<T>(core: web::Data<JMAPServer<T>>, stream: TcpStream)
where
    T: for<'x> Store<'x> + 'static,
{
    let peer_addr = stream.peer_addr().unwrap();
    let mut frames = Framed::new(stream, RpcProtocol::default());

    let peer_id = match time::timeout(Duration::from_millis(RPC_TIMEOUT_MS), frames.next()).await {
        Ok(Some(result)) => match result {
            Ok(Request::AuthRequest { peer_id, key }) => {
                if core.cluster.lock().key == key {
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

    while let Some(frame) = frames.next().await {
        match frame {
            Ok(request) => {
                debug!("Received RPC request from {}: {:?}", peer_id, request);
                let response = {
                    match request {
                        Request::SynchronizePeers(peers) => {
                            Request::SynchronizePeers(sync_peer_info(&core, peers, true).unwrap())
                        }
                        Request::VoteRequest {
                            term,
                            last_log_index,
                            last_log_term,
                        } => handle_vote_request(
                            &mut core.cluster.lock(),
                            peer_id,
                            term,
                            last_log_index,
                            last_log_term,
                        ),
                        Request::FollowLeaderRequest {
                            term,
                            last_log_index,
                            last_log_term,
                        } => handle_follow_leader_request(
                            &mut core.cluster.lock(),
                            peer_id,
                            term,
                            last_log_index,
                            last_log_term,
                        ),
                        _ => {
                            error!("Received unexpected RPC request from {}.", peer_id);
                            return;
                        }
                    }
                };

                if let Err(err) = frames.send(response).await {
                    error!("Failed to send RPC response: {}", err);
                    return;
                }
            }
            Err(err) => {
                error!("Failed to read RPC request from {}: {}", peer_addr, err);
                return;
            }
        }
    }
}

async fn connect_peer(
    addr: SocketAddr,
    auth_frame: Request,
) -> std::io::Result<Framed<TcpStream, RpcProtocol>> {
    time::timeout(Duration::from_millis(RPC_TIMEOUT_MS), async {
        let mut conn = Framed::new(TcpStream::connect(&addr).await?, RpcProtocol::default());
        conn.send(auth_frame).await?;
        Ok(conn)
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
    conn: &mut Framed<TcpStream, RpcProtocol>,
    request: Request,
) -> std::io::Result<Request> {
    conn.send(request).await?;
    match conn.next().await {
        Some(Ok(response)) => Ok(response),
        Some(Err(err)) => Err(err),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "RPC connection unexpectedly closed.",
        )),
    }
}
