use std::time::Duration;

use actix_web::web;
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use store::Store;
use tokio::sync::mpsc;
use tracing::{debug, error};

use crate::{error::JMAPServerError, JMAPServer};

use super::{
    gossip::{build_peer_info, sync_peer_info, PeerInfo, PING_INTERVAL},
    raft::{
        handle_follow_leader_request, handle_follow_leader_responses, handle_join_raft_responses,
        handle_vote_request, handle_vote_responses, LogIndex, TermId,
    },
    Cluster, Message, PeerId, IPC_CHANNEL_BUFFER,
};

pub const MAX_PARALLEL_REQUESTS: usize = 5;

#[derive(Debug, Serialize, Deserialize)]
struct Request {
    pub key: String,
    pub peer_id: PeerId,
    pub cmd: Command,
}

impl Request {
    pub fn new(peer_id: PeerId, key: String, cmd: Command) -> Self {
        Self { peer_id, key, cmd }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub peer_id: PeerId,
    pub cmd: Command,
}

impl Response {
    pub fn new(peer_id: PeerId, cmd: Command) -> Self {
        Self { peer_id, cmd }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    SynchronizePeers(Vec<PeerInfo>),
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
    JoinRaftRequest,
    JoinRaftResponse {
        term: TermId,
        leader_id: Option<PeerId>,
    },
}

impl Default for Command {
    fn default() -> Self {
        Command::VoteRequest {
            term: 0,
            last_log_index: 0,
            last_log_term: 0,
        }
    }
}

impl From<&Cluster> for Command {
    fn from(cluster: &Cluster) -> Self {
        Command::SynchronizePeers(build_peer_info(cluster))
    }
}

pub async fn handle_rpc<T>(
    request: web::Bytes,
    core: web::Data<JMAPServer<T>>,
) -> Result<web::Bytes, JMAPServerError>
where
    T: for<'x> Store<'x> + 'static,
{
    let mut cluster = core
        .cluster
        .lock()
        .map_err(|_| JMAPServerError::from("Failed to obtain lock."))?;

    if !cluster.is_enabled() {
        return Err(JMAPServerError::from("Cluster not configured."));
    }

    let request = bincode::deserialize::<Request>(&request).map_err(|e| {
        JMAPServerError::from(format!(
            "Failed to deserialize RPC request: {}",
            e.to_string()
        ))
    })?;

    if request.key != cluster.key {
        debug!("Received RPC with invalid key: {}", request.key);
        return Err(JMAPServerError::from("Invalid cluster key."));
    }

    //debug!("OutA {:?} from {}", request.cmd, request.peer_id);

    let command = match request.cmd {
        Command::SynchronizePeers(peers) => {
            sync_peer_info(&mut cluster, peers);
            Command::SynchronizePeers(build_peer_info(&cluster))
        }
        Command::VoteRequest {
            term,
            last_log_index,
            last_log_term,
        } => handle_vote_request(
            &mut cluster,
            request.peer_id,
            term,
            last_log_index,
            last_log_term,
        ),
        Command::FollowLeaderRequest {
            term,
            last_log_index,
            last_log_term,
        } => handle_follow_leader_request(
            &mut cluster,
            request.peer_id,
            term,
            last_log_index,
            last_log_term,
        ),
        Command::JoinRaftRequest => Command::JoinRaftResponse {
            term: cluster.term,
            leader_id: cluster.leader_peer_id(),
        },
        _ => {
            return Err(JMAPServerError::from("Invalid command."));
        }
    };

    //debug!("InA {:?} to {}.", command, request.peer_id);

    Ok(bincode::serialize(&Response::new(cluster.peer_id, command))
        .map_err(|e| {
            JMAPServerError::from(format!(
                "Failed to serialize RPC response: {}",
                e.to_string()
            ))
        })?
        .into())
}

pub async fn start_rpc_queue<T>(core: web::Data<JMAPServer<T>>) -> mpsc::Sender<Message>
where
    T: for<'x> Store<'x> + 'static,
{
    let mut request = if let Ok(cluster) = core.cluster.lock() {
        Request::new(cluster.peer_id, cluster.key.clone(), Command::default())
    } else {
        panic!("Failed to obtain cluster lock.");
    };

    let (tx, mut rx) = mpsc::channel::<Message>(IPC_CHANNEL_BUFFER);

    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            //debug!("OutB {:?}", message);

            match message {
                Message::SyncResponse { url, peers } => {
                    request.cmd = Command::SynchronizePeers(peers);
                    if let Some(Command::SynchronizePeers(peer_info)) =
                        post_one(&url, &request).await.map(|r| r.cmd)
                    {
                        core.cluster
                            .lock()
                            .map(|mut cluster| {
                                debug!(
                                    "Successful full sync with {}, received {} peers.",
                                    url,
                                    peer_info.len()
                                );
                                sync_peer_info(&mut cluster, peer_info);
                            })
                            .unwrap_or_else(|_| {
                                error!("Failed to obtain cluster lock.");
                            });
                    }
                }
                Message::VoteRequest {
                    urls,
                    term,
                    last_log_index,
                    last_log_term,
                } => {
                    request.cmd = Command::VoteRequest {
                        term,
                        last_log_index,
                        last_log_term,
                    };
                    if let Some(become_leader) = post_many(urls.clone(), &request)
                        .await
                        .map(|responses| {
                            core.cluster
                                .lock()
                                .map(|mut cluster| handle_vote_responses(&mut cluster, responses))
                                .unwrap_or_default()
                        })
                        .unwrap_or_default()
                    {
                        request.cmd = become_leader;

                        post_many(urls, &request)
                            .await
                            .map(|responses| {
                                core.cluster
                                    .lock()
                                    .map(|mut cluster| {
                                        //debug!("In FollowLeaderResponses {:?}", responses);

                                        handle_follow_leader_responses(&mut cluster, responses)
                                    })
                                    .unwrap_or_default()
                            })
                            .unwrap_or_default();
                    }
                }
                Message::JoinRaftRequest { urls } => {
                    request.cmd = Command::JoinRaftRequest;

                    post_many(urls.clone(), &request)
                        .await
                        .map(|responses| {
                            core.cluster
                                .lock()
                                .map(|mut cluster| {
                                    //debug!("In JoinRaftResponses {:?}", responses);

                                    handle_join_raft_responses(&mut cluster, responses)
                                })
                                .unwrap_or_default()
                        })
                        .unwrap_or_default();
                }
                _ => unreachable!(),
            }
        }
    });

    tx
}

fn build_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_millis(PING_INTERVAL))
        .connect_timeout(Duration::from_millis(PING_INTERVAL))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

async fn post_many(urls: Vec<String>, request: &Request) -> Option<Vec<Option<Response>>> {
    let body = match bincode::serialize(&request) {
        Ok(body) => body,
        Err(err) => {
            error!("Failed to serialize RPC request to {:?}: {}", urls, err);
            return None;
        }
    };
    let client = build_client();
    let mut responses = Vec::with_capacity(urls.len());

    let mut results = stream::iter(urls)
        .map(|url| {
            let client = client.clone();
            let body = body.clone();
            tokio::spawn(async move { client.post(url).body(body).send().await?.bytes().await })
        })
        .buffer_unordered(MAX_PARALLEL_REQUESTS);

    while let Some(result) = results.next().await {
        match result {
            Ok(Ok(bytes)) => match bincode::deserialize::<Response>(&bytes) {
                Ok(response) => {
                    responses.push(Some(response));
                }
                Err(err) => {
                    error!("Failed to deserialize RPC response: {}", err);
                    return None;
                }
            },
            Ok(Err(err)) => {
                debug!("Failed HTTP request while sending RPC request: {}", err);
                responses.push(None);
            }
            Err(err) => {
                error!("Failed to send RPC request: {}", err);
                return None;
            }
        }
    }

    Some(responses)
}

async fn post_one(url: &str, request: &Request) -> Option<Response> {
    let body = match bincode::serialize(&request) {
        Ok(body) => body,
        Err(err) => {
            error!("Failed to serialize RPC request to {}: {}", url, err);
            return None;
        }
    };

    match build_client().post(url).body(body).send().await {
        Ok(response) => match response.bytes().await {
            Ok(bytes) => match bincode::deserialize::<Response>(&bytes) {
                Ok(response) => Some(response),
                Err(err) => {
                    error!("Failed to deserialize RPC response from {}: {}", url, err);
                    None
                }
            },
            Err(err) => {
                error!("Failed to process RPC request to {}: {}", url, err);
                None
            }
        },
        Err(err) => {
            error!("Failed to post RPC request to {}: {}", url, err);
            None
        }
    }
}
