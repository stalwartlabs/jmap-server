pub mod listener;
pub mod peer;
pub mod request;
pub mod serialize;

use super::log::{AppendEntriesRequest, AppendEntriesResponse};
use super::{gossip::PeerInfo, PeerId};
use serde::{Deserialize, Serialize};
use store::log::raft::{RaftId, TermId};
use store::tracing::error;
use tokio::sync::oneshot;

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

#[derive(Debug, Serialize, Deserialize)]
pub enum Protocol {
    Request(Request),
    Response(Response),
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
