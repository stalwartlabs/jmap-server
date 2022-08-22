pub mod command;
pub mod listener;
pub mod peer;
pub mod request;
pub mod serialize;
pub mod tls;

use self::command::{Command, CommandResponse};

use super::log::{AppendEntriesRequest, AppendEntriesResponse};
use super::{gossip::PeerInfo, PeerId};
use serde::{Deserialize, Serialize};
use store::log::raft::{RaftId, TermId};
use store::tracing::error;
use tokio::sync::oneshot;

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    UpdatePeers {
        peers: Vec<PeerInfo>,
    },
    Auth {
        peer_id: PeerId,
        response: Vec<u8>,
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
    Command {
        command: Command,
    },
    Ping,
    None,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Auth { challenge: [u8; 12] },
    UpdatePeers { peers: Vec<PeerInfo> },
    Vote { term: TermId, vote_granted: bool },
    StepDown { term: TermId },
    AppendEntries(AppendEntriesResponse),
    Command { response: CommandResponse },
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
