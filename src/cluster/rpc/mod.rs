/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

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
