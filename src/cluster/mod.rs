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

use self::gossip::PeerInfo;
use self::rpc::command::{Command, CommandResponse};
use crate::JMAPServer;
use actix_web::web;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{net::SocketAddr, sync::atomic::AtomicU8, time::Instant};
use store::log::raft::{LogIndex, RaftId, TermId};
use store::{
    bincode,
    serialize::{StoreDeserialize, StoreSerialize},
    Store,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_rustls::TlsConnector;

pub mod follower;
pub mod gossip;
pub mod init;
pub mod leader;
pub mod log;
pub mod main;
pub mod peer;
pub mod raft;
pub mod rpc;

pub type PeerId = u64;
pub type ShardId = u32;
pub type EpochId = u64;
pub type GenerationId = u64;

pub const IPC_CHANNEL_BUFFER: usize = 1024;
const HEARTBEAT_WINDOW: usize = 1 << 10;
const HEARTBEAT_WINDOW_MASK: usize = HEARTBEAT_WINDOW - 1;

pub const RAFT_LOG_BEHIND: u8 = 0;
pub const RAFT_LOG_UPDATED: u8 = 1;
pub const RAFT_LOG_LEADER: u8 = 2;

pub struct Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    // Local node peer and shard id
    pub peer_id: PeerId,
    pub shard_id: ShardId,

    // Gossip state
    pub generation: GenerationId,
    pub epoch: EpochId,

    // Local gossip address and API urls
    pub addr: SocketAddr,
    pub hostname: String,

    // Cluster key
    pub config: Config,

    // Peer list
    pub peers: Vec<Peer>,
    pub last_peer_pinged: usize,

    // IPC
    pub core: web::Data<JMAPServer<T>>,
    pub tx: mpsc::Sender<Event>,
    pub gossip_tx: mpsc::Sender<(SocketAddr, self::gossip::request::Request)>,
    pub commit_index_tx: watch::Sender<LogIndex>,

    // Raft status
    pub term: TermId,
    pub last_log: RaftId,
    pub uncommitted_index: LogIndex,
    pub state: raft::State,
}

pub struct Config {
    pub key: String,
    pub raft_batch_max: usize,       // 10 * 1024 * 1024
    pub raft_election_timeout: u64,  // 1000
    pub rpc_inactivity_timeout: u64, // 5 * 60 * 1000
    pub rpc_timeout: u64,            // 1000
    pub rpc_retries_max: u32,        // 5
    pub rpc_backoff_max: u64,        // 3 * 60 * 1000 (1 minute)
    pub tls_connector: Arc<TlsConnector>,
    pub tls_domain: String,
}

#[derive(Debug)]
pub enum Event {
    Gossip {
        addr: SocketAddr,
        request: self::gossip::request::Request,
    },
    RpcRequest {
        peer_id: PeerId,
        request: rpc::Request,
        response_tx: oneshot::Sender<rpc::Response>,
    },
    RpcResponse {
        peer_id: PeerId,
        response: rpc::Response,
    },
    RpcCommand {
        command: Command,
        response_tx: oneshot::Sender<CommandResponse>,
    },
    StepDown {
        term: TermId,
    },
    UpdateLastLog {
        last_log: RaftId,
    },
    AdvanceUncommittedIndex {
        uncommitted_index: LogIndex,
    },
    AdvanceCommitIndex {
        peer_id: PeerId,
        commit_index: LogIndex,
    },
    Shutdown,

    #[cfg(test)]
    SetOffline {
        is_offline: bool,
        notify_peers: bool,
    },
}

#[derive(Debug)]
pub struct Peer {
    pub peer_id: PeerId,
    pub shard_id: ShardId,
    pub tx: mpsc::Sender<rpc::RpcEvent>,
    pub online_rx: watch::Receiver<bool>,

    // Peer status
    pub epoch: EpochId,
    pub generation: GenerationId,
    pub state: gossip::State,

    // Peer addresses
    pub addr: SocketAddr,
    pub hostname: String,

    // Heartbeat state
    pub last_heartbeat: Instant,
    pub hb_window: Vec<u32>,
    pub hb_window_pos: usize,
    pub hb_sum: u64,
    pub hb_sq_sum: u64,
    pub hb_is_full: bool,

    // Raft state
    pub last_log_index: LogIndex,
    pub last_log_term: TermId,
    pub commit_index: LogIndex,
    pub vote_granted: bool,
}

pub struct ClusterIpc {
    pub tx: mpsc::Sender<Event>,
    pub state: AtomicU8,
    pub leader_hostname: store::parking_lot::Mutex<Option<String>>,
    pub commit_index_rx: watch::Receiver<LogIndex>,
}

#[derive(Serialize, Deserialize)]
pub struct PeerList {
    peers: Vec<PeerInfo>,
}

impl From<Vec<PeerInfo>> for PeerList {
    fn from(peers: Vec<PeerInfo>) -> Self {
        Self { peers }
    }
}

impl StoreSerialize for PeerList {
    fn serialize(&self) -> Option<Vec<u8>> {
        bincode::serialize(self).ok()
    }
}

impl StoreDeserialize for PeerList {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }
}
