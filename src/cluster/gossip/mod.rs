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

pub mod heartbeat;
pub mod join;
pub mod leave;
pub mod peer;
pub mod ping;
pub mod request;
pub mod spawn;

use super::{
    rpc, Cluster, EpochId, Event, GenerationId, Peer, PeerId, PeerList, ShardId, HEARTBEAT_WINDOW,
    HEARTBEAT_WINDOW_MASK,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use store::log::raft::{LogIndex, TermId};
use store::tracing::error;
use store::Store;

const UDP_MAX_PAYLOAD: usize = 65500;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum State {
    Seed,
    Alive,
    Suspected,
    Offline,
    Left,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PeerStatus {
    pub peer_id: PeerId,
    pub epoch: EpochId,
    pub generation: GenerationId,
    pub last_log_term: TermId,
    pub last_log_index: LogIndex,
}

impl From<&Peer> for PeerStatus {
    fn from(peer: &Peer) -> Self {
        PeerStatus {
            peer_id: peer.peer_id,
            epoch: peer.epoch,
            generation: peer.generation,
            last_log_term: peer.last_log_term,
            last_log_index: peer.last_log_index,
        }
    }
}

impl<T> From<&Cluster<T>> for PeerStatus
where
    T: for<'x> Store<'x> + 'static,
{
    fn from(cluster: &Cluster<T>) -> Self {
        PeerStatus {
            peer_id: cluster.peer_id,
            epoch: cluster.epoch,
            generation: cluster.generation,
            last_log_term: cluster.last_log.term,
            last_log_index: cluster.last_log.index,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PeerInfo {
    pub peer_id: PeerId,
    pub shard_id: ShardId,
    pub epoch: EpochId,
    pub last_log_term: TermId,
    pub last_log_index: LogIndex,
    pub generation: GenerationId,
    pub addr: SocketAddr,
    pub hostname: String,
}

impl From<&Peer> for PeerInfo {
    fn from(peer: &Peer) -> Self {
        PeerInfo {
            peer_id: peer.peer_id,
            shard_id: peer.shard_id,
            epoch: peer.epoch,
            generation: peer.generation,
            addr: peer.addr,
            last_log_index: peer.last_log_index,
            last_log_term: peer.last_log_term,
            hostname: peer.hostname.clone(),
        }
    }
}

impl<T> From<&Cluster<T>> for PeerInfo
where
    T: for<'x> Store<'x> + 'static,
{
    fn from(cluster: &Cluster<T>) -> Self {
        PeerInfo {
            peer_id: cluster.peer_id,
            shard_id: cluster.shard_id,
            epoch: cluster.epoch,
            last_log_index: cluster.last_log.index,
            last_log_term: cluster.last_log.term,
            generation: cluster.generation,
            addr: cluster.addr,
            hostname: cluster.hostname.clone(),
        }
    }
}

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn send_gossip(
        &self,
        dest: SocketAddr,
        request: crate::cluster::gossip::request::Request,
    ) {
        if let Err(err) = self.gossip_tx.send((dest, request)).await {
            error!("Failed to send gossip message: {}", err);
        };
    }
}
