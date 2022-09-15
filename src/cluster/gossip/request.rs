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

use std::time::SystemTime;

use store::serialize::leb128::{Leb128Iterator, Leb128Vec};

use crate::authorization::SymmetricEncrypt;

use super::{PeerStatus, UDP_MAX_PAYLOAD};

#[derive(Debug)]
pub enum Request {
    Join { id: usize, port: u16 },
    JoinReply { id: usize },
    Ping(Vec<PeerStatus>),
    Pong(Vec<PeerStatus>),
    Leave(Vec<PeerStatus>),
}

impl Request {
    const JOIN: usize = 0;
    const JOIN_REPLY: usize = 1;
    const PING: usize = 2;
    const PONG: usize = 3;
    const LEAVE: usize = 4;

    pub fn from_bytes(bytes: &[u8]) -> Option<Request> {
        let mut it = bytes.iter();
        match it.next_leb128()? {
            Self::JOIN => Request::Join {
                id: it.next_leb128()?,
                port: it.next_leb128()?,
            },
            Self::JOIN_REPLY => Request::JoinReply {
                id: it.next_leb128()?,
            },
            mut num_peers => {
                num_peers -= Self::PING;
                if num_peers > (UDP_MAX_PAYLOAD / std::mem::size_of::<PeerStatus>()) {
                    return None;
                }
                let mut peers = Vec::with_capacity(num_peers - 2);
                while num_peers > 0 {
                    let peer_id = if let Some(peer_id) = it.next_leb128() {
                        peer_id
                    } else {
                        break;
                    };
                    peers.push(PeerStatus {
                        peer_id,
                        epoch: it.next_leb128()?,
                        generation: it.next_leb128()?,
                        last_log_term: it.next_leb128()?,
                        last_log_index: it.next_leb128()?,
                    });
                    num_peers -= 1;
                }
                match num_peers {
                    0 => Request::Ping(peers),
                    1 => Request::Pong(peers),
                    2 => Request::Leave(peers),
                    _ => return None,
                }
            }
        }
        .into()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let (flag, peers) = match self {
            Request::Join { id, port } => {
                let mut bytes = Vec::with_capacity(
                    std::mem::size_of::<usize>()
                        + std::mem::size_of::<u64>()
                        + std::mem::size_of::<u16>()
                        + SymmetricEncrypt::ENCRYPT_TAG_LEN
                        + 1,
                );
                bytes.push(Self::JOIN as u8);
                bytes.push_leb128(*id);
                bytes.push_leb128(*port);
                bytes.push_leb128(
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0),
                );
                return bytes;
            }
            Request::JoinReply { id } => {
                let mut bytes = Vec::with_capacity(
                    std::mem::size_of::<usize>()
                        + std::mem::size_of::<u64>()
                        + SymmetricEncrypt::ENCRYPT_TAG_LEN
                        + 1,
                );
                bytes.push(Self::JOIN_REPLY as u8);
                bytes.push_leb128(*id);
                bytes.push_leb128(
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0),
                );
                return bytes;
            }
            Request::Ping(peers) => (Self::PING, peers),
            Request::Pong(peers) => (Self::PONG, peers),
            Request::Leave(peers) => (Self::LEAVE, peers),
        };

        debug_assert!(!peers.is_empty());

        let mut bytes = Vec::with_capacity(
            std::mem::size_of::<usize>()
                + (peers.len() * std::mem::size_of::<PeerStatus>())
                + SymmetricEncrypt::ENCRYPT_TAG_LEN,
        );
        bytes.push_leb128(flag + peers.len());

        for peer in peers {
            bytes.push_leb128(peer.peer_id);
            bytes.push_leb128(peer.epoch);
            bytes.push_leb128(peer.generation);
            bytes.push_leb128(peer.last_log_term);
            bytes.push_leb128(peer.last_log_index);
        }

        bytes
    }
}
