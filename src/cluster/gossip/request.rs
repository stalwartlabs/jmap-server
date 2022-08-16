use std::time::SystemTime;

use crate::authorization::SymmetricEncrypt;

use super::{EpochId, GenerationId, PeerId, PeerStatus, UDP_MAX_PAYLOAD};
use store::log::raft::{LogIndex, TermId};
use store::serialize::leb128::Leb128;

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
        match usize::from_leb128_it(&mut it)? {
            Self::JOIN => Request::Join {
                id: usize::from_leb128_it(&mut it)?,
                port: u16::from_leb128_it(&mut it)?,
            },
            Self::JOIN_REPLY => Request::JoinReply {
                id: usize::from_leb128_it(&mut it)?,
            },
            mut num_peers => {
                num_peers -= Self::PING;
                if num_peers > (UDP_MAX_PAYLOAD / std::mem::size_of::<PeerStatus>()) {
                    return None;
                }
                let mut peers = Vec::with_capacity(num_peers - 2);
                while num_peers > 0 {
                    let peer_id = if let Some(peer_id) = PeerId::from_leb128_it(&mut it) {
                        peer_id
                    } else {
                        break;
                    };
                    peers.push(PeerStatus {
                        peer_id,
                        epoch: EpochId::from_leb128_it(&mut it)?,
                        generation: GenerationId::from_leb128_it(&mut it)?,
                        last_log_term: TermId::from_leb128_it(&mut it)?,
                        last_log_index: LogIndex::from_leb128_it(&mut it)?,
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
                id.to_leb128_bytes(&mut bytes);
                port.to_leb128_bytes(&mut bytes);
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0)
                    .to_leb128_bytes(&mut bytes);
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
                id.to_leb128_bytes(&mut bytes);
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0)
                    .to_leb128_bytes(&mut bytes);
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
        (flag + peers.len()).to_leb128_bytes(&mut bytes);

        for peer in peers {
            peer.peer_id.to_leb128_bytes(&mut bytes);
            peer.epoch.to_leb128_bytes(&mut bytes);
            peer.generation.to_leb128_bytes(&mut bytes);
            peer.last_log_term.to_leb128_bytes(&mut bytes);
            peer.last_log_index.to_leb128_bytes(&mut bytes);
        }

        bytes
    }
}
