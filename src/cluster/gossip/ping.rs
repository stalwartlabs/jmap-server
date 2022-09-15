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

use super::request::Request;
use super::{rpc, Cluster, PeerStatus};
use store::tracing::debug;
use store::Store;

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn ping_peers(&mut self) -> store::Result<()> {
        // Total and alive peers in the cluster.
        let total_peers = self.peers.len();
        let mut alive_peers: u32 = 0;

        // Start a new election on startup or on an election timeout.
        let mut leader_is_offline = false;
        let leader_peer_id = self.leader_peer_id();

        // Count alive peers and start a new election if the current leader becomes offline.
        for peer in self.peers.iter_mut() {
            if !peer.is_offline() {
                // Failure detection
                if peer.check_heartbeat() {
                    alive_peers += 1;
                } else if !leader_is_offline
                    && leader_peer_id.map(|id| id == peer.peer_id).unwrap_or(false)
                    && peer.hb_sum > 0
                {
                    // Current leader is offline, start election
                    leader_is_offline = true;
                }
            }
        }

        // Start a new election
        if leader_is_offline {
            debug!(
                "[{}] Leader is offline, starting a new election.",
                self.addr
            );
            self.request_votes(true).await?;
        }

        // Find next peer to ping
        for _ in 0..total_peers {
            self.last_peer_pinged = (self.last_peer_pinged + 1) % total_peers;
            let (peer_state, target_addr) = {
                let peer = &self.peers[self.last_peer_pinged];
                (peer.state, peer.addr)
            };

            match peer_state {
                super::State::Seed => {
                    self.send_gossip(
                        target_addr,
                        Request::Join {
                            id: self.last_peer_pinged,
                            port: self.addr.port(),
                        },
                    )
                    .await;
                    break;
                }
                super::State::Alive | super::State::Suspected => {
                    self.epoch += 1;
                    self.send_gossip(target_addr, Request::Ping(self.build_peer_status()))
                        .await;
                    break;
                }
                super::State::Offline if alive_peers == 0 => {
                    // Probe offline nodes
                    self.send_gossip(target_addr, Request::Ping(self.build_peer_status()))
                        .await;
                    break;
                }
                _ => (),
            }
        }

        Ok(())
    }

    pub async fn broadcast_ping(&self) {
        let status = self.build_peer_status();
        for peer in &self.peers {
            if !peer.is_offline() {
                self.send_gossip(peer.addr, Request::Pong(status.clone()))
                    .await;
            }
        }
    }

    pub async fn handle_ping(&mut self, peers: Vec<PeerStatus>, send_pong: bool) {
        if peers.is_empty() {
            debug!("Received empty ping packet.");
        }

        let mut source_peer_idx = None;

        // Increase epoch
        self.epoch += 1;

        let mut do_full_sync = self.peers.len() + 1 != peers.len();

        'outer: for (pos, peer) in peers.iter().enumerate() {
            if self.peer_id != peer.peer_id {
                for (idx, mut local_peer) in self.peers.iter_mut().enumerate() {
                    if local_peer.peer_id == peer.peer_id {
                        if peer.epoch > local_peer.epoch {
                            local_peer.epoch = peer.epoch;
                            local_peer.last_log_index = peer.last_log_index;
                            local_peer.last_log_term = peer.last_log_term;
                            if local_peer.update_heartbeat(pos == 0) {
                                // This peer reconnected
                                if pos != 0 && local_peer.is_in_shard(self.shard_id) {
                                    // Wake up RPC process
                                    local_peer.dispatch_request(rpc::Request::Ping).await;
                                } else {
                                    do_full_sync = true;
                                }
                            }

                            if (local_peer.generation != peer.generation) && !do_full_sync {
                                do_full_sync = true;
                            }
                        }

                        // Keep idx of first item, the source peer.
                        if pos == 0 {
                            source_peer_idx = idx.into();
                        }
                        continue 'outer;
                    }
                }
                if !do_full_sync {
                    do_full_sync = true;
                }
            }
        }

        if let Some(source_peer_idx) = source_peer_idx {
            if do_full_sync {
                self.peers[source_peer_idx]
                    .dispatch_request(rpc::Request::UpdatePeers {
                        peers: self.build_peer_info(),
                    })
                    .await;
            } else if send_pong {
                self.send_gossip(
                    self.peers[source_peer_idx].addr,
                    Request::Pong(self.build_peer_status()),
                )
                .await;
            }
        } else {
            debug!(
                "Received peers sync packet from unknown peer id: {}",
                peers.first().unwrap().peer_id
            );
        }
    }
}
