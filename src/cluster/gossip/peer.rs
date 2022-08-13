use super::{rpc, Cluster, Peer, PeerInfo, PeerList, PeerStatus};
use crate::cluster::gossip::State;
use crate::cluster::rpc::peer::spawn_peer_rpc;
use store::tracing::{debug, error, info};
use store::Store;
use tokio::sync::oneshot;

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn sync_peer_info(&mut self, peers: Vec<PeerInfo>) {
        let mut remove_seeds = false;
        let mut peers_changed = false;
        let is_leading = self.is_leading();

        'outer: for (pos, peer) in peers.into_iter().enumerate() {
            if peer.peer_id != self.peer_id {
                for local_peer in self.peers.iter_mut() {
                    if !local_peer.is_seed() {
                        if local_peer.peer_id == peer.peer_id {
                            let mut update_peer_info =
                                pos == 0 && local_peer.generation != peer.generation;

                            if peer.epoch > local_peer.epoch {
                                if !update_peer_info && local_peer.generation != peer.generation {
                                    update_peer_info = true;
                                }
                                local_peer.epoch = peer.epoch;
                                local_peer.last_log_index = peer.last_log_index;
                                local_peer.last_log_term = peer.last_log_term;
                                if local_peer.update_heartbeat(pos == 0)
                                    && local_peer.is_in_shard(self.shard_id)
                                {
                                    // Wake up RPC process
                                    local_peer.dispatch_request(rpc::Request::Ping).await;
                                }
                            }

                            // Update peer info if generationId has changed and
                            // the request comes from the peer itself, or if the epoch is higher.
                            if update_peer_info {
                                if local_peer.addr != peer.addr {
                                    // Peer changed its address, reconnect.
                                    let (tx, online_rx) = spawn_peer_rpc(
                                        self.tx.clone(),
                                        self.peer_id,
                                        &self.config,
                                        peer.peer_id,
                                        peer.addr,
                                    );
                                    local_peer.addr = peer.addr;
                                    local_peer.tx = tx;
                                    local_peer.online_rx = online_rx;
                                }
                                local_peer.generation = peer.generation;
                                local_peer.shard_id = peer.shard_id;
                                local_peer.hostname = peer.hostname;
                                peers_changed = true;
                            }

                            continue 'outer;
                        }
                    } else if !remove_seeds {
                        remove_seeds = true;
                    }
                }

                // Add new peer to the list.
                info!(
                    "Discovered new peer {} (shard {}) listening at {}.",
                    peer.peer_id, peer.shard_id, peer.addr
                );
                let peer_id = peer.peer_id;
                let is_follower = is_leading && peer.shard_id == self.shard_id;
                self.peers.push(Peer::new(self, peer, State::Alive));
                if is_follower {
                    self.add_follower(peer_id);
                }
                if !peers_changed {
                    peers_changed = true;
                }
            } else if peer.epoch > self.epoch {
                debug!(
                    "This node was already part of the cluster, updating local epoch to {}",
                    peer.epoch
                );
                self.epoch = peer.epoch + 1;
            }
        }

        if remove_seeds {
            self.peers.retain(|peer| !peer.is_seed());
        }

        // Update store
        if peers_changed {
            self.core.queue_set_key(
                "peer_list",
                PeerList::from(
                    self.peers
                        .iter()
                        .map(|p| p.into())
                        .collect::<Vec<PeerInfo>>(),
                ),
            );
        }
    }

    pub fn build_peer_status(&self) -> Vec<PeerStatus> {
        let mut result: Vec<PeerStatus> = Vec::with_capacity(self.peers.len() + 1);
        result.push(self.into());
        for peer in self.peers.iter() {
            result.push(peer.into());
        }
        result
    }

    pub fn build_peer_info(&self) -> Vec<PeerInfo> {
        let mut result: Vec<PeerInfo> = Vec::with_capacity(self.peers.len() + 1);
        result.push(self.into());
        for peer in self.peers.iter() {
            if !peer.is_seed() {
                result.push(peer.into());
            }
        }
        result
    }

    pub async fn handle_update_peers(
        &mut self,
        response_tx: oneshot::Sender<rpc::Response>,
        peers: Vec<PeerInfo>,
    ) {
        self.sync_peer_info(peers).await;
        response_tx
            .send(rpc::Response::UpdatePeers {
                peers: self.build_peer_info(),
            })
            .unwrap_or_else(|_| error!("Oneshot response channel closed."));
    }
}
