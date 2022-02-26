use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use actix_web::web;
use store::Store;
use tokio::time;
use tracing::{debug, error};

use crate::JMAPServer;

use super::{
    gossip::{
        self, build_peer_status, check_heartbeat, handle_gossip, start_gossip, PING_INTERVAL,
    },
    raft::start_election,
    rpc::start_rpc_queue,
    Cluster, Message,
};

pub async fn start_cluster<T>(core: web::Data<JMAPServer<T>>, bind_addr: SocketAddr)
where
    T: for<'x> Store<'x> + 'static,
{
    let rpc_tx = start_rpc_queue(core.clone()).await;
    let (mut gossip_rx, gossip_tx) = start_gossip(bind_addr).await;

    tokio::spawn(async move {
        let mut wait_timeout = Duration::from_millis(PING_INTERVAL);
        let mut last_ping = Instant::now();
        let mut last_peer_pinged = u32::MAX as usize;
        let mut requests = Vec::<Message>::with_capacity(10);

        loop {
            match time::timeout(wait_timeout, gossip_rx.recv()).await {
                Ok(Some((source_addr, request))) => {
                    match handle_gossip(&core, source_addr, request).await {
                        Message::None => (),
                        response => requests.push(response),
                    }
                }
                Ok(None) => {
                    debug!("Gossip thread exiting.");
                    break;
                }
                Err(_) => (),
            }

            core.cluster
                .lock()
                .map(|mut cluster| {
                    if !cluster.peers.is_empty() {
                        let time_since_last_ping = last_ping.elapsed().as_millis() as u64;
                        let time_to_next_ping = if time_since_last_ping >= PING_INTERVAL {
                            last_peer_pinged =
                                ping_peers(&mut cluster, &mut requests, last_peer_pinged);
                            last_ping = Instant::now();
                            PING_INTERVAL
                        } else {
                            PING_INTERVAL - time_since_last_ping
                        };

                        wait_timeout = Duration::from_millis(
                            cluster
                                .time_to_next_election()
                                .map(|time_to_next_election| {
                                    if time_to_next_election == 0 {
                                        start_election(&mut cluster, &mut requests);
                                        time_to_next_ping
                                    } else if time_to_next_election < time_to_next_ping {
                                        time_to_next_election
                                    } else {
                                        time_to_next_ping
                                    }
                                })
                                .unwrap_or(time_to_next_ping),
                        );
                    }
                })
                .unwrap_or_else(|_| {
                    error!("Failed to acquire cluster write lock.");
                });

            // Dispatch messages to the gossip and RPC processes.
            if !requests.is_empty() {
                for request in requests.drain(..) {
                    match &request {
                        Message::VoteRequest { .. }
                        | Message::SyncResponse { .. }
                        | Message::JoinRaftRequest { .. } => {
                            rpc_tx.send(request).await.ok();
                        }
                        Message::Ping { .. }
                        | Message::Pong { .. }
                        | Message::Join { .. }
                        | Message::SyncRequest { .. } => {
                            gossip_tx.send(request).await.ok();
                        }
                        Message::None => unreachable!(),
                    }
                }
            }
        }
    });
}

fn ping_peers(
    cluster: &mut Cluster,
    requests: &mut Vec<Message>,
    mut last_peer_pinged: usize,
) -> usize {
    // Total and alive peers in the cluster.
    let total_peers = cluster.peers.len();
    let mut alive_peers: u32 = 0;

    // Start a new election on startup or on an election timeout.
    let mut leader_is_offline = false;
    let leader_peer_id = cluster.leader_peer_id();

    // Count alive peers and start a new election if the current leader becomes offline.
    for peer in cluster.peers.iter_mut() {
        if !peer.is_offline() {
            // Failure detection
            if check_heartbeat(peer) {
                alive_peers += 1;
            } else if !leader_is_offline
                && leader_peer_id.map(|id| id == peer.peer_id).unwrap_or(false)
            {
                // Current leader is offline, start election
                leader_is_offline = true;
            }
        }
    }

    // Start a new election
    if leader_is_offline {
        start_election(cluster, requests);
    }

    // Find next peer to ping
    for _ in 0..total_peers {
        last_peer_pinged = (last_peer_pinged + 1) % total_peers;
        let (peer_state, target_addr) = {
            let peer = &cluster.peers[last_peer_pinged];
            (peer.state, peer.gossip_addr)
        };

        match peer_state {
            gossip::State::Seed => {
                requests.push(Message::Join {
                    addr: target_addr,
                    port: cluster.gossip_addr.port(),
                });
                break;
            }
            gossip::State::Alive | gossip::State::Suspected => {
                cluster.epoch += 1;
                requests.push(Message::Ping {
                    addr: target_addr,
                    peers: build_peer_status(cluster),
                });
                break;
            }
            gossip::State::Offline if alive_peers == 0 => {
                // Probe offline nodes
                cluster.epoch += 1;
                requests.push(Message::Ping {
                    addr: target_addr,
                    peers: build_peer_status(cluster),
                });
                break;
            }
            _ => (),
        }
    }

    last_peer_pinged
}
