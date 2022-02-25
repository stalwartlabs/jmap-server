use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use actix_web::web;
use store::Store;
use tokio::time;
use tracing::{debug, error, info};

use crate::JMAPServer;

use super::{
    gossip::{
        self, build_peer_info, build_peer_status, check_heartbeat, handle_ping, start_gossip,
        PING_INTERVAL,
    },
    raft::{self, ELECTION_TIMEOUT},
    rpc::start_rpc_queue,
    Message,
};

pub async fn start_cluster<T>(core: web::Data<JMAPServer<T>>, bind_addr: SocketAddr)
where
    T: for<'x> Store<'x> + 'static,
{
    let rpc_tx = start_rpc_queue(core.clone()).await;
    let (mut gossip_rx, gossip_tx) = start_gossip(bind_addr).await;

    tokio::spawn(async move {
        let mut last_ping = Instant::now();
        let mut last_peer_pinged = u32::MAX as usize;
        let mut requests = Vec::<Message>::with_capacity(10);

        loop {
            match time::timeout(Duration::from_millis(PING_INTERVAL), gossip_rx.recv()).await {
                Ok(Some((source_addr, request))) => {
                    let response = match request {
                        // Join request, reply with this node's RPC url.
                        gossip::Request::Join(reply_port) => core
                            .cluster
                            .lock()
                            .map(|cluster| Message::SyncRequest {
                                addr: SocketAddr::from((source_addr.ip(), reply_port)),
                                url: cluster.rpc_url.clone(),
                            })
                            .unwrap_or(Message::None),

                        // Synchronize request, perform a full sync over HTTP.
                        gossip::Request::Synchronize(rpc_url) => core
                            .cluster
                            .lock()
                            .map(|cluster| Message::SyncResponse {
                                url: rpc_url,
                                peers: build_peer_info(&cluster),
                            })
                            .unwrap_or(Message::None),

                        // Hearbeat request, reply with the cluster status.
                        gossip::Request::Ping(peer_list) => {
                            handle_ping(&core, peer_list, true).await
                        }

                        // Heartbeat response, update the cluster status if needed.
                        gossip::Request::Pong(peer_list) => {
                            handle_ping(&core, peer_list, false).await
                        }
                    };

                    if !matches!(response, Message::None) {
                        requests.push(response);
                    }
                }
                Ok(None) => {
                    debug!("Gossip thread exiting.");
                    break;
                }
                Err(_) => (),
            }

            if last_ping.elapsed().as_millis() as u64 >= PING_INTERVAL {
                if let Ok(mut cluster) = core.cluster.lock() {
                    if cluster.peers.is_empty() {
                        continue;
                    }
                    let raft_state = cluster.state;
                    let shard_id = cluster.shard_id;

                    // Total and alive peers in the cluster.
                    let total_peers = cluster.peers.len();
                    let mut alive_peers: u32 = 0;

                    // Total and alive peers in this shard.
                    let mut alive_peers_shard: u32 = 0;
                    let mut total_peers_shard: u32 = 0;

                    // Start a new election on startup or on an election timeout.
                    let mut start_election = match &raft_state {
                        raft::State::None => true,
                        raft::State::Candidate(election_start)
                        | raft::State::Wait(election_start)
                        | raft::State::VotedFor((_, election_start))
                            if election_start.elapsed().as_millis() >= ELECTION_TIMEOUT =>
                        {
                            true
                        }
                        _ => false,
                    };

                    // Count alive peers and start a new election if the current leader becomes offline.
                    for peer in cluster.peers.iter_mut() {
                        if !matches!(peer.state, gossip::State::Offline) {
                            // Failure detection
                            if check_heartbeat(peer) {
                                if peer.shard_id == shard_id {
                                    alive_peers_shard += 1;
                                }
                                alive_peers += 1;
                            } else if !start_election
                                && matches!(raft_state, raft::State::Follower(peer_id) if peer_id == peer.peer_id)
                            {
                                // Current leader is offline, start election
                                start_election = true;
                            }
                        }
                        if peer.shard_id == shard_id {
                            total_peers_shard += 1;
                        }
                    }

                    // Start a new election
                    if start_election {
                        // Check if there is enough quorum for an election.
                        let needed_peers = ((total_peers_shard as f64 + 1.0) / 2.0).floor() as u32;
                        if alive_peers_shard >= needed_peers {
                            // Assess whether this node could become the leader for the next term.
                            let mut is_up_to_date = true;
                            let mut urls = Vec::with_capacity(cluster.peers.len());

                            for peer in cluster.peers.iter() {
                                if peer.shard_id == shard_id && peer.state != gossip::State::Offline
                                {
                                    if is_up_to_date
                                        && ((peer.last_log_term > cluster.last_log_term)
                                            || (peer.last_log_term == cluster.last_log_term
                                                && peer.last_log_index > cluster.last_log_index))
                                    {
                                        is_up_to_date = false;
                                    }
                                    urls.push(peer.rpc_url.clone());
                                }
                            }

                            if is_up_to_date {
                                // Increase term and start election
                                cluster.state = raft::State::Candidate(Instant::now());
                                cluster.term += 1;

                                requests.push(Message::VoteRequest {
                                    urls,
                                    term: cluster.term,
                                    last_log_index: cluster.last_log_index,
                                    last_log_term: cluster.last_log_term,
                                });
                            } else {
                                // Query who is the current leader while at the same time wait to
                                // receive a vote request from a more up-to-date peer.
                                cluster.state = raft::State::Wait(Instant::now());
                                requests.push(Message::QueryLeader { urls });
                            }
                        } else {
                            info!(
                                    "Not enough peers in shard {} to start election: {} alive ouf of {} total, {} alive needed.",
                                    cluster.shard_id, alive_peers_shard, total_peers_shard, needed_peers
                                );
                        }
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
                                    peers: build_peer_status(&cluster),
                                });
                                break;
                            }
                            gossip::State::Offline if alive_peers == 0 => {
                                // Probe offline nodes
                                cluster.epoch += 1;
                                requests.push(Message::Ping {
                                    addr: target_addr,
                                    peers: build_peer_status(&cluster),
                                });
                                break;
                            }
                            _ => (),
                        }
                    }
                } else {
                    error!("Failed to acquire cluster write lock.");
                }

                // Dispatch messages to the gossip and RPC processes.
                for request in requests.drain(..) {
                    match &request {
                        Message::VoteRequest { .. }
                        | Message::SyncResponse { .. }
                        | Message::QueryLeader { .. } => {
                            rpc_tx.send(request).await.ok();
                        }
                        Message::Ping { .. }
                        | Message::Pong { .. }
                        | Message::Join { .. }
                        | Message::SyncRequest { .. } => {
                            gossip_tx.send(request).await.ok();
                        }
                        _ => unreachable!(),
                    }
                }

                last_ping = Instant::now();
            }
        }
    });
}
