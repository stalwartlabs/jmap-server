use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use actix_web::web;
use store::Store;
use store::{
    config::EnvSettings,
    tracing::{debug, error, info},
};
use tokio::{
    sync::{mpsc, watch},
    time,
};

use crate::{cluster::IPC_CHANNEL_BUFFER, JMAPServer, DEFAULT_RPC_PORT};

use super::{
    gossip::{self, spawn_quidnunc, PING_INTERVAL},
    rpc::{self, spawn_rpc},
    Cluster, Event,
};

pub async fn start_cluster<T>(
    core: web::Data<JMAPServer<T>>,
    settings: &EnvSettings,
    mut main_rx: mpsc::Receiver<Event>,
    main_tx: mpsc::Sender<Event>,
) where
    T: for<'x> Store<'x> + 'static,
{
    let (gossip_tx, gossip_rx) = mpsc::channel::<(SocketAddr, gossip::Request)>(IPC_CHANNEL_BUFFER);

    let mut cluster = Cluster::init(settings, core.clone(), main_tx.clone(), gossip_tx).await;

    let bind_addr = SocketAddr::from((
        settings.parse_ipaddr("bind-addr", "127.0.0.1"),
        settings.parse("rpc-port").unwrap_or(DEFAULT_RPC_PORT),
    ));
    info!("Starting RPC server at {} (UDP/TCP)...", bind_addr);
    let (shutdown_tx, shutdown_rx) = watch::channel(true);

    spawn_rpc(
        bind_addr,
        shutdown_rx.clone(),
        main_tx.clone(),
        cluster.key.clone(),
    )
    .await;
    spawn_quidnunc(bind_addr, shutdown_rx.clone(), gossip_rx, main_tx.clone()).await;

    tokio::spawn(async move {
        let mut wait_timeout = Duration::from_millis(PING_INTERVAL);
        let mut last_ping = Instant::now();

        #[cfg(test)]
        let mut is_offline = false;

        loop {
            match time::timeout(wait_timeout, main_rx.recv()).await {
                Ok(Some(message)) => {
                    #[cfg(test)]
                    if let Event::IsOffline(status) = &message {
                        if *status {
                            debug!("[{}] Marked as offline.", cluster.addr);
                        } else {
                            debug!("[{}] Marked as online.", cluster.addr);
                        }
                        is_offline = *status;
                        for peer in &mut cluster.peers {
                            peer.state = if is_offline {
                                gossip::State::Offline
                            } else {
                                gossip::State::Suspected
                            };
                        }

                        cluster.start_election_timer(!is_offline);
                    }
                    #[cfg(test)]
                    if is_offline {
                        continue;
                    }

                    match cluster.handle_message(message).await {
                        Ok(true) => (),
                        Ok(false) => {
                            debug!("Cluster shutting down.");
                            shutdown_tx.send(false).ok();
                            break;
                        }
                        Err(err) => {
                            error!("Cluster process exiting due to error: {:?}", err);
                            shutdown_tx.send(false).ok();
                            break;
                        }
                    }
                }
                Ok(None) => {
                    debug!("Cluster main process exiting.");
                    break;
                }
                Err(_) =>
                {
                    #[cfg(test)]
                    if is_offline {
                        continue;
                    }
                }
            }

            if !cluster.peers.is_empty() {
                let time_since_last_ping = last_ping.elapsed().as_millis() as u64;
                let time_to_next_ping = if time_since_last_ping >= PING_INTERVAL {
                    if let Err(err) = cluster.ping_peers().await {
                        debug!("Failed to ping peers: {:?}", err);
                        break;
                    }
                    last_ping = Instant::now();
                    PING_INTERVAL
                } else {
                    PING_INTERVAL - time_since_last_ping
                };

                wait_timeout = Duration::from_millis(
                    if let Some(time_to_next_election) = cluster.time_to_next_election() {
                        if time_to_next_election == 0 {
                            if let Err(err) = cluster.request_votes(false).await {
                                debug!("Failed to request votes: {:?}", err);
                                break;
                            }
                            time_to_next_ping
                        } else if time_to_next_election < time_to_next_ping {
                            time_to_next_election
                        } else {
                            time_to_next_ping
                        }
                    } else {
                        time_to_next_ping
                    },
                );
            }
        }
    });
}

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn handle_message(&mut self, message: Event) -> store::Result<bool> {
        match message {
            Event::Gossip { request, addr } => {
                //println!("From {}: {:?}", addr, request);

                match request {
                    // Join request, add node and perform full sync.
                    gossip::Request::Join { id, port } => self.handle_join(id, addr, port).await,

                    // Join reply.
                    gossip::Request::JoinReply { id } => self.handle_join_reply(id).await,

                    // Hearbeat request, reply with the cluster status.
                    gossip::Request::Ping(peer_list) => self.handle_ping(peer_list, true).await,

                    // Heartbeat response, update the cluster status if needed.
                    gossip::Request::Pong(peer_list) => self.handle_ping(peer_list, false).await,
                }
            }
            Event::RpcRequest {
                peer_id,
                request,
                response_tx,
            } => match request {
                rpc::Request::UpdatePeers { peers } => {
                    self.handle_update_peers(response_tx, peers).await;
                }
                rpc::Request::Vote { term, last } => {
                    self.handle_vote_request(peer_id, response_tx, term, last);
                }
                rpc::Request::BecomeFollower { term, last_log } => {
                    self.handle_become_follower(peer_id, response_tx, term, last_log)
                        .await?;
                }
                rpc::Request::AppendEntries { term, request } => {
                    self.handle_append_entries(peer_id, response_tx, term, request)
                        .await;
                }
                rpc::Request::Ping => response_tx
                    .send(rpc::Response::Pong)
                    .unwrap_or_else(|_| error!("Oneshot response channel closed.")),
                _ => response_tx
                    .send(rpc::Response::None)
                    .unwrap_or_else(|_| error!("Oneshot response channel closed.")),
            },
            Event::RpcResponse { peer_id, response } => match response {
                rpc::Response::UpdatePeers { peers } => {
                    self.sync_peer_info(peers).await;
                }
                rpc::Response::Vote { term, vote_granted } => {
                    self.handle_vote_response(peer_id, term, vote_granted)
                        .await?;
                }
                rpc::Response::UnregisteredPeer => {
                    self.get_peer(peer_id)
                        .unwrap()
                        .dispatch_request(rpc::Request::UpdatePeers {
                            peers: self.build_peer_info(),
                        })
                        .await;
                }
                _ => (),
            },
            Event::StepDown { term } => {
                //TODO use separate thread

                self.core.reset_uncommitted_changes().await?;

                if term > self.term {
                    self.step_down(term);
                } else {
                    self.start_election_timer(false);
                }
            }
            Event::StoreChanged { last_log } => {
                // When leading, last_log contains the last uncommited index.
                // When following, last_log contains the last committed index.
                if self.is_leading() {
                    self.last_log.term = last_log.term;
                    self.uncommitted_index = last_log.index;
                    self.send_append_entries();
                } else {
                    self.last_log = last_log;
                }
            }
            Event::AdvanceCommitIndex {
                peer_id,
                commit_index,
            } => {
                self.advance_commit_index(peer_id, commit_index).await?;
            }
            Event::Shutdown => return Ok(false),

            #[cfg(test)]
            Event::IsOffline(_) => (),
        }
        Ok(true)
    }

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
                gossip::State::Seed => {
                    self.send_gossip(
                        target_addr,
                        gossip::Request::Join {
                            id: self.last_peer_pinged,
                            port: self.addr.port(),
                        },
                    )
                    .await;
                    break;
                }
                gossip::State::Alive | gossip::State::Suspected => {
                    self.epoch += 1;
                    self.send_gossip(target_addr, gossip::Request::Ping(self.build_peer_status()))
                        .await;
                    break;
                }
                gossip::State::Offline if alive_peers == 0 => {
                    // Probe offline nodes
                    self.send_gossip(target_addr, gossip::Request::Ping(self.build_peer_status()))
                        .await;
                    break;
                }
                _ => (),
            }
        }

        Ok(())
    }
}
