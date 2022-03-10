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
use tokio::{sync::mpsc, time};

use crate::{cluster::IPC_CHANNEL_BUFFER, JMAPServer, DEFAULT_RPC_PORT};

use super::{
    gossip::{self, spawn_quidnunc, PING_INTERVAL},
    rpc::{self, spawn_rpc},
    Cluster, Event,
};

pub async fn start_cluster<T>(core: web::Data<JMAPServer<T>>, settings: &EnvSettings) -> Option<()>
where
    T: for<'x> Store<'x> + 'static,
{
    let (tx, mut rx) = mpsc::channel::<Event>(IPC_CHANNEL_BUFFER);
    let (gossip_tx, gossip_rx) = mpsc::channel::<(SocketAddr, gossip::Request)>(IPC_CHANNEL_BUFFER);

    let mut cluster = Cluster::init(settings, core.clone(), tx.clone(), gossip_tx).await?;

    let bind_addr = SocketAddr::from((
        settings.parse_ipaddr("bind-addr", "127.0.0.1"),
        settings.parse("rpc-port").unwrap_or(DEFAULT_RPC_PORT),
    ));
    info!("Starting RPC server at {} (UDP/TCP)...", bind_addr);

    spawn_rpc(bind_addr, tx.clone(), cluster.key.clone()).await;
    spawn_quidnunc(bind_addr, gossip_rx, tx).await;

    tokio::spawn(async move {
        let mut wait_timeout = Duration::from_millis(PING_INTERVAL);
        let mut last_ping = Instant::now();

        loop {
            match time::timeout(wait_timeout, rx.recv()).await {
                Ok(Some(message)) => cluster.handle_message(message).await,
                Ok(None) => {
                    debug!("Cluster thread exiting.");
                    break;
                }
                Err(_) => (),
            }

            if !cluster.peers.is_empty() {
                let time_since_last_ping = last_ping.elapsed().as_millis() as u64;
                let time_to_next_ping = if time_since_last_ping >= PING_INTERVAL {
                    cluster.ping_peers().await;
                    last_ping = Instant::now();
                    PING_INTERVAL
                } else {
                    PING_INTERVAL - time_since_last_ping
                };

                wait_timeout = Duration::from_millis(
                    if let Some(time_to_next_election) = cluster.time_to_next_election() {
                        if time_to_next_election == 0 {
                            cluster.start_election(false).await;
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

    None
}

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn handle_message(&mut self, message: Event) {
        match message {
            Event::Gossip { request, addr } => match request {
                // Join request, add node and perform full sync.
                gossip::Request::Join { id, port } => self.handle_join(id, addr, port).await,

                // Join reply.
                gossip::Request::JoinReply { id } => self.handle_join_reply(id).await,

                // Hearbeat request, reply with the cluster status.
                gossip::Request::Ping(peer_list) => self.handle_ping(peer_list, true).await,

                // Heartbeat response, update the cluster status if needed.
                gossip::Request::Pong(peer_list) => self.handle_ping(peer_list, false).await,
            },
            Event::RpcRequest {
                peer_id,
                request,
                response_tx,
            } => {
                //debug!("Req [{}]: {:?}", peer_id, request);

                response_tx
                    .send(match request {
                        rpc::Request::Synchronize(peers) => {
                            self.sync_peer_info(peers).await;
                            rpc::Response::Synchronize(self.build_peer_info())
                        }
                        rpc::Request::Vote { term, last } => {
                            self.handle_vote_request(peer_id, term, last)
                        }
                        rpc::Request::MatchLog { term, last } => {
                            self.handle_match_log_request(peer_id, term, last).await
                        }
                        _ => rpc::Response::None,
                    })
                    .ok()
                    .unwrap_or_else(|| error!("Oneshot response channel closed."));
            }
            Event::RpcResponse { peer_id, response } => {
                //debug!("Reply [{}]: {:?}", peer_id, response);

                match response {
                    rpc::Response::Synchronize(peers) => {
                        self.sync_peer_info(peers).await;
                    }
                    rpc::Response::Vote { term, vote_granted } => {
                        self.handle_vote_response(peer_id, term, vote_granted).await;
                    }
                    _ => (),
                }
            }
            Event::StepDown { term } => {
                if term > self.term {
                    self.step_down(term);
                } else {
                    self.start_election_timer(false);
                }
            }
            Event::StoreChanged => {
                let last_log_index = self.core.last_log_index();
                if last_log_index > self.last_log_index {
                    self.last_log_index = last_log_index;
                    self.send_append_entries();
                }
            }
        }
    }

    pub async fn ping_peers(&mut self) {
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
            self.start_election(true).await;
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
    }
}
