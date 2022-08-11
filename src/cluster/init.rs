use crate::{
    cluster::{
        gossip::{spawn::spawn_quidnunc, PING_INTERVAL},
        rpc::listener::spawn_rpc,
        Cluster, Peer, PeerId, PeerList,
    },
    JMAPServer, DEFAULT_RPC_PORT,
};
use actix_web::web;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    net::{SocketAddr, ToSocketAddrs},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use store::{
    config::env_settings::EnvSettings,
    log::raft::{LogIndex, RaftId},
    tracing::{error, info},
};
use store::{tracing::debug, Store};
use tokio::sync::{mpsc, watch};

use super::{ClusterIpc, Event, IPC_CHANNEL_BUFFER, RAFT_LOG_BEHIND};

pub struct ClusterInit {
    main_rx: mpsc::Receiver<Event>,
    main_tx: mpsc::Sender<Event>,
    commit_index_tx: watch::Sender<LogIndex>,
}

pub fn init_cluster(settings: &EnvSettings) -> Option<(ClusterIpc, ClusterInit)> {
    if settings.get("cluster").is_some() {
        let (main_tx, main_rx) = mpsc::channel::<Event>(IPC_CHANNEL_BUFFER);
        let (commit_index_tx, commit_index_rx) = watch::channel(LogIndex::MAX);
        (
            ClusterIpc {
                tx: main_tx.clone(),
                state: RAFT_LOG_BEHIND.into(),
                commit_index_rx,
                leader_hostname: None.into(),
            },
            ClusterInit {
                main_rx,
                main_tx,
                commit_index_tx,
            },
        )
            .into()
    } else {
        None
    }
}

pub async fn start_cluster<T>(
    init: ClusterInit,
    core: web::Data<JMAPServer<T>>,
    settings: &EnvSettings,
) where
    T: for<'x> Store<'x> + 'static,
{
    let (gossip_tx, gossip_rx) =
        mpsc::channel::<(SocketAddr, crate::cluster::gossip::request::Request)>(IPC_CHANNEL_BUFFER);
    let main_tx = init.main_tx;
    let mut main_rx = init.main_rx;
    let commit_index_tx = init.commit_index_tx;

    let mut cluster = Cluster::init(
        settings,
        core.clone(),
        main_tx.clone(),
        gossip_tx,
        commit_index_tx,
    )
    .await;

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
            match tokio::time::timeout(wait_timeout, main_rx.recv()).await {
                Ok(Some(message)) => {
                    #[cfg(test)]
                    if let Event::SetOffline {
                        is_offline: set_offline,
                        notify_peers,
                    } = &message
                    {
                        if *set_offline {
                            debug!("[{}] Marked as offline.", cluster.addr);
                            if *notify_peers {
                                cluster.broadcast_leave().await;
                                for peer in &mut cluster.peers {
                                    peer.state = crate::cluster::gossip::State::Offline;
                                }
                            }
                        } else {
                            debug!("[{}] Marked as online.", cluster.addr);
                            if *notify_peers {
                                cluster.broadcast_ping().await;
                                last_ping = Instant::now();
                            } else {
                                last_ping =
                                    Instant::now() - Duration::from_millis(PING_INTERVAL + 50);
                                for peer in &mut cluster.peers {
                                    peer.state = crate::cluster::gossip::State::Suspected;
                                }
                            }
                        }
                        is_offline = *set_offline;

                        cluster.start_election_timer(!is_offline).await;
                    }
                    #[cfg(test)]
                    if is_offline {
                        continue;
                    }

                    match cluster.handle_message(message).await {
                        Ok(true) => (),
                        Ok(false) => {
                            debug!("Broadcasting leave request to peers and shutting down.");
                            cluster.broadcast_leave().await;
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
                    #[cfg(test)]
                    if time_since_last_ping > (PING_INTERVAL + 200) {
                        error!(
                            "[{}] Possible event loop block: {}ms since last ping.",
                            cluster.addr, time_since_last_ping
                        );
                    }

                    if let Err(err) = cluster.ping_peers().await {
                        debug!("Failed to ping peers: {:?}", err);
                        break;
                    }
                    last_ping = Instant::now();
                    PING_INTERVAL
                } else {
                    PING_INTERVAL - time_since_last_ping
                };

                let time = Instant::now();
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

                if time.elapsed().as_millis() > 50 {
                    panic!(
                        "{}ms [{}] Request votes took too long!",
                        time.elapsed().as_millis(),
                        cluster.addr,
                    );
                }
            }
        }
    });
}

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    async fn init(
        settings: &EnvSettings,
        core: web::Data<JMAPServer<T>>,
        tx: mpsc::Sender<Event>,
        gossip_tx: mpsc::Sender<(SocketAddr, crate::cluster::gossip::request::Request)>,
        commit_index_tx: watch::Sender<LogIndex>,
    ) -> Self {
        let key = settings.get("cluster").unwrap();

        // Obtain public addresses to advertise
        let advertise_addr = settings.parse_ipaddr("advertise-addr", "127.0.0.1");
        let rpc_port = settings.parse("rpc-port").unwrap_or(DEFAULT_RPC_PORT);

        // Obtain peer id from disk or generate a new one.
        let peer_id = if let Some(peer_id) = core.get_key("peer_id").await.unwrap() {
            peer_id
        } else {
            // Generate peerId for this node.
            let mut s = DefaultHasher::new();
            gethostname::gethostname().hash(&mut s);
            thread::current().id().hash(&mut s);
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|_| Duration::new(0, 0))
                .as_nanos()
                .hash(&mut s);

            let peer_id = s.finish();
            core.set_key("peer_id", peer_id).await.unwrap();
            peer_id
        };

        // Obtain shard id from disk or generate a new one.
        let shard_id = if let Some(shard_id) = core.get_key("shard_id").await.unwrap() {
            shard_id
        } else {
            let shard_id = settings.parse("shard-id").unwrap_or(0);
            core.set_key("shard_id", shard_id).await.unwrap();
            shard_id
        };
        info!(
            "This node will join shard '{}' with id '{}'.",
            shard_id, peer_id
        );

        // Create advertise addresses
        let addr = SocketAddr::from((advertise_addr, rpc_port));

        // Calculate generationId
        let hostname = format!(
            "{}://{}",
            if settings.contains_key("cert-path") {
                "https"
            } else {
                "http"
            },
            settings.get("hostname").unwrap()
        );
        let mut generation = DefaultHasher::new();
        peer_id.hash(&mut generation);
        shard_id.hash(&mut generation);
        addr.hash(&mut generation);
        hostname.hash(&mut generation);

        // Rollback uncommitted entries for a previous leader term.
        core.commit_leader(LogIndex::MAX, true).await.unwrap();

        // Apply committed updates and rollback uncommited ones for
        // a previous follower term.
        core.commit_follower(LogIndex::MAX, true).await.unwrap();

        let last_log = core
            .get_last_log()
            .await
            .unwrap()
            .unwrap_or_else(RaftId::none);
        let mut cluster = Cluster {
            peer_id,
            shard_id,
            generation: generation.finish(),
            epoch: 0,
            addr,
            key,
            hostname,
            term: last_log.term,
            uncommitted_index: last_log.index,
            last_log,
            state: crate::cluster::raft::State::default(),
            core,
            peers: vec![],
            last_peer_pinged: u32::MAX as usize,
            tx,
            gossip_tx,
            commit_index_tx,
        };

        // Add previously discovered peers
        if let Some(peer_list) = cluster.core.get_key::<PeerList>("peer_list").await.unwrap() {
            for peer in peer_list.peers {
                cluster.peers.push(Peer::new(
                    &cluster,
                    peer,
                    crate::cluster::gossip::State::Offline,
                ));
            }
        };

        // Add any seed nodes
        if let Some(seed_nodes) = settings.parse_list("seed-nodes") {
            for (node_id, seed_node) in seed_nodes.into_iter().enumerate() {
                let peer_addr = if !seed_node.contains(':') {
                    format!("{}:{}", seed_node, rpc_port)
                } else {
                    seed_node.to_string()
                }
                .to_socket_addrs()
                .map_err(|e| {
                    error!("Failed to parse seed node '{}': {}", seed_node, e);
                    std::process::exit(1);
                })
                .unwrap()
                .next()
                .unwrap_or_else(|| {
                    error!("Failed to parse seed node '{}'.", seed_node);
                    std::process::exit(1);
                });

                if !cluster.peers.iter().any(|p| p.addr == peer_addr) {
                    info!("Adding seed node '{}'.", peer_addr);
                    cluster
                        .peers
                        .push(Peer::new_seed(&cluster, node_id as PeerId, peer_addr));
                }
            }
        }

        cluster
    }
}