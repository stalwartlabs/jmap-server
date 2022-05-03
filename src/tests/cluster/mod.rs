use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use actix_web::web;
use futures::future::join_all;

use store::{
    config::env_settings::EnvSettings, log::raft::RaftId, parking_lot::Mutex, AccountId, JMAPId,
    JMAPStore, Store,
};

use store_rocksdb::RocksDB;
use store_test::{
    destroy_temp_dir, init_settings,
    jmap_mail_set::{delete_email, insert_email, update_email},
    jmap_mailbox::{delete_mailbox, insert_mailbox, update_mailbox},
    StoreCompareWith,
};
use tokio::{sync::mpsc, time::sleep};

use crate::{
    cluster::{self, main::start_cluster},
    JMAPServer,
};
use crate::{jmap::server::init_jmap_server, tests::cluster::fuzz::cluster_fuzz};

use self::{
    crud::crud_ops, election::raft_election, log_conflict::resolve_log_conflict,
    mail_thread_merge::merge_mail_threads,
};

pub mod crud;
pub mod election;
pub mod fuzz;
pub mod log_conflict;
pub mod mail_thread_merge;

struct Peer<T> {
    tx: mpsc::Sender<cluster::Event>,
    rx: mpsc::Receiver<cluster::Event>,
    temp_dir: PathBuf,
    jmap_server: web::Data<JMAPServer<T>>,
    settings: EnvSettings,
}

struct Cluster<T> {
    peers: Vec<Peer<T>>,
    temp_dirs: Vec<PathBuf>,
    num_peers: u32,
    delete_if_exists: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Ac {
    NewEmail((JMAPId, JMAPId)),
    UpdateEmail(JMAPId),
    DeleteEmail(JMAPId),
    InsertMailbox(JMAPId),
    UpdateMailbox(JMAPId),
    DeleteMailbox(JMAPId),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Cmd {
    StopLeader,
    StopFollower,
    StartOneOffline,
    StartAllOffline,
    Update { account_id: AccountId, action: Ac },
}

#[derive(Debug, Default)]
struct Cmds {
    cmds: Vec<Cmd>,
    clean_exit: bool,
}

impl Drop for Cmds {
    fn drop(&mut self) {
        if !self.clean_exit && !self.cmds.is_empty() {
            println!(
                "Executed commands: {}",
                serde_json::to_string(&self.cmds).unwrap()
            );
        }
    }
}

impl<T> Peer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn new(peer_num: u32, num_peers: u32, delete_if_exists: bool) -> Self {
        let (settings, temp_dir) =
            init_settings("st_cluster", peer_num, num_peers, delete_if_exists);

        let (jmap_server, cluster) = init_jmap_server(&settings);
        let (tx, rx) = cluster.unwrap();

        Peer {
            tx,
            rx,
            settings,
            temp_dir,
            jmap_server,
        }
    }

    pub async fn start_cluster(self) -> (web::Data<JMAPServer<T>>, PathBuf) {
        start_cluster(self.jmap_server.clone(), &self.settings, self.rx, self.tx).await;
        (self.jmap_server, self.temp_dir)
    }
}

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn new(num_peers: u32, delete_if_exists: bool) -> Self {
        Cluster {
            peers: (1..=num_peers)
                .into_iter()
                .map(|peer_num| Peer::new(peer_num, num_peers, delete_if_exists))
                .collect(),
            temp_dirs: vec![],
            num_peers,
            delete_if_exists,
        }
    }

    pub async fn start_cluster(&mut self) -> Arc<Vec<web::Data<JMAPServer<T>>>> {
        let mut peers = Vec::new();

        for peer in self.peers.drain(..) {
            let (server, temp_dir) = peer.start_cluster().await;
            self.temp_dirs.push(temp_dir);
            peers.push(server);
        }

        Arc::new(peers)
    }

    pub async fn extend_cluster(
        &mut self,
        peers: Arc<Vec<web::Data<JMAPServer<T>>>>,
        n: u32,
    ) -> Arc<Vec<web::Data<JMAPServer<T>>>> {
        let mut peers = match Arc::try_unwrap(peers) {
            Ok(peers) => peers,
            Err(_) => panic!("Unable to unwrap peers"),
        };

        for peer_num in 1..=n {
            let (server, temp_dir) = Peer::new(
                peer_num + self.num_peers,
                self.num_peers,
                self.delete_if_exists,
            )
            .start_cluster()
            .await;
            self.temp_dirs.push(temp_dir);
            peers.push(server);
        }

        self.num_peers += n;

        Arc::new(peers)
    }

    pub fn cleanup(self) {
        for temp_dir in self.temp_dirs {
            destroy_temp_dir(temp_dir);
        }
    }
}

impl Ac {
    pub fn execute<T>(
        &self,
        store: &Arc<JMAPStore<T>>,
        mailbox_map: &Arc<Mutex<HashMap<JMAPId, JMAPId>>>,
        email_map: &Arc<Mutex<HashMap<JMAPId, JMAPId>>>,
        batch_num: usize,
    ) where
        T: for<'x> Store<'x> + 'static,
    {
        match self {
            Ac::NewEmail((local_id, mailbox_id)) => {
                email_map.lock().insert(
                    *local_id,
                    insert_email(
                        store,
                        2,
                        format!(
                            "From: test@test.com\nSubject: test {}\n\nTest message {}",
                            local_id, local_id
                        )
                        .into_bytes(),
                        vec![*mailbox_map.lock().get(mailbox_id).unwrap()],
                        vec![],
                        None,
                    ),
                );
            }
            Ac::UpdateEmail(local_id) => {
                update_email(
                    store,
                    2,
                    *email_map.lock().get(local_id).unwrap(),
                    None,
                    Some(vec![format!("tag_{}", batch_num)]),
                );
            }
            Ac::DeleteEmail(local_id) => {
                delete_email(store, 2, email_map.lock().remove(local_id).unwrap());
            }
            Ac::InsertMailbox(local_id) => {
                mailbox_map.lock().insert(
                    *local_id,
                    insert_mailbox(
                        store,
                        2,
                        &format!("Mailbox {}", local_id),
                        &format!("role_{}", local_id),
                    ),
                );
            }
            Ac::UpdateMailbox(local_id) => {
                update_mailbox(
                    store,
                    2,
                    *mailbox_map.lock().get(local_id).unwrap(),
                    *local_id as u32,
                    batch_num as u32,
                );
            }
            Ac::DeleteMailbox(local_id) => {
                delete_mailbox(store, 2, mailbox_map.lock().remove(local_id).unwrap());
            }
        }
    }
}

fn test_batch() -> Vec<Vec<Ac>> {
    vec![
        vec![
            Ac::InsertMailbox(1),
            Ac::NewEmail((1, 1)),
            Ac::UpdateEmail(1),
            Ac::DeleteEmail(1),
        ],
        vec![
            Ac::InsertMailbox(2),
            Ac::UpdateMailbox(2),
            Ac::DeleteMailbox(2),
        ],
        vec![
            Ac::NewEmail((1, 1)),
            Ac::NewEmail((2, 1)),
            Ac::NewEmail((3, 1)),
            Ac::InsertMailbox(3),
            Ac::InsertMailbox(4),
            Ac::InsertMailbox(5),
        ],
        vec![Ac::UpdateEmail(1), Ac::UpdateEmail(2), Ac::UpdateEmail(3)],
        vec![
            Ac::UpdateMailbox(3),
            Ac::UpdateMailbox(4),
            Ac::UpdateMailbox(5),
        ],
        vec![Ac::DeleteEmail(1)],
        vec![Ac::DeleteEmail(2)],
        vec![
            Ac::UpdateMailbox(3),
            Ac::DeleteMailbox(4),
            Ac::UpdateMailbox(5),
        ],
        vec![Ac::DeleteEmail(3)],
        vec![Ac::DeleteMailbox(3), Ac::DeleteMailbox(5)],
        vec![
            Ac::InsertMailbox(2),
            Ac::NewEmail((1, 2)),
            Ac::NewEmail((2, 2)),
            Ac::NewEmail((3, 2)),
        ],
        vec![Ac::UpdateEmail(1)],
        vec![Ac::DeleteEmail(2)],
        vec![Ac::DeleteMailbox(2)],
    ]
}

async fn assert_leader_elected<T>(peers: &[web::Data<JMAPServer<T>>]) -> &web::Data<JMAPServer<T>>
where
    T: for<'x> Store<'x> + 'static,
{
    for _ in 0..100 {
        for peer in peers.iter() {
            if peer.is_leader() {
                return peer;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("No leader elected.");
}

async fn assert_no_quorum<T>(peers: &[web::Data<JMAPServer<T>>])
where
    T: for<'x> Store<'x> + 'static,
{
    sleep(Duration::from_millis(1000)).await;
    'outer: for _ in 0..100 {
        for peer in peers.iter() {
            if peer.is_leader() {
                continue 'outer;
            }
            sleep(Duration::from_millis(500)).await;
        }
        return;
    }
    panic!("Leader still active, expected no quorum.");
}

fn num_online_peers<T>(peers: &[web::Data<JMAPServer<T>>]) -> usize
where
    T: for<'x> Store<'x> + 'static,
{
    let mut num_online = 0;
    for peer in peers.iter() {
        if !peer.is_offline() {
            num_online += 1;
        }
    }
    num_online
}

async fn activate_all_peers<T>(peers: &[web::Data<JMAPServer<T>>])
where
    T: for<'x> Store<'x> + 'static,
{
    for peer in peers.iter() {
        if peer.is_offline() {
            peer.set_offline(false, true).await;
        }
    }
}

async fn assert_cluster_updated<T>(peers: &[web::Data<JMAPServer<T>>])
where
    T: for<'x> Store<'x> + 'static,
{
    let mut updated = vec![false; peers.len()];

    'outer: for _ in 0..100 {
        let mut last_logs = vec![RaftId::none(); peers.len()];

        sleep(Duration::from_millis(100)).await;

        for (peer_num, peer) in peers.iter().enumerate() {
            if peer.is_offline() {
                updated[peer_num] = true;
            } else if peer.is_up_to_date() {
                updated[peer_num] = true;
                last_logs[peer_num] = peer
                    .get_last_log()
                    .await
                    .unwrap()
                    .unwrap_or_else(RaftId::none);
            } else {
                updated[peer_num] = false;
            }
        }

        if updated.iter().all(|u| *u) {
            let mut last_log = RaftId::none();
            for peer_last_log in &last_logs {
                if !peer_last_log.is_none() {
                    if last_log.is_none() {
                        last_log = *peer_last_log;
                    } else if last_log != *peer_last_log {
                        continue 'outer;
                    }
                }
            }
            return;
        }
    }

    for peer in peers.iter() {
        println!(
            "{}: {:?} ({})",
            if peer.is_offline() {
                "offline"
            } else if peer.is_leader() {
                "leader"
            } else {
                "follower"
            },
            peer.get_last_log().await,
            peer.is_up_to_date()
        );
    }
    panic!("Some nodes are not up to date.");
}

async fn assert_mirrored_stores<T>(peers: Arc<Vec<web::Data<JMAPServer<T>>>>, ignore_offline: bool)
where
    T: for<'x> Store<'x> + 'static,
{
    tokio::task::spawn_blocking(move || {
        for (leader_pos, leader) in peers.iter().enumerate() {
            if leader.is_leader() {
                for (follower_pos, follower) in peers.iter().enumerate() {
                    if follower_pos != leader_pos {
                        if follower.is_offline() {
                            if ignore_offline {
                                continue;
                            } else {
                                panic!("Follower {} is offline.", follower_pos);
                            }
                        }
                        assert!(follower.is_up_to_date());
                        let keys_leader = leader.store.compare_with(&follower.store);
                        let keys_follower = follower.store.compare_with(&leader.store);
                        assert!(
                            keys_leader.iter().map(|(_, v)| *v).sum::<usize>() > 0,
                            "{:?}",
                            keys_leader
                        );
                        assert_eq!(keys_leader, keys_follower);
                    }
                }
                return;
            }
        }
        panic!("Leader not elected.");
    })
    .await
    .unwrap();
}

async fn shutdown_all<T>(peers: Arc<Vec<web::Data<JMAPServer<T>>>>)
where
    T: for<'x> Store<'x> + 'static,
{
    for peer in peers.iter() {
        peer.shutdown().await;
    }
    drop(peers);
    sleep(Duration::from_millis(1000)).await;
}

async fn compact_log<T>(peers: Arc<Vec<web::Data<JMAPServer<T>>>>)
where
    T: for<'x> Store<'x> + 'static,
{
    let last_log = peers.last().unwrap().get_last_log().await.unwrap().unwrap();

    join_all(peers.iter().map(|peer| {
        let store = peer.store.clone();
        tokio::task::spawn_blocking(move || store.compact_log(last_log.index).unwrap())
    }))
    .await;
}

#[test]
#[ignore]
fn postmortem() {
    let dbs = (1..=6)
        .map(|n| super::store::init_db_params::<RocksDB>("st_cluster", n, 5, false).0)
        .collect::<Vec<_>>();

    for (pos1, db1) in dbs.iter().enumerate() {
        for (pos2, db2) in dbs.iter().enumerate() {
            if pos1 != pos2 {
                print!("{}/{} -> ", pos1, pos2);
                println!("{:?}", db1.compare_with(db2));
            }
        }
    }
}

#[tokio::test]
#[cfg_attr(not(feature = "test_cluster"), ignore)]
async fn test_cluster() {
    tracing_subscriber::fmt::init();
    raft_election::<RocksDB>().await;
    merge_mail_threads::<RocksDB>().await;
    crud_ops::<RocksDB>().await;
    resolve_log_conflict::<RocksDB>().await;
}

#[tokio::test]
#[cfg_attr(not(feature = "fuzz_cluster"), ignore)]
async fn fuzz_cluster() {
    tracing_subscriber::fmt::init();
    cluster_fuzz::<RocksDB>(vec![]).await;
}
