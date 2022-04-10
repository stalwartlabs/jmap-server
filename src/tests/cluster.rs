use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use actix_web::web;
use futures::future::join_all;
use rand::Rng;
use store::{
    config::EnvSettings,
    log::{LogIndex, RaftId},
    parking_lot::Mutex,
    AccountId, Collection, DocumentId, JMAPId, JMAPIdPrefix, JMAPStore, Store,
};

use store_rocksdb::RocksDB;
use store_test::{
    destroy_temp_dir, init_settings,
    jmap_mail_merge_threads::build_thread_test_messages,
    jmap_mail_set::{delete_email, insert_email, update_email},
    jmap_mailbox::{delete_mailbox, insert_mailbox, update_mailbox},
    StoreCompareWith,
};
use tokio::{sync::mpsc, time::sleep};

use crate::{
    cluster::{self, main::start_cluster, IPC_CHANNEL_BUFFER},
    tests::store::init_db_params,
    JMAPServer,
};

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
enum Ac {
    NewEmail((JMAPId, JMAPId)),
    UpdateEmail(JMAPId),
    DeleteEmail(JMAPId),
    InsertMailbox(JMAPId),
    UpdateMailbox(JMAPId),
    DeleteMailbox(JMAPId),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum Cmd {
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

        let (tx, rx) = mpsc::channel::<cluster::Event>(IPC_CHANNEL_BUFFER);
        let jmap_server = web::Data::new(JMAPServer {
            store: JMAPStore::new(T::open(&settings).unwrap(), &settings).into(),
            worker_pool: rayon::ThreadPoolBuilder::new()
                .num_threads(num_cpus::get())
                .build()
                .unwrap(),
            cluster_tx: tx.clone(),
            is_leader: false.into(),
            is_up_to_date: false.into(),
            #[cfg(test)]
            is_offline: false.into(),
        });

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
                delete_mailbox(store, 2, mailbox_map.lock().remove(&local_id).unwrap());
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
                        /*println!(
                            "Comparing store of leader {} with follower {}",
                            leader_pos, follower_pos
                        );*/
                        let keys_leader = leader.store.compare_with(&follower.store);
                        /*println!(
                            "Comparing store of follower {} with leader {}",
                            follower_pos, leader_pos
                        );*/
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

async fn election<T>()
where
    T: for<'x> Store<'x> + 'static,
{
    // Test election.
    println!("Testing raft elections on a 5 nodes cluster...");
    let mut cluster = Cluster::<RocksDB>::new(5, true);
    let peers = cluster.start_cluster().await;

    assert_cluster_updated(&peers).await;
    assert_leader_elected(&peers)
        .await
        .set_offline(true, true)
        .await;
    assert_leader_elected(&peers)
        .await
        .set_offline(true, true)
        .await;
    assert_leader_elected(&peers)
        .await
        .set_offline(true, true)
        .await;
    assert_no_quorum(&peers).await;
    activate_all_peers(&peers).await;
    assert_leader_elected(&peers).await;
}

async fn distributed_thread_merge<T>()
where
    T: for<'x> Store<'x> + 'static,
{
    let mut cluster = Cluster::<T>::new(5, true);
    let peers = cluster.start_cluster().await;

    let mut messages = build_thread_test_messages()
        .into_iter()
        .rev()
        .collect::<Vec<_>>();
    println!(
        "Testing distributed e-mail thread merge ({} messages)...",
        messages.len()
    );

    // Create the Inbox
    let leader = assert_leader_elected(&peers).await;
    let inbox_id = insert_mailbox(&leader.store, 1, "Inbox", "INBOX");
    leader.update_uncommitted_index().await;
    assert_cluster_updated(&peers).await;

    // Keep one peer down to test full sync at the end
    leader.set_offline(true, true).await;

    // Insert chunks of ten messages in different nodes
    let mut prev_offline_leader: Option<&web::Data<JMAPServer<T>>> = None;
    while !messages.is_empty() {
        let chunk = messages
            .drain(0..std::cmp::min(20, messages.len()))
            .collect::<Vec<_>>();
        let leader = assert_leader_elected(&peers).await;
        let store = leader.store.clone();
        tokio::task::spawn_blocking(move || {
            for raw_message in chunk {
                insert_email(
                    &store,
                    1,
                    raw_message.into_bytes(),
                    vec![inbox_id],
                    vec![],
                    None,
                );
            }
        })
        .await
        .unwrap();

        // Notify peers of changes
        leader.update_uncommitted_index().await;
        assert_cluster_updated(&peers).await;

        // Bring back previous offline leader
        if let Some(prev_offline_leader) = prev_offline_leader {
            prev_offline_leader.set_offline(false, true).await;
        }
        assert_cluster_updated(&peers).await;
        assert_mirrored_stores(peers.clone(), true).await;

        // Deactivate the current leader
        leader.set_offline(true, true).await;
        prev_offline_leader = Some(leader);
    }

    // Activate all nodes
    activate_all_peers(&peers).await;
    assert_cluster_updated(&peers).await;
    assert_mirrored_stores(peers.clone(), false).await;

    // Add a new peer and send a snapshot to it.
    compact_log(peers.clone()).await;
    let peers = cluster.extend_cluster(peers, 1).await;
    assert_cluster_updated(&peers).await;
    assert_mirrored_stores(peers.clone(), false).await;

    shutdown_all(peers).await;

    cluster.cleanup();
}

async fn distributed_update_delete<T>()
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Testing distributed update/delete operations...");
    let mut cluster = Cluster::<T>::new(5, true);
    let peers = cluster.start_cluster().await;

    // Keep one node offline
    assert_leader_elected(&peers)
        .await
        .set_offline(true, true)
        .await;

    let mut prev_offline_leader: Option<&web::Data<JMAPServer<T>>> = None;
    let mailbox_map = Arc::new(Mutex::new(HashMap::new()));
    let email_map = Arc::new(Mutex::new(HashMap::new()));

    for (batch_num, batch) in test_batch().into_iter().enumerate() {
        let leader = assert_leader_elected(&peers).await;
        let store = leader.store.clone();

        let mailbox_map = mailbox_map.clone();
        let email_map = email_map.clone();

        tokio::task::spawn_blocking(move || {
            for action in batch {
                action.execute(&store, &mailbox_map, &email_map, batch_num);
            }
        })
        .await
        .unwrap();

        // Notify peers of changes
        leader.update_uncommitted_index().await;
        assert_cluster_updated(&peers).await;

        // Bring back previous offline leader
        if let Some(prev_offline_leader) = prev_offline_leader {
            prev_offline_leader.set_offline(false, true).await;
        }
        assert_cluster_updated(&peers).await;
        assert_mirrored_stores(peers.clone(), true).await;

        // Deactivate the current leader
        leader.set_offline(true, true).await;
        prev_offline_leader = Some(leader);
    }

    // Activate all nodes
    activate_all_peers(&peers).await;
    assert_cluster_updated(&peers).await;
    assert_mirrored_stores(peers.clone(), false).await;

    // Add a new peer and send a snapshot to it.
    compact_log(peers.clone()).await;
    let peers = cluster.extend_cluster(peers, 1).await;
    assert_cluster_updated(&peers).await;
    assert_mirrored_stores(peers.clone(), false).await;

    shutdown_all(peers).await;

    cluster.cleanup();
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

#[allow(clippy::comparison_chain)]
async fn resolve_log_conflict<T>()
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Testing log conflict resolution...");

    for conflict_term in 14..30 {
        let mut cluster = Cluster::<T>::new(2, true);

        let store1 = cluster.peers[0].jmap_server.store.clone();
        let store2 = cluster.peers[1].jmap_server.store.clone();
        let peer1 = cluster.peers[0].jmap_server.clone();
        let peer2 = cluster.peers[1].jmap_server.clone();

        let mailbox_map1 = Arc::new(Mutex::new(HashMap::new()));
        let mailbox_map2 = Arc::new(Mutex::new(HashMap::new()));
        let email_map1 = Arc::new(Mutex::new(HashMap::new()));
        let email_map2 = Arc::new(Mutex::new(HashMap::new()));

        let mut term_count = 0;

        println!("------- TERM {} ", conflict_term);

        for (batch_num, batch) in test_batch().into_iter().enumerate() {
            for action in batch {
                term_count += 1;

                peer1.set_leader(term_count);
                action.execute(&store1, &mailbox_map1, &email_map1, batch_num);

                if term_count < conflict_term {
                    peer2.set_leader(term_count);
                    action.execute(&store2, &mailbox_map2, &email_map2, batch_num);
                } else {
                    match term_count % 4 {
                        0 if !email_map2.lock().is_empty() => {
                            Ac::UpdateEmail(*email_map2.lock().keys().next().unwrap())
                        }
                        1 if !mailbox_map2.lock().is_empty() => {
                            Ac::UpdateMailbox(*mailbox_map2.lock().keys().next().unwrap())
                        }
                        2 => Ac::InsertMailbox(term_count),
                        _ => Ac::NewEmail((term_count, 1)),
                    }
                    .execute(
                        &store2,
                        &mailbox_map2,
                        &email_map2,
                        term_count as usize,
                    );
                }
            }
        }

        peer1
            .set_leader_commit_index(peer1.get_last_log().await.unwrap().unwrap().index)
            .await
            .unwrap();
        peer2
            .set_leader_commit_index(peer2.get_last_log().await.unwrap().unwrap().index)
            .await
            .unwrap();
        peer1.set_follower();
        peer2.set_follower();

        // Activate all nodes
        let peers = cluster.start_cluster().await;
        assert_leader_elected(&peers).await;
        assert_cluster_updated(&peers).await;
        assert_mirrored_stores(peers.clone(), false).await;

        shutdown_all(peers).await;

        cluster.cleanup();
    }
}

#[allow(clippy::type_complexity)]
async fn cluster_fuzz<T>(mut replay_cmds: Vec<Cmd>)
where
    T: for<'x> Store<'x> + 'static,
{
    let is_replay = !replay_cmds.is_empty();
    if is_replay {
        println!("Replaying {} commands...", replay_cmds.len());
    } else {
        println!("Fuzzing cluster...");
    }

    let mut cluster = Cluster::<T>::new(5, true);
    let peers = cluster.start_cluster().await;
    let mut actions = Cmds::default();
    let id_map: Arc<Mutex<HashMap<(AccountId, Collection), HashMap<JMAPId, JMAPId>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let mut id_seq = 0;
    let mut change_seq = 0;

    let get_random_id = |account_id,
                         collection,
                         id_map: &HashMap<(AccountId, Collection), HashMap<JMAPId, JMAPId>>|
     -> Option<(JMAPId, JMAPId)> {
        let ids = id_map.get(&(account_id, collection))?;
        let ids_num = ids.len();
        if ids_num > 0 {
            ids.iter()
                .nth(if ids_num > 1 {
                    rand::thread_rng().gen_range::<usize, _>(0..ids_num)
                } else {
                    0
                })
                .map(|(id1, id2)| (*id1, *id2))
        } else {
            None
        }
    };

    assert_leader_elected(&peers).await;
    assert_cluster_updated(&peers).await;

    loop {
        let cmd = if !is_replay {
            match rand::thread_rng().gen_range::<i32, _>(0..15) {
                0 => Cmd::StopLeader,
                1 => Cmd::StopFollower,
                2 => Cmd::StartOneOffline,
                3..=6 => Cmd::StartAllOffline,
                _ => {
                    let account_id = rand::thread_rng().gen_range::<AccountId, _>(1..=5);

                    Cmd::Update {
                        account_id,
                        action: match rand::thread_rng().gen_range::<i32, _>(0..6) {
                            0 => {
                                let mailbox_id = if let Some((local_id, _)) =
                                    get_random_id(account_id, Collection::Mailbox, &id_map.lock())
                                {
                                    local_id
                                } else {
                                    0
                                };
                                id_seq += 1;
                                Ac::NewEmail((mailbox_id, id_seq as JMAPId))
                            }
                            1 => {
                                if let Some((local_id, _)) =
                                    get_random_id(account_id, Collection::Mail, &id_map.lock())
                                {
                                    Ac::UpdateEmail(local_id)
                                } else {
                                    continue;
                                }
                            }
                            2 => {
                                if let Some((local_id, _)) =
                                    get_random_id(account_id, Collection::Mail, &id_map.lock())
                                {
                                    Ac::DeleteEmail(local_id)
                                } else {
                                    continue;
                                }
                            }
                            3 => {
                                id_seq += 1;
                                Ac::InsertMailbox(id_seq as JMAPId)
                            }
                            4 => {
                                if let Some((local_id, _)) =
                                    get_random_id(account_id, Collection::Mailbox, &id_map.lock())
                                {
                                    Ac::UpdateMailbox(local_id)
                                } else {
                                    continue;
                                }
                            }
                            5 => {
                                if let Some((local_id, _)) =
                                    get_random_id(account_id, Collection::Mailbox, &id_map.lock())
                                {
                                    Ac::DeleteMailbox(local_id)
                                } else {
                                    continue;
                                }
                            }
                            _ => unreachable!(),
                        },
                    }
                }
            }
        } else {
            if replay_cmds.is_empty() {
                break;
            }
            let cmd = replay_cmds.remove(0);
            if matches!(
                &cmd,
                Cmd::Update {
                    action: Ac::InsertMailbox(_) | Ac::NewEmail(_),
                    ..
                }
            ) {
                id_seq += 1;
            }
            cmd
        };

        let mut success = false;

        println!("{:?} (seq {})", cmd, change_seq);
        actions.cmds.push(cmd);

        match actions.cmds.last().unwrap() {
            Cmd::StopLeader => {
                for peer in peers.iter() {
                    if peer.is_leader() && !peer.is_offline() {
                        peer.set_offline(true, true).await;
                        success = true;
                        break;
                    }
                }
            }
            Cmd::StopFollower => {
                for peer in peers.iter() {
                    if !peer.is_leader() && !peer.is_offline() {
                        peer.set_offline(true, true).await;
                        success = true;
                        break;
                    }
                }
            }
            Cmd::StartOneOffline => {
                for peer in peers.iter() {
                    if peer.is_offline() {
                        peer.set_offline(false, true).await;
                        success = true;
                        break;
                    }
                }
            }
            Cmd::StartAllOffline => {
                for peer in peers.iter() {
                    if peer.is_offline() {
                        peer.set_offline(false, true).await;
                        success = true;
                    }
                }
            }
            Cmd::Update { account_id, action } => {
                if num_online_peers(&peers) < 3 {
                    if is_replay {
                        panic!("No quorum to execute {:?}.", actions.cmds.last().unwrap());
                    } else {
                        println!(
                            "Skipping {:?} due to no quorum.",
                            actions.cmds.pop().unwrap()
                        );
                    }
                    continue;
                }

                let mut leader = None;
                'o: for _ in 0..100 {
                    for peer in peers.iter() {
                        if !peer.is_offline() && peer.is_leader() {
                            leader = peer.into();
                            break 'o;
                        }
                    }
                    sleep(Duration::from_millis(100)).await;
                }
                let (leader, store) = if let Some(leader) = leader {
                    (leader, leader.store.clone())
                } else {
                    panic!(
                        "No elected leader to execute {:?}.",
                        actions.cmds.last().unwrap()
                    );
                };

                if matches!(action, Ac::NewEmail((mailbox_id, _)) if mailbox_id == &0) {
                    id_seq += 1;
                }
                change_seq += 1;

                let action = action.clone();
                let id_map = id_map.clone();
                let account_id = *account_id;

                tokio::task::spawn_blocking(move || match action {
                    Ac::NewEmail((local_mailbox_id, local_id)) => {
                        let (local_mailbox_id, local_id) = if local_mailbox_id == 0 {
                            id_map
                                .lock()
                                .entry((account_id, Collection::Mailbox))
                                .or_insert_with(HashMap::new)
                                .insert(
                                    id_seq as JMAPId,
                                    insert_mailbox(
                                        &store,
                                        account_id,
                                        &format!("Mailbox {}", id_seq),
                                        &format!("role_{}", id_seq),
                                    ),
                                );
                            (id_seq, JMAPId::from_parts(id_seq as u32, local_id as u32))
                        } else {
                            (
                                local_mailbox_id,
                                JMAPId::from_parts(local_mailbox_id as u32, local_id as u32),
                            )
                        };

                        let store_mailbox_id = *id_map
                            .lock()
                            .get(&(account_id, Collection::Mailbox))
                            .unwrap()
                            .get(&local_mailbox_id)
                            .unwrap();
                        id_map
                            .lock()
                            .entry((account_id, Collection::Mail))
                            .or_insert_with(HashMap::new)
                            .insert(
                                local_id,
                                insert_email(
                                    &store,
                                    account_id,
                                    format!(
                                        "From: test@test.com\nSubject: test {}\n\nTest message {}",
                                        local_id, local_id
                                    )
                                    .into_bytes(),
                                    vec![store_mailbox_id],
                                    vec![],
                                    None,
                                ),
                            );
                    }
                    Ac::UpdateEmail(local_id) => {
                        println!("{:?}", id_map.lock().get(&(account_id, Collection::Mail)));
                        update_email(
                            &store,
                            account_id,
                            *id_map
                                .lock()
                                .get(&(account_id, Collection::Mail))
                                .unwrap()
                                .get(&local_id)
                                .unwrap(),
                            None,
                            Some(vec![format!("tag_{}", change_seq)]),
                        );
                    }
                    Ac::DeleteEmail(local_id) => {
                        delete_email(
                            &store,
                            account_id,
                            id_map
                                .lock()
                                .get_mut(&(account_id, Collection::Mail))
                                .unwrap()
                                .remove(&local_id)
                                .unwrap(),
                        );
                    }
                    Ac::InsertMailbox(local_id) => {
                        id_map
                            .lock()
                            .entry((account_id, Collection::Mailbox))
                            .or_insert_with(HashMap::new)
                            .insert(
                                local_id,
                                insert_mailbox(
                                    &store,
                                    account_id,
                                    &format!("Mailbox {}", local_id),
                                    &format!("role_{}", local_id),
                                ),
                            );
                    }
                    Ac::UpdateMailbox(local_id) => update_mailbox(
                        &store,
                        account_id,
                        *id_map
                            .lock()
                            .get(&(account_id, Collection::Mailbox))
                            .unwrap()
                            .get(&local_id)
                            .unwrap(),
                        local_id as u32,
                        change_seq,
                    ),
                    Ac::DeleteMailbox(local_id) => {
                        if let Some(ids) = id_map.lock().get_mut(&(account_id, Collection::Mail)) {
                            if !ids.is_empty() {
                                let del_keys: Vec<JMAPId> = ids
                                    .keys()
                                    .filter(|key| (*key).get_prefix_id() == local_id as u32)
                                    .copied()
                                    .collect();
                                for del_key in del_keys {
                                    ids.remove(&del_key);
                                }
                            }
                        }

                        delete_mailbox(
                            &store,
                            account_id,
                            id_map
                                .lock()
                                .get_mut(&(account_id, Collection::Mailbox))
                                .unwrap()
                                .remove(&local_id)
                                .unwrap(),
                        );
                    }
                })
                .await
                .unwrap();

                success = true;

                leader.update_uncommitted_index().await;
                assert_cluster_updated(&peers).await;
                assert_mirrored_stores(peers.clone(), true).await;
            }
        }

        if !success {
            if is_replay {
                panic!("Failed to execute {:?}", actions.cmds.last().unwrap());
            } else {
                actions.cmds.pop();
            }
        } else if !matches!(actions.cmds.last().unwrap(), Cmd::Update { .. }) {
            sleep(Duration::from_millis(1000)).await;
        }
    }

    // Activate all nodes
    activate_all_peers(&peers).await;
    assert_cluster_updated(&peers).await;
    assert_mirrored_stores(peers.clone(), false).await;
    shutdown_all(peers).await;
    actions.clean_exit = true;

    cluster.cleanup();
}

#[tokio::test]
async fn test_cluster() {
    tracing_subscriber::fmt::init();

    //election::<RocksDB>().await;
    //distributed_thread_merge::<RocksDB>().await;
    //distributed_update_delete::<RocksDB>().await;
    //resolve_log_conflict::<RocksDB>().await;
    cluster_fuzz::<RocksDB>(vec![/*
        Cmd::Update {
            account_id: 2,
            action: Ac::InsertMailbox(1),
        },
        Cmd::Update {
            account_id: 5,
            action: Ac::NewEmail((0, 2)),
        },
        Cmd::Update {
            account_id: 5,
            action: Ac::UpdateMailbox(3),
        },
        Cmd::StopFollower,
        Cmd::StopLeader,
        Cmd::StartAllOffline,
        Cmd::Update {
            account_id: 5,
            action: Ac::DeleteEmail(12884901890),
        },
        Cmd::Update {
            account_id: 2,
            action: Ac::UpdateMailbox(1),
        },
        Cmd::Update {
            account_id: 2,
            action: Ac::InsertMailbox(4),
        },
        Cmd::Update {
            account_id: 5,
            action: Ac::NewEmail((3, 5)),
        },
        Cmd::Update {
            account_id: 1,
            action: Ac::InsertMailbox(6),
        },
        Cmd::Update {
            account_id: 1,
            action: Ac::DeleteMailbox(6),
        },
        Cmd::StopLeader,
        Cmd::Update {
            account_id: 3,
            action: Ac::NewEmail((0, 7)),
        },
        Cmd::StopFollower,
        Cmd::StartAllOffline,
        Cmd::Update {
            account_id: 1,
            action: Ac::InsertMailbox(9),
        },
        Cmd::Update {
            account_id: 3,
            action: Ac::UpdateEmail(34359738375),
        },
        Cmd::Update {
            account_id: 1,
            action: Ac::DeleteMailbox(9),
        },
        Cmd::Update {
            account_id: 2,
            action: Ac::InsertMailbox(10),
        },
        Cmd::StopFollower,
        Cmd::StartOneOffline,
        Cmd::StopFollower,
        Cmd::StartAllOffline,
        Cmd::Update {
            account_id: 4,
            action: Ac::NewEmail((0, 11)),
        },
        Cmd::Update {
            account_id: 2,
            action: Ac::UpdateMailbox(4),
        },
        Cmd::StopLeader,
        Cmd::Update {
            account_id: 2,
            action: Ac::NewEmail((4, 13)),
        },
        Cmd::Update {
            account_id: 5,
            action: Ac::UpdateMailbox(3),
        },
        Cmd::Update {
            account_id: 5,
            action: Ac::DeleteEmail(12884901893),
        },
        Cmd::StartAllOffline,
        Cmd::StopFollower,
        Cmd::Update {
            account_id: 3,
            action: Ac::DeleteEmail(34359738375),
        },
        Cmd::StartAllOffline,
        Cmd::Update {
            account_id: 5,
            action: Ac::DeleteMailbox(3),
        },
        Cmd::StopFollower,
        Cmd::StartAllOffline,
        Cmd::StopLeader,
        Cmd::StopFollower,
        Cmd::StartAllOffline,
        Cmd::StopLeader,
        Cmd::StartAllOffline,
        Cmd::Update {
            account_id: 2,
            action: Ac::UpdateMailbox(1),
        },
        Cmd::Update {
            account_id: 4,
            action: Ac::UpdateMailbox(12),
        },
        Cmd::Update {
            account_id: 3,
            action: Ac::NewEmail((8, 14)),
        },
        Cmd::Update {
            account_id: 3,
            action: Ac::UpdateMailbox(8),
        },
        Cmd::Update {
            account_id: 2,
            action: Ac::NewEmail((10, 15)),
        },
        Cmd::Update {
            account_id: 2,
            action: Ac::UpdateEmail(42949672975),
        },
        Cmd::StopFollower,
        Cmd::StartAllOffline,
        Cmd::StopFollower,
        Cmd::StopFollower,
        Cmd::StartAllOffline,
        Cmd::Update {
            account_id: 4,
            action: Ac::InsertMailbox(16),
        },
        Cmd::Update {
            account_id: 5,
            action: Ac::InsertMailbox(17),
        },
        Cmd::StopFollower,
        Cmd::Update {
            account_id: 2,
            action: Ac::NewEmail((1, 18)),
        },
        Cmd::Update {
            account_id: 4,
            action: Ac::DeleteMailbox(16),
        },
        Cmd::Update {
            account_id: 2,
            action: Ac::UpdateMailbox(1),
        },
        Cmd::Update {
            account_id: 5,
            action: Ac::DeleteMailbox(17),
        },
        Cmd::StartAllOffline,
        Cmd::Update {
            account_id: 5,
            action: Ac::NewEmail((0, 19)),
        },
        Cmd::Update {
            account_id: 4,
            action: Ac::NewEmail((12, 21)),
        },
        Cmd::Update {
            account_id: 5,
            action: Ac::UpdateEmail(85899345939),
        },
        Cmd::Update {
            account_id: 5,
            action: Ac::UpdateEmail(85899345939),
        },
        Cmd::Update {
            account_id: 4,
            action: Ac::DeleteEmail(51539607573),
        },
        Cmd::Update {
            account_id: 1,
            action: Ac::InsertMailbox(22),
        },
        Cmd::Update {
            account_id: 3,
            action: Ac::InsertMailbox(23),
        },
        Cmd::StopFollower,
        Cmd::StopFollower,
        Cmd::Update {
            account_id: 1,
            action: Ac::UpdateMailbox(22),
        },
        Cmd::Update {
            account_id: 3,
            action: Ac::DeleteEmail(34359738382),
        },
        Cmd::Update {
            account_id: 4,
            action: Ac::NewEmail((12, 24)),
        },
        Cmd::Update {
            account_id: 4,
            action: Ac::DeleteMailbox(12),
        },
        Cmd::Update {
            account_id: 5,
            action: Ac::InsertMailbox(25),
        },
        Cmd::Update {
            account_id: 1,
            action: Ac::UpdateMailbox(22),
        },
        Cmd::StartAllOffline,
        Cmd::StopLeader,
        Cmd::StartOneOffline,
        Cmd::Update {
            account_id: 3,
            action: Ac::DeleteMailbox(8),
        },
        Cmd::StopLeader,
        Cmd::StartAllOffline,
        Cmd::Update {
            account_id: 2,
            action: Ac::NewEmail((4, 26)),
        },*/
    ])
    .await;
}

#[test]
fn postmortem() {
    let dbs = (1..=6)
        .map(|n| init_db_params::<RocksDB>("st_cluster", n, 5, false).0)
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
