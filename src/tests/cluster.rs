use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use actix_web::web;
use store::{raft::RaftId, JMAPStore, Store};
use store_rocksdb::RocksDB;
use store_test::{
    destroy_temp_dir, init_settings,
    jmap_mail_merge_threads::build_thread_test_messages,
    jmap_mail_set::{delete_email, insert_email, update_email},
    jmap_mailbox::{delete_mailbox, insert_mailbox, rename_mailbox},
    StoreCompareWith,
};
use tokio::{sync::mpsc, time::sleep};

use crate::{
    cluster::{self, main::start_cluster, IPC_CHANNEL_BUFFER},
    tests::store::init_db_params,
    JMAPServer,
};

async fn build_peer(
    peer_num: u32,
    num_peers: u32,
    delete_if_exists: bool,
) -> (web::Data<JMAPServer<RocksDB>>, PathBuf) {
    let (settings, temp_dir) = init_settings("st_cluster", peer_num, num_peers, delete_if_exists);

    let (tx, rx) = mpsc::channel::<cluster::Event>(IPC_CHANNEL_BUFFER);
    let jmap_server = web::Data::new(JMAPServer {
        store: JMAPStore::new(RocksDB::open(&settings).unwrap(), &settings).into(),
        worker_pool: rayon::ThreadPoolBuilder::new()
            .num_threads(num_cpus::get())
            .build()
            .unwrap(),
        cluster_tx: tx.clone(),
        is_cluster: true,
        is_leader: false.into(),
        is_up_to_date: false.into(),
        #[cfg(test)]
        is_offline: false.into(),
    });
    start_cluster(jmap_server.clone(), &settings, rx, tx).await;

    (jmap_server, temp_dir)
}

async fn build_cluster(
    num_peers: u32,
    delete_if_exists: bool,
) -> (Arc<Vec<web::Data<JMAPServer<RocksDB>>>>, Vec<PathBuf>) {
    tracing_subscriber::fmt::init();
    let mut servers = Vec::new();
    let mut paths = Vec::new();
    for peer_num in 1..=num_peers {
        let (server, path) = build_peer(peer_num, num_peers, delete_if_exists).await;
        servers.push(server);
        paths.push(path);
    }
    (Arc::new(servers), paths)
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

async fn activate_all_peers<T>(peers: &[web::Data<JMAPServer<T>>])
where
    T: for<'x> Store<'x> + 'static,
{
    for peer in peers.iter() {
        if peer.is_offline() {
            peer.set_offline(false).await;
        }
    }
}

async fn assert_cluster_updated<T>(peers: &[web::Data<JMAPServer<T>>])
where
    T: for<'x> Store<'x> + 'static,
{
    let mut updated = vec![false; peers.len()];

    for _ in 0..100 {
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
                        panic!("Raft index mismatch: {:?} {:?}", last_logs, updated);
                    }
                }
            }
            return;
        }
    }
    panic!("Some nodes are not up to date: {:?}", updated);
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
                        /*println!(
                            "Comparing stores of leader {} with follower {}",
                            leader_pos, follower_pos
                        );*/
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

async fn test_election<T>(peers: Arc<Vec<web::Data<JMAPServer<T>>>>)
where
    T: for<'x> Store<'x> + 'static,
{
    // Test election.
    println!("Testing raft election...");
    assert_cluster_updated(&peers).await;
    assert_leader_elected(&peers).await.set_offline(true).await;
    assert_leader_elected(&peers).await.set_offline(true).await;
    assert_leader_elected(&peers).await.set_offline(true).await;
    assert_no_quorum(&peers).await;
    activate_all_peers(&peers).await;
    assert_leader_elected(&peers).await;
}

async fn test_distruibuted_thread_merge<T>(peers: Arc<Vec<web::Data<JMAPServer<T>>>>)
where
    T: for<'x> Store<'x> + 'static,
{
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
    leader.notify_changes().await;
    assert_cluster_updated(&peers).await;

    // Keep one peer down to test full sync at the end
    leader.set_offline(true).await;

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
        leader.notify_changes().await;
        assert_cluster_updated(&peers).await;

        // Bring back previous offline leader
        if let Some(prev_offline_leader) = prev_offline_leader {
            prev_offline_leader.set_offline(false).await;
        }
        assert_cluster_updated(&peers).await;
        assert_mirrored_stores(peers.clone(), true).await;

        // Deactivate the current leader
        leader.set_offline(true).await;
        prev_offline_leader = Some(leader);
    }

    // Activate all nodes
    activate_all_peers(&peers).await;
    assert_cluster_updated(&peers).await;
    assert_mirrored_stores(peers, false).await;
}

#[derive(Debug)]
enum Ac {
    NewEmail((u32, u32)),
    UpdateEmail(u32),
    DeleteEmail(u32),
    InsertMailbox(u32),
    UpdateMailbox(u32),
    DeleteMailbox(u32),
}

async fn test_distruibuted_update_delete<T>(peers: Arc<Vec<web::Data<JMAPServer<T>>>>)
where
    T: for<'x> Store<'x> + 'static,
{
    let test_batches = vec![
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
    ];

    // Keep one node offline
    assert_leader_elected(&peers).await.set_offline(true).await;
    let mut prev_offline_leader: Option<&web::Data<JMAPServer<T>>> = None;
    let mut mailbox_map = HashMap::new();
    let mut email_map = HashMap::new();

    for (batch_num, batch) in test_batches.into_iter().enumerate() {
        println!("{:?}", batch);
        let leader = assert_leader_elected(&peers).await;

        for action in batch {
            match action {
                Ac::NewEmail((local_id, mailbox_id)) => {
                    email_map.insert(
                        local_id,
                        insert_email(
                            &leader.store,
                            2,
                            format!(
                                "From: test@test.com\nSubject: test {}\n\nTest message {}",
                                local_id, local_id
                            )
                            .into_bytes(),
                            vec![*mailbox_map.get(&mailbox_id).unwrap()],
                            vec![],
                            None,
                        ),
                    );
                }
                Ac::UpdateEmail(local_id) => {
                    update_email(
                        &leader.store,
                        2,
                        *mailbox_map.get(&local_id).unwrap(),
                        None,
                        Some(vec![format!("tag_{}", batch_num)]),
                    );
                }
                Ac::DeleteEmail(local_id) => {
                    delete_email(&leader.store, 2, email_map.remove(&local_id).unwrap());
                }
                Ac::InsertMailbox(local_id) => {
                    mailbox_map.insert(
                        local_id,
                        insert_mailbox(
                            &leader.store,
                            2,
                            &format!("Mailbox {}", local_id),
                            &format!("role_{}", local_id),
                        ),
                    );
                }
                Ac::UpdateMailbox(local_id) => {
                    rename_mailbox(
                        &leader.store,
                        2,
                        *mailbox_map.get(&local_id).unwrap(),
                        &format!("Mailbox {}_{}", local_id, batch_num),
                    );
                }
                Ac::DeleteMailbox(local_id) => {
                    delete_mailbox(&leader.store, 2, mailbox_map.remove(&local_id).unwrap());
                }
            }

            println!("{:?}\n{:?}", mailbox_map, email_map);
        }
        // Notify peers of changes
        leader.notify_changes().await;
        assert_cluster_updated(&peers).await;

        // Bring back previous offline leader
        if let Some(prev_offline_leader) = prev_offline_leader {
            prev_offline_leader.set_offline(false).await;
        }
        assert_cluster_updated(&peers).await;
        assert_mirrored_stores(peers.clone(), true).await;

        // Deactivate the current leader
        leader.set_offline(true).await;
        prev_offline_leader = Some(leader);
    }

    // Activate all nodes
    activate_all_peers(&peers).await;
    assert_cluster_updated(&peers).await;
    assert_mirrored_stores(peers, false).await;
}

#[tokio::test]
async fn test_cluster() {
    let (peers, temp_dirs) = build_cluster(5, true).await;

    //test_election(peers.clone()).await;
    //test_distruibuted_thread_merge(peers.clone()).await;
    test_distruibuted_update_delete(peers.clone()).await;

    for temp_dir in temp_dirs {
        destroy_temp_dir(temp_dir);
    }
}

#[test]
fn test_coco() {
    let dbs = (1..=5)
        .map(|n| init_db_params("st_cluster", n, 5, false).0)
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
