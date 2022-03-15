use std::{path::PathBuf, time::Duration};

use actix_web::web;
use jmap_mail::import::JMAPMailImport;
use store::{JMAPStore, Tag};
use store_rocksdb::RocksDB;
use store_test::{destroy_temp_dir, init_settings};
use tokio::{sync::mpsc, time::sleep};

use crate::{
    cluster::{self, main::start_cluster, IPC_CHANNEL_BUFFER},
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
    });
    start_cluster(jmap_server.clone(), &settings, rx, tx).await;

    (jmap_server, temp_dir)
}

async fn build_cluster(
    num_peers: u32,
    delete_if_exists: bool,
) -> (Vec<web::Data<JMAPServer<RocksDB>>>, Vec<PathBuf>) {
    tracing_subscriber::fmt::init();
    let mut servers = Vec::new();
    let mut paths = Vec::new();
    for peer_num in 1..=num_peers {
        let (server, path) = build_peer(peer_num, num_peers, delete_if_exists).await;
        servers.push(server);
        paths.push(path);
    }
    (servers, paths)
}

async fn get_leader(
    peers: &[web::Data<JMAPServer<RocksDB>>],
) -> (usize, &web::Data<JMAPServer<RocksDB>>) {
    for _ in 0..100 {
        sleep(Duration::from_millis(100)).await;
        for (peer_num, peer) in peers.iter().enumerate() {
            if peer.is_leader() {
                return (peer_num, peer);
            }
        }
    }
    panic!("No leader found.");
}

async fn wait_for_update(peers: &[web::Data<JMAPServer<RocksDB>>]) {
    let mut updated = vec![false; peers.len()];
    for _ in 0..100 {
        sleep(Duration::from_millis(100)).await;
        for (peer_num, peer) in peers.iter().enumerate() {
            updated[peer_num] = peer.is_up_to_date();
        }
        if updated.iter().all(|u| *u) {
            println!("Cluster is updated.");
            return;
        }
    }
    panic!("Failed to propagate update to peers: {:?}", updated);
}

#[tokio::test]
async fn test_cluster() {
    let (peers, temp_dirs) = build_cluster(2, true).await;

    let (_, leader) = get_leader(&peers).await;

    leader
        .store
        .mail_import_blob(
            1,
            leader.store.assign_raft_id(),
            b"From: test@test.com\nSubject: hey\n\ntest".to_vec(),
            vec![1],
            vec![Tag::Text("hey".to_string())],
            None,
        )
        .unwrap();

    leader.store_changed().await;
    wait_for_update(&peers).await;

    for temp_dir in temp_dirs {
        destroy_temp_dir(temp_dir);
    }
}
