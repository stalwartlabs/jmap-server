use std::{path::PathBuf, sync::Arc, time::Duration};

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

async fn get_leader(peers: &[web::Data<JMAPServer<RocksDB>>]) -> &web::Data<JMAPServer<RocksDB>> {
    for _ in 0..100 {
        sleep(Duration::from_millis(100)).await;
        for (peer_num, peer) in peers.iter().enumerate() {
            if peer.is_leader() {
                println!("Peer {} is the leader.", peer_num);
                return peer;
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

    /*leader
    .store
    .mail_import_blob(
        1,
        b"From: test@test.com\nSubject: hey\n\ntest".to_vec(),
        vec![1],
        vec![Tag::Text("hey".to_string())],
        None,
    )
    .unwrap();*/
    let leader = get_leader(&peers).await;
    let _leader = leader.clone();
    tokio::task::spawn_blocking(move || {
        store_test::jmap_mail_query::jmap_mail_query_prepare(&_leader.store, 1);
    })
    .await
    .unwrap();

    leader.store_changed().await;
    wait_for_update(&peers).await;
    for peer in peers.iter() {
        if !peer.is_leader() {
            println!("Testing peer...");
            store_test::jmap_mail_query::jmap_mail_query(&peer.store, 1);
        }
    }

    for temp_dir in temp_dirs {
        destroy_temp_dir(temp_dir);
    }
}
