use std::sync::Arc;

use actix_web::web;
use store::{ahash::AHashMap, parking_lot::Mutex, Store};

use crate::{
    tests::cluster::utils::{
        activate_all_peers, assert_cluster_updated, assert_leader_elected, assert_mirrored_stores,
        compact_log, shutdown_all, test_batch, Clients, Cluster,
    },
    JMAPServer,
};

pub async fn test<T>()
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Testing distributed CRUD operations...");
    let mut cluster = Cluster::<T>::new(5, true).await;
    let peers = cluster.start_cluster().await;

    // Wait for leader to be elected
    let leader = assert_leader_elected(&peers).await;

    // Connect clients
    let clients = Arc::new(Clients::new(5).await);

    // Keep one leader offline
    leader.set_offline(true, true).await;

    let mut prev_offline_leader: Option<&web::Data<JMAPServer<T>>> = None;
    let mailbox_map = Arc::new(Mutex::new(AHashMap::new()));
    let email_map = Arc::new(Mutex::new(AHashMap::new()));

    for (batch_num, batch) in test_batch().into_iter().enumerate() {
        let leader = assert_leader_elected(&peers).await;
        //let store = leader.store.clone();

        let mailbox_map = mailbox_map.clone();
        let email_map = email_map.clone();
        let clients = clients.clone();

        for action in batch {
            action
                .execute::<T>(&clients, &mailbox_map, &email_map, batch_num)
                .await;
        }

        // Notify peers of changes
        //leader.commit_last_index().await;
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
    println!("Activating all nodes...");
    activate_all_peers(&peers).await;
    assert_cluster_updated(&peers).await;
    assert_mirrored_stores(peers.clone(), false).await;

    // Add a new peer and send a snapshot to it.
    println!("Compacting log...");
    compact_log(peers.clone()).await;
    println!("Adding peer to cluster...");
    let peers = cluster.extend_cluster(peers, 1).await;
    assert_cluster_updated(&peers).await;
    assert_mirrored_stores(peers.clone(), false).await;

    shutdown_all(peers).await;

    cluster.cleanup();
}
