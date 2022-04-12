use std::{collections::HashMap, sync::Arc};

use actix_web::web;

use store::{parking_lot::Mutex, Store};

use crate::{
    tests::cluster::{
        activate_all_peers, assert_cluster_updated, assert_leader_elected, assert_mirrored_stores,
        compact_log, shutdown_all, test_batch, Cluster,
    },
    JMAPServer,
};

pub async fn crud_ops<T>()
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Testing distributed CRUD operations...");
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
