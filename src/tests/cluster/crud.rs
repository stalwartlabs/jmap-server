use std::sync::Arc;

use actix_web::web;
use jmap_client::client::{Client, Credentials};
use store::{ahash::AHashMap, parking_lot::Mutex, Store};

use crate::{
    tests::cluster::utils::{
        activate_all_peers, assert_cluster_updated, assert_leader_elected, assert_mirrored_stores,
        compact_log, find_online_follower, shutdown_all, test_batch, Clients, Cluster,
    },
    JMAPServer,
};

pub async fn test<T>()
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Testing distributed CRUD operations...");
    let mut cluster = Cluster::<T>::new("st_cluster_crud", 5, true).await;
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
        if batch_num == 0 {
            let client = &clients.clients[0];
            // Create an account
            client.domain_create("example.com").await.unwrap();
            let account_id = client
                .individual_create("jdoe@example.com", "12345", "John Doe")
                .await
                .unwrap()
                .take_id();

            // Connect to one of the followers and test read replicas
            // Disable redirects to avoid the request from being redirected to the leader
            let follower_client = Client::new()
                .credentials(Credentials::bearer("DO_NOT_ATTEMPT_THIS_AT_HOME"))
                .connect(&format!(
                    "http://127.0.0.1:{}",
                    8001 + find_online_follower(&peers)
                ))
                .await
                .unwrap();
            assert_eq!(
                follower_client
                    .principal_get(&account_id, None::<Vec<_>>)
                    .await
                    .unwrap()
                    .unwrap()
                    .email()
                    .unwrap(),
                "jdoe@example.com"
            );
        }

        let mailbox_map = mailbox_map.clone();
        let email_map = email_map.clone();
        let clients = clients.clone();

        for action in batch {
            action
                .execute::<T>(&clients, &mailbox_map, &email_map, batch_num)
                .await;
        }

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
    let peers = cluster.extend_cluster("st_cluster_crud", peers, 1).await;
    assert_cluster_updated(&peers).await;
    assert_mirrored_stores(peers.clone(), false).await;

    // Stop cluster
    println!("Stopping cluster...");
    cluster.stop_cluster().await;
    println!("Shutting down cluster...");
    shutdown_all(peers).await;
    cluster.cleanup();
}
