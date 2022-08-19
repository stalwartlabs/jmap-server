use store::Store;

use crate::tests::cluster::utils::{
    activate_all_peers, assert_cluster_updated, assert_leader_elected, assert_no_quorum,
    shutdown_all, Cluster,
};

pub async fn test<T>()
where
    T: for<'x> Store<'x> + 'static,
{
    // Test election.
    println!("Testing raft elections on a 5 nodes cluster...");
    let mut cluster = Cluster::<T>::new("st_cluster_election", 5, true).await;
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

    // Stop cluster
    cluster.stop_cluster().await;
    shutdown_all(peers).await;
    cluster.cleanup();
}
