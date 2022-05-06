use store::Store;

use crate::tests::cluster::utils::{
    activate_all_peers, assert_cluster_updated, assert_leader_elected, assert_no_quorum, Cluster,
};

pub async fn raft_election<T>()
where
    T: for<'x> Store<'x> + 'static,
{
    // Test election.
    println!("Testing raft elections on a 5 nodes cluster...");
    let mut cluster = Cluster::<T>::new(5, true);
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
