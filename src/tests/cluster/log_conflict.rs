use std::sync::Arc;

use store::{ahash::AHashMap, parking_lot::Mutex, Store};

use crate::tests::cluster::utils::{
    assert_cluster_updated, assert_leader_elected, assert_mirrored_stores, shutdown_all,
    test_batch, Ac, Clients, Cluster,
};

#[allow(clippy::comparison_chain)]
pub async fn test<T>()
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Testing log conflict resolution...");

    for conflict_term in 14..30 {
        let mut cluster = Cluster::<T>::new(2, true).await;

        //let store1 = cluster.peers[0].jmap_server.store.clone();
        //let store2 = cluster.peers[1].jmap_server.store.clone();
        let peer1 = cluster.peers[0].jmap_server.clone();
        let peer2 = cluster.peers[1].jmap_server.clone();

        let mailbox_map1 = Arc::new(Mutex::new(AHashMap::new()));
        let mailbox_map2 = Arc::new(Mutex::new(AHashMap::new()));
        let email_map1 = Arc::new(Mutex::new(AHashMap::new()));
        let email_map2 = Arc::new(Mutex::new(AHashMap::new()));

        // Connect clients
        let clients = Arc::new(Clients::new(2).await);

        let mut term_count = 0;

        println!("------- TERM {} ", conflict_term);

        for (batch_num, batch) in test_batch().into_iter().enumerate() {
            for action in batch {
                term_count += 1;

                peer1.set_leader(term_count).await;
                action
                    .execute::<T>(&clients, &mailbox_map1, &email_map1, batch_num)
                    .await;

                if term_count < conflict_term {
                    peer2.set_leader(term_count).await;
                    action
                        .execute::<T>(&clients, &mailbox_map2, &email_map2, batch_num)
                        .await;
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
                    .execute::<T>(&clients, &mailbox_map2, &email_map2, term_count as usize)
                    .await;
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
        peer1.set_follower(None).await;
        peer2.set_follower(None).await;

        // Activate all nodes
        let peers = cluster.start_cluster().await;
        assert_leader_elected(&peers).await;
        assert_cluster_updated(&peers).await;
        assert_mirrored_stores(peers.clone(), false).await;

        shutdown_all(peers).await;

        cluster.cleanup();
    }
}
