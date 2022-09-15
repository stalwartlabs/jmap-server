/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use std::sync::Arc;

use actix_web::web;
use jmap_client::{
    client::{Client, Credentials},
    email::{query::Filter, Property},
};
use store::{ahash::AHashMap, parking_lot::Mutex, Store};

use crate::{
    tests::{
        cluster::utils::{
            activate_all_peers, assert_cluster_updated, assert_leader_elected,
            assert_mirrored_stores, compact_log, find_online_follower, shutdown_all, test_batch,
            Clients, Cluster,
        },
        jmap_mail::lmtp::{AssertResult, SmtpConnection},
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

    // Create test principals
    assert_leader_elected(&peers).await;
    let client = &clients.clients[0];
    client.domain_create("example.com").await.unwrap();
    let account_id_1 = client
        .individual_create("jdoe@example.com", "12345", "John Doe")
        .await
        .unwrap()
        .take_id();
    let account_id_2 = client
        .individual_create("jane@example.com", "abcde", "Jane Doe")
        .await
        .unwrap()
        .take_id();
    client
        .list_create(
            "members@example.com",
            "Mailing List",
            [&account_id_1, &account_id_2],
        )
        .await
        .unwrap()
        .take_id();

    // Connect to one of the followers and test read replicas
    // Disable redirects to avoid the request from being redirected to the leader
    let follower_id = find_online_follower(&peers) + 1;
    let mut follower_client = Client::new()
        .credentials(Credentials::bearer("DO_NOT_ATTEMPT_THIS_AT_HOME"))
        .connect(&format!("http://127.0.0.1:{}", 8000 + follower_id))
        .await
        .unwrap();
    assert_eq!(
        follower_client
            .principal_get(&account_id_1, None::<Vec<_>>)
            .await
            .unwrap()
            .unwrap()
            .email()
            .unwrap(),
        "jdoe@example.com"
    );

    // LMTP requests should be forwarded to the leader over RPC
    let mut lmtp = SmtpConnection::connect_peer(follower_id).await;
    lmtp.expn("members@example.com", 2)
        .await
        .assert_contains("jdoe@example.com")
        .assert_contains("jane@example.com");
    lmtp.vrfy("jdoe@example.com", 2).await;
    lmtp.ingest(
        "bill@otherdomain.com",
        &["jdoe@example.com"],
        concat!(
            "From: bill@otherdomain.com\r\n",
            "To: jdoe@example.com\r\n",
            "Subject: TPS Report\r\n",
            "\r\n",
            "I'm going to need those TPS reports ASAP. ",
            "So, if you could do that, that'd be great."
        ),
    )
    .await;
    lmtp.quit().await;
    assert_cluster_updated(&peers).await;

    // Make sure the message was delivered
    let mut ids = follower_client
        .set_default_account_id(&account_id_1)
        .email_query(None::<Filter>, None::<Vec<_>>)
        .await
        .unwrap()
        .take_ids();
    assert_eq!(ids.len(), 1);
    assert_eq!(
        follower_client
            .email_get(&ids.pop().unwrap(), [Property::Subject].into())
            .await
            .unwrap()
            .unwrap()
            .subject()
            .unwrap(),
        "TPS Report"
    );

    assert_cluster_updated(&peers).await;
    assert_mirrored_stores(peers.clone(), true).await;

    // Test create, update, delete operations
    let mut prev_offline_leader: Option<&web::Data<JMAPServer<T>>> = None;
    let mailbox_map = Arc::new(Mutex::new(AHashMap::new()));
    let email_map = Arc::new(Mutex::new(AHashMap::new()));

    for (batch_num, batch) in test_batch().into_iter().enumerate() {
        let leader = assert_leader_elected(&peers).await;
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
