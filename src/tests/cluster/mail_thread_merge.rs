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
use jmap_client::mailbox::Role;
use store::Store;

use crate::JMAPServer;

use crate::tests::cluster::utils::{
    activate_all_peers, assert_cluster_updated, assert_leader_elected, assert_mirrored_stores,
    compact_log, shutdown_all, Clients, Cluster,
};
use crate::tests::jmap_mail::email_thread_merge::build_thread_test_messages;

pub async fn test<T>()
where
    T: for<'x> Store<'x> + 'static,
{
    let mut cluster = Cluster::<T>::new("st_cluster_log_merge", 5, true).await;
    let peers = cluster.start_cluster().await;

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

    // Connect clients
    let clients = Arc::new(Clients::new(5).await);

    let inbox_id = clients
        .insert_mailbox(0, 1, "Inbox".to_string(), Role::None)
        .await;

    //leader.commit_last_index().await;
    assert_cluster_updated(&peers).await;

    // Keep one peer down to test full sync at the end
    leader.set_offline(true, true).await;

    // Insert chunks of ten messages in different nodes
    let mut prev_offline_leader: Option<&web::Data<JMAPServer<T>>> = None;
    while !messages.is_empty() {
        let chunk = messages
            .drain(0..std::cmp::min(20, messages.len()))
            .collect::<Vec<_>>();

        let leader = assert_leader_elected(&peers).await;
        for raw_message in chunk {
            clients
                .insert_email(
                    0,
                    1,
                    raw_message.into_bytes(),
                    vec![inbox_id.to_string()],
                    vec![],
                )
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
    activate_all_peers(&peers).await;
    assert_cluster_updated(&peers).await;
    assert_mirrored_stores(peers.clone(), false).await;

    // Add a new peer and send a snapshot to it.
    compact_log(peers.clone()).await;
    let peers = cluster.extend_cluster("st_cluster", peers, 1).await;
    assert_cluster_updated(&peers).await;
    assert_mirrored_stores(peers.clone(), false).await;

    // Stop cluster
    cluster.stop_cluster().await;
    shutdown_all(peers).await;
    cluster.cleanup();
}
