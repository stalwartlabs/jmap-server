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

use std::{fs, sync::Arc};

use store::{ahash::AHashMap, parking_lot::Mutex, Store};

use crate::tests::{
    cluster::utils::{
        assert_cluster_updated, assert_leader_elected, assert_mirrored_stores, shutdown_all,
        test_batch, Ac, Clients, Cluster,
    },
    jmap::init_jmap_tests_opts,
    store::utils::{destroy_temp_dir, make_temp_dir},
};

#[allow(clippy::comparison_chain)]
pub async fn test<T>()
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Testing log conflict resolution...");

    for conflict_term in 14..30 {
        println!("Causing a log conflict on term {}...", conflict_term);

        // Dirty hack as Actix is not dropping RocksDB properly and the lock is held
        // between tests --  this should be fixed in the future.
        let base_dir_1 = format!("st_conf_{}", conflict_term);
        let base_dir_2 = format!("st_conf_{}_c", conflict_term);
        let tmp_next_path_1 = make_temp_dir(&base_dir_1, 1);
        let tmp_next_path_2 = make_temp_dir(&base_dir_1, 2);

        destroy_temp_dir(&tmp_next_path_1);
        destroy_temp_dir(&tmp_next_path_2);

        {
            let (peer1, client1, tmp_path_1, handle1) =
                init_jmap_tests_opts::<T>(&base_dir_2, 1, 1, true).await;
            let (peer2, client2, tmp_path_2, handle2) =
                init_jmap_tests_opts::<T>(&base_dir_2, 2, 1, true).await;

            let clients1 = Clients {
                clients: vec![client1],
            };
            let clients2 = Clients {
                clients: vec![client2],
            };

            let mailbox_map1 = Arc::new(Mutex::new(AHashMap::new()));
            let mailbox_map2 = Arc::new(Mutex::new(AHashMap::new()));
            let email_map1 = Arc::new(Mutex::new(AHashMap::new()));
            let email_map2 = Arc::new(Mutex::new(AHashMap::new()));

            let mut term_count = 0;

            for (batch_num, batch) in test_batch().into_iter().enumerate() {
                for action in batch {
                    term_count += 1;

                    peer1.set_leader_term(term_count).await;
                    action
                        .execute::<T>(&clients1, &mailbox_map1, &email_map1, batch_num)
                        .await;

                    if term_count < conflict_term {
                        peer2.set_leader_term(term_count).await;
                        action
                            .execute::<T>(&clients2, &mailbox_map2, &email_map2, batch_num)
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
                        .execute::<T>(&clients2, &mailbox_map2, &email_map2, term_count as usize)
                        .await;
                    }
                }
            }

            for (peer, handle) in [(&peer1, &handle1), (&peer2, &handle2)] {
                peer.set_leader_commit_index(peer.get_last_log().await.unwrap().unwrap().index)
                    .await
                    .unwrap();
                handle.stop(true).await;
                peer.store.db.close().unwrap();
            }

            drop(peer1);
            drop(peer2);

            fs::rename(&tmp_path_1, &tmp_next_path_1).unwrap();
            fs::rename(&tmp_path_2, &tmp_next_path_2).unwrap();
        }

        // Activate all nodes
        let mut cluster = Cluster::<T>::new(&base_dir_1, 2, false).await;
        let peers = cluster.start_cluster().await;
        assert_leader_elected(&peers).await;
        assert_cluster_updated(&peers).await;
        assert_mirrored_stores(peers.clone(), false).await;

        // Stop cluster
        cluster.stop_cluster().await;
        shutdown_all(peers).await;
        cluster.cleanup();
    }
}
