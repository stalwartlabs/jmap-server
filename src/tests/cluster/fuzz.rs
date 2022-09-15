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

use crate::tests::cluster::utils::{assert_mirrored_stores, num_online_peers, Ac};

use jmap_client::mailbox::Role;
use std::{sync::Arc, time::Duration};
use store::ahash::AHashMap;
use store::core::JMAPIdPrefix;
use store::rand::{self, Rng};
use store::{core::collection::Collection, parking_lot::Mutex, AccountId, JMAPId, Store};
use tokio::time::sleep;

use super::utils::{
    activate_all_peers, assert_cluster_updated, assert_leader_elected, shutdown_all, Clients,
    Cluster, Cmd, Cmds,
};

#[allow(clippy::type_complexity)]
pub async fn test<T>(mut replay_cmds: Vec<Cmd>)
where
    T: for<'x> Store<'x> + 'static,
{
    let is_replay = !replay_cmds.is_empty();
    if is_replay {
        println!("Replaying {} commands...", replay_cmds.len());
    } else {
        println!("Fuzzing cluster...");
    }

    let mut cluster = Cluster::<T>::new("st_cluster_fuzz", 5, true).await;
    let peers = cluster.start_cluster().await;
    let mut actions = Cmds::default();
    let id_map: Arc<Mutex<AHashMap<(AccountId, Collection), AHashMap<JMAPId, String>>>> =
        Arc::new(Mutex::new(AHashMap::new()));
    let mut id_seq = 0;
    let mut change_seq = 0;

    let get_random_id = |account_id,
                         collection,
                         id_map: &AHashMap<(AccountId, Collection), AHashMap<JMAPId, String>>|
     -> Option<(JMAPId, String)> {
        let ids = id_map.get(&(account_id, collection))?;
        let ids_num = ids.len();
        if ids_num > 0 {
            ids.iter()
                .nth(if ids_num > 1 {
                    rand::thread_rng().gen_range::<usize, _>(0..ids_num)
                } else {
                    0
                })
                .map(|(id1, id2)| (*id1, id2.to_string()))
        } else {
            None
        }
    };

    assert_leader_elected(&peers).await;
    assert_cluster_updated(&peers).await;

    // Connect clients
    let clients = Arc::new(Clients::new(5).await);

    loop {
        let cmd = if !is_replay {
            match rand::thread_rng().gen_range::<i32, _>(0..15) {
                0 => Cmd::StopLeader,
                1 => Cmd::StopFollower,
                2 => Cmd::StartOneOffline,
                3..=6 => Cmd::StartAllOffline,
                _ => {
                    let account_id = rand::thread_rng().gen_range::<AccountId, _>(1..=5);

                    Cmd::Update {
                        account_id,
                        action: match rand::thread_rng().gen_range::<i32, _>(0..6) {
                            0 => {
                                let mailbox_id = if let Some((local_id, _)) =
                                    get_random_id(account_id, Collection::Mailbox, &id_map.lock())
                                {
                                    local_id
                                } else {
                                    0
                                };
                                id_seq += 1;
                                Ac::NewEmail((mailbox_id, id_seq as JMAPId))
                            }
                            1 => {
                                if let Some((local_id, _)) =
                                    get_random_id(account_id, Collection::Mail, &id_map.lock())
                                {
                                    Ac::UpdateEmail(local_id)
                                } else {
                                    continue;
                                }
                            }
                            2 => {
                                if let Some((local_id, _)) =
                                    get_random_id(account_id, Collection::Mail, &id_map.lock())
                                {
                                    Ac::DeleteEmail(local_id)
                                } else {
                                    continue;
                                }
                            }
                            3 => {
                                id_seq += 1;
                                Ac::InsertMailbox(id_seq as JMAPId)
                            }
                            4 => {
                                if let Some((local_id, _)) =
                                    get_random_id(account_id, Collection::Mailbox, &id_map.lock())
                                {
                                    Ac::UpdateMailbox(local_id)
                                } else {
                                    continue;
                                }
                            }
                            5 => {
                                if let Some((local_id, _)) =
                                    get_random_id(account_id, Collection::Mailbox, &id_map.lock())
                                {
                                    Ac::DeleteMailbox(local_id)
                                } else {
                                    continue;
                                }
                            }
                            _ => unreachable!(),
                        },
                    }
                }
            }
        } else {
            if replay_cmds.is_empty() {
                break;
            }
            let cmd = replay_cmds.remove(0);
            if matches!(
                &cmd,
                Cmd::Update {
                    action: Ac::InsertMailbox(_) | Ac::NewEmail(_),
                    ..
                }
            ) {
                id_seq += 1;
            }
            cmd
        };

        let mut success = false;

        println!("{:?} (seq {})", cmd, change_seq);
        actions.cmds.push(cmd);

        match actions.cmds.last().unwrap() {
            Cmd::StopLeader => {
                for peer in peers.iter() {
                    if peer.is_leader() && !peer.is_offline() {
                        peer.set_offline(true, true).await;
                        success = true;
                        break;
                    }
                }
            }
            Cmd::StopFollower => {
                for peer in peers.iter() {
                    if !peer.is_leader() && !peer.is_offline() {
                        peer.set_offline(true, true).await;
                        success = true;
                        break;
                    }
                }
            }
            Cmd::StartOneOffline => {
                for peer in peers.iter() {
                    if peer.is_offline() {
                        peer.set_offline(false, true).await;
                        success = true;
                        break;
                    }
                }
            }
            Cmd::StartAllOffline => {
                for peer in peers.iter() {
                    if peer.is_offline() {
                        peer.set_offline(false, true).await;
                        success = true;
                    }
                }
            }
            Cmd::Update { account_id, action } => {
                if num_online_peers(&peers) < 3 {
                    if is_replay {
                        panic!("No quorum to execute {:?}.", actions.cmds.last().unwrap());
                    } else {
                        if matches!(action, Ac::InsertMailbox(_) | Ac::NewEmail(_)) {
                            id_seq -= 1;
                        }
                        println!(
                            "Skipping {:?} due to no quorum.",
                            actions.cmds.pop().unwrap()
                        );
                    }
                    continue;
                }

                let mut leader = None;
                'o: for _ in 0..100 {
                    for (peer_num, peer) in peers.iter().enumerate() {
                        if !peer.is_offline() && peer.is_leader() {
                            leader = peer.into();

                            for (pos, peer) in peers.iter().enumerate() {
                                // Clients might try to contact an "offline" peer, redirect them
                                // to the right leader.
                                if pos == peer_num {
                                    println!("Peer {} is leading.", peer_num + 1);
                                } else {
                                    println!(
                                        "Peer {} has {:?}",
                                        pos + 1,
                                        peer.cluster.as_ref().unwrap().leader_hostname.lock()
                                    );
                                }
                                if pos != peer_num && peer.is_offline() {
                                    *peer.cluster.as_ref().unwrap().leader_hostname.lock() =
                                        format!("http://127.0.0.1:{}", 8000 + peer_num + 1).into();
                                }
                            }
                            break 'o;
                        }
                    }
                    sleep(Duration::from_millis(100)).await;
                }
                if leader.is_none() {
                    panic!(
                        "No elected leader to execute {:?}.",
                        actions.cmds.last().unwrap()
                    );
                };

                if matches!(action, Ac::NewEmail((mailbox_id, _)) if mailbox_id == &0) {
                    id_seq += 1;
                }
                change_seq += 1;

                let action = action.clone();
                let id_map = id_map.clone();
                let account_id = *account_id;

                match action {
                    Ac::NewEmail((local_mailbox_id, local_id)) => {
                        let (local_mailbox_id, local_id) = if local_mailbox_id == 0 {
                            let mailbox_id = clients
                                .insert_mailbox(
                                    0,
                                    account_id,
                                    format!("Mailbox {}", id_seq),
                                    Role::None,
                                )
                                .await;

                            id_map
                                .lock()
                                .entry((account_id, Collection::Mailbox))
                                .or_insert_with(AHashMap::new)
                                .insert(id_seq as JMAPId, mailbox_id);
                            (id_seq, JMAPId::from_parts(id_seq as u32, local_id as u32))
                        } else {
                            (
                                local_mailbox_id,
                                JMAPId::from_parts(local_mailbox_id as u32, local_id as u32),
                            )
                        };

                        let store_mailbox_id = id_map
                            .lock()
                            .get(&(account_id, Collection::Mailbox))
                            .unwrap()
                            .get(&local_mailbox_id)
                            .unwrap()
                            .to_string();

                        let email_id = clients
                            .insert_email(
                                0,
                                account_id,
                                format!(
                                    "From: test@test.com\nSubject: test {}\n\nTest message {}",
                                    local_id, local_id
                                )
                                .into_bytes(),
                                vec![store_mailbox_id],
                                vec![],
                            )
                            .await;

                        id_map
                            .lock()
                            .entry((account_id, Collection::Mail))
                            .or_insert_with(AHashMap::new)
                            .insert(local_id, email_id);
                    }
                    Ac::UpdateEmail(local_id) => {
                        let email_id = id_map
                            .lock()
                            .get(&(account_id, Collection::Mail))
                            .unwrap()
                            .get(&local_id)
                            .unwrap()
                            .to_string();
                        clients
                            .update_email(
                                0,
                                account_id,
                                email_id,
                                None,
                                Some(vec![format!("tag_{}", change_seq)]),
                            )
                            .await;
                    }
                    Ac::DeleteEmail(local_id) => {
                        let email_id = id_map
                            .lock()
                            .get_mut(&(account_id, Collection::Mail))
                            .unwrap()
                            .remove(&local_id)
                            .unwrap();
                        clients.delete_email(0, account_id, email_id).await;
                    }
                    Ac::InsertMailbox(local_id) => {
                        let mailbox_id = clients
                            .insert_mailbox(
                                0,
                                account_id,
                                format!("Mailbox {}", local_id),
                                Role::None,
                            )
                            .await;

                        id_map
                            .lock()
                            .entry((account_id, Collection::Mailbox))
                            .or_insert_with(AHashMap::new)
                            .insert(local_id, mailbox_id);
                    }
                    Ac::UpdateMailbox(local_id) => {
                        let mailbox_id = id_map
                            .lock()
                            .get(&(account_id, Collection::Mailbox))
                            .unwrap()
                            .get(&local_id)
                            .unwrap()
                            .to_string();
                        clients
                            .update_mailbox(0, account_id, mailbox_id, local_id as u32, change_seq)
                            .await;
                    }
                    Ac::DeleteMailbox(local_id) => {
                        if let Some(ids) = id_map.lock().get_mut(&(account_id, Collection::Mail)) {
                            if !ids.is_empty() {
                                let del_keys: Vec<JMAPId> = ids
                                    .keys()
                                    .filter(|key| (*key).get_prefix_id() == local_id as u32)
                                    .copied()
                                    .collect();
                                for del_key in del_keys {
                                    ids.remove(&del_key);
                                }
                            }
                        }

                        let mailbox_id = id_map
                            .lock()
                            .get_mut(&(account_id, Collection::Mailbox))
                            .unwrap()
                            .remove(&local_id)
                            .unwrap();

                        clients.delete_mailbox(0, account_id, mailbox_id).await;
                    }
                }

                success = true;

                assert_cluster_updated(&peers).await;
                assert_mirrored_stores(peers.clone(), true).await;
            }
        }

        if !success {
            if is_replay {
                panic!("Failed to execute {:?}", actions.cmds.last().unwrap());
            } else {
                actions.cmds.pop();
            }
        } else if !matches!(actions.cmds.last().unwrap(), Cmd::Update { .. }) {
            sleep(Duration::from_millis(2000)).await;
        }
    }

    // Activate all nodes
    activate_all_peers(&peers).await;
    assert_cluster_updated(&peers).await;
    assert_mirrored_stores(peers.clone(), false).await;
    shutdown_all(peers).await;
    actions.clean_exit = true;

    cluster.cleanup();
}
