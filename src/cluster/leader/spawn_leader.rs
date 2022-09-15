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

use crate::cluster::leader::State;
use crate::cluster::log::changes_merge::MergedChanges;
use crate::cluster::log::entries_get::RaftStoreEntries;
use crate::cluster::log::{AppendEntriesRequest, AppendEntriesResponse};
use futures::poll;
use std::task::Poll;
use store::log::raft::{LogIndex, RaftId};
use store::roaring::{RoaringBitmap, RoaringTreemap};
use store::tracing::{debug, error};
use store::Store;
use tokio::sync::watch;

use super::{
    rpc::{Request, Response},
    Cluster,
};
use super::{Event, Peer};

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn spawn_raft_leader(
        &self,
        peer: &Peer,
        mut log_index_rx: watch::Receiver<Event>,
        mut init_rx: Option<watch::Receiver<bool>>,
        max_batch_size: usize,
    ) {
        let peer_tx = peer.tx.clone();
        let mut online_rx = peer.online_rx.clone();
        let peer_name = peer.to_string();
        let peer_id = peer.peer_id;
        let local_name = self.addr.to_string();

        let term = self.term;
        let mut last_log = self.last_log;
        let mut uncommitted_index = self.uncommitted_index;

        let main_tx = self.tx.clone();
        let core = self.core.clone();

        tokio::spawn(async move {
            let mut state = State::BecomeLeader;
            let mut follower_last_index = LogIndex::MAX;

            debug!(
                "[{}] Starting raft leader process for peer {}.",
                local_name, peer_name
            );

            'main: loop {
                // Poll the receiver to make sure this node is still the leader.
                match poll!(Box::pin(log_index_rx.changed())) {
                    Poll::Ready(result) => match result {
                        Ok(_) => {
                            let log_index = *log_index_rx.borrow();
                            last_log.index = log_index.last_log_index;
                            last_log.term = term;
                            uncommitted_index = log_index.uncommitted_index;

                            if matches!(&state, State::Wait) {
                                state = State::AppendLogs {
                                    pending_changes: vec![],
                                };
                            }
                        }
                        Err(_) => {
                            debug!(
                                "[{}] Raft leader process for {} exiting.",
                                local_name, peer_name
                            );
                            break;
                        }
                    },
                    Poll::Pending => (),
                }

                let request = match state {
                    State::BecomeLeader => {
                        state = State::BecomeLeader;
                        Request::BecomeFollower { term, last_log }
                    }
                    State::Synchronize => {
                        state = State::Synchronize;
                        Request::AppendEntries {
                            term,
                            request: AppendEntriesRequest::Synchronize {
                                match_terms: {
                                    match core.get_raft_match_terms().await {
                                        Ok(match_terms) => {
                                            debug_assert!(!match_terms.is_empty());
                                            match_terms
                                        }
                                        Err(err) => {
                                            error!("Error getting raft match list: {:?}", err);
                                            break;
                                        }
                                    }
                                },
                            },
                        }
                    }
                    State::Merge { matched_log } => {
                        state = State::Merge { matched_log };
                        Request::AppendEntries {
                            term,
                            request: AppendEntriesRequest::Merge { matched_log },
                        }
                    }
                    State::Wait => {
                        // Wait for the next change
                        if log_index_rx.changed().await.is_ok() {
                            let log_index = *log_index_rx.borrow();
                            last_log.index = log_index.last_log_index;
                            last_log.term = term;
                            uncommitted_index = log_index.uncommitted_index;
                            debug!("[{}] Received new log index: {:?}", local_name, log_index);
                        } else {
                            debug!(
                                "[{}] Raft leader process for {} exiting.",
                                local_name, peer_name
                            );
                            break;
                        }
                        state = State::AppendLogs {
                            pending_changes: vec![],
                        };
                        continue;
                    }
                    State::AppendLogs { pending_changes } => {
                        debug_assert!(uncommitted_index != LogIndex::MAX);

                        if !pending_changes.is_empty() || follower_last_index != uncommitted_index {
                            let _core = core.clone();
                            match core
                                .spawn_worker(move || {
                                    _core.store.get_log_entries(
                                        follower_last_index,
                                        uncommitted_index,
                                        pending_changes,
                                        max_batch_size,
                                    )
                                })
                                .await
                            {
                                Ok((updates, pending_changes, last_index)) => {
                                    follower_last_index = last_index;
                                    state = State::AppendLogs { pending_changes };
                                    Request::AppendEntries {
                                        term,
                                        request: AppendEntriesRequest::Update {
                                            commit_index: last_log.index,
                                            updates,
                                        },
                                    }
                                }
                                Err(err) => {
                                    error!("Error fetching log entries: {:?}", err);
                                    break;
                                }
                            }
                        } else {
                            debug!(
                                "[{}] No more entries left to send to peer {}.",
                                local_name, peer_name
                            );

                            state = State::Wait;
                            Request::AppendEntries {
                                term,
                                request: AppendEntriesRequest::AdvanceCommitIndex {
                                    commit_index: last_log.index,
                                },
                            }
                        }
                    }
                    State::AppendChanges {
                        account_id,
                        collection,
                        mut changes,
                        is_rollback,
                    } => {
                        match core
                            .prepare_changes(
                                account_id,
                                collection,
                                &mut changes,
                                is_rollback,
                                max_batch_size,
                            )
                            .await
                        {
                            Ok(updates) => {
                                state = State::AppendChanges {
                                    account_id,
                                    collection,
                                    changes,
                                    is_rollback,
                                };
                                Request::AppendEntries {
                                    term,
                                    request: AppendEntriesRequest::Update {
                                        commit_index: last_log.index,
                                        updates,
                                    },
                                }
                            }
                            Err(err) => {
                                error!("Failed to prepare changes: {:?}", err);
                                break;
                            }
                        }
                    }
                    State::AppendBlobs { pending_blob_ids } => {
                        if pending_blob_ids.is_empty() {
                            debug!(
                                "[{}] Peer {} requested blobs but there is nothing else left to send. Aborting.",
                                local_name, peer_name
                            );
                            break;
                        }

                        match core.prepare_blobs(pending_blob_ids, max_batch_size).await {
                            Ok((updates, pending_blob_ids)) => {
                                state = State::AppendBlobs { pending_blob_ids };
                                Request::AppendEntries {
                                    term,
                                    request: AppendEntriesRequest::Update {
                                        commit_index: last_log.index,
                                        updates,
                                    },
                                }
                            }
                            Err(err) => {
                                error!("Failed to prepare blobs: {:?}", err);
                                break;
                            }
                        }
                    }
                };

                let response = if let Some(response) = request.send(&peer_tx).await {
                    match response {
                        Response::StepDown { term: peer_term } => {
                            if let Err(err) = main_tx
                                .send(crate::cluster::Event::StepDown { term: peer_term })
                                .await
                            {
                                error!("Error sending step down message: {}", err);
                            }
                            debug!(
                                "[{}] Peer {} requested this node to step down.",
                                local_name, peer_name
                            );
                            break;
                        }
                        Response::None => {
                            // Wait until the peer is back online
                            debug!(
                                concat!(
                                    "[{}] Could not send message to {}, ",
                                    "waiting until it is confirmed online."
                                ),
                                local_name, peer_name
                            );
                            'online: loop {
                                tokio::select! {
                                    changed = log_index_rx.changed() => {
                                        match changed {
                                            Ok(()) => {
                                                let log_index = *log_index_rx.borrow();
                                                last_log.index = log_index.last_log_index;
                                                last_log.term = term;
                                                uncommitted_index = log_index.uncommitted_index;

                                                debug!(
                                                    "[{}] Received new log index {:?} while waiting for peer {}.",
                                                    local_name, log_index, peer_name
                                                );
                                            }
                                            Err(_) => {
                                                debug!(
                                                    "[{}] Raft leader process for {} exiting.",
                                                    local_name, peer_name
                                                );
                                                break 'main;
                                            }
                                        }
                                    },
                                    online = online_rx.changed() => {
                                        match online {
                                            Ok(()) => {
                                                if *online_rx.borrow() {
                                                    debug!("[{}] Peer {} is back online (rpc).", local_name, peer_name);
                                                    break 'online;
                                                } else {
                                                    debug!("[{}] Peer {} is still offline (rpc).", local_name, peer_name);
                                                    continue 'online;
                                                }
                                            },
                                            Err(_) => {
                                                debug!(
                                                    "[{}] Raft leader process for {} exiting.",
                                                    local_name, peer_name
                                                );
                                                break 'main;
                                            },
                                        }
                                    }
                                };
                            }
                            state = State::BecomeLeader;
                            continue;
                        }
                        Response::UnregisteredPeer => {
                            debug!(
                                "[{}] Peer {} does not know this node, retrying...",
                                local_name, peer_name
                            );
                            state = State::BecomeLeader;
                            continue;
                        }
                        Response::AppendEntries(response) => response,
                        response @ (Response::UpdatePeers { .. }
                        | Response::Vote { .. }
                        | Response::Pong
                        | Response::Command { .. }
                        | Response::Auth { .. }) => {
                            error!(
                                "Unexpected response from peer {}: {:?}",
                                peer_name, response
                            );
                            continue;
                        }
                    }
                } else {
                    debug!(
                        "[{}] Raft leader process for {} exiting (peer_tx channel closed).",
                        local_name, peer_name
                    );
                    break;
                };

                match response {
                    AppendEntriesResponse::Match { match_log } => {
                        if let Some(mut init_rx) = Option::take(&mut init_rx) {
                            debug!(
                                "[{}] Leader process for peer {} waiting for init...",
                                local_name, peer_name
                            );
                            init_rx.changed().await.ok();
                            if !*init_rx.borrow() {
                                error!(
                                    "[{}] Leader failed to init, exiting process for peer {}.",
                                    local_name, peer_name
                                );
                                break;
                            }
                        }

                        follower_last_index = match_log.index;
                        if !match_log.is_none() {
                            let local_match = match core.get_next_raft_id(match_log).await {
                                Ok(Some(local_match)) => local_match,
                                Ok(None) => {
                                    let last_log = core
                                        .get_last_log()
                                        .await
                                        .unwrap_or(None)
                                        .unwrap_or_else(RaftId::none);
                                    error!("Log sync failed: could not match id {:?}, last local log: {:?}.", match_log, last_log);
                                    break;
                                }
                                Err(err) => {
                                    error!("Error getting next raft id: {:?}", err);
                                    break;
                                }
                            };

                            if local_match == match_log {
                                main_tx
                                    .send(crate::cluster::Event::AdvanceCommitIndex {
                                        peer_id,
                                        commit_index: local_match.index,
                                    })
                                    .await
                                    .ok();

                                debug!(
                                    "[{}] Matched index {:?} for peer {}.",
                                    local_name, local_match, peer_name
                                );

                                state = State::AppendLogs {
                                    pending_changes: vec![],
                                };
                            } else {
                                state = State::Synchronize;
                            }
                        } else {
                            debug!(
                                "[{}] Peer {} requested all log entries to be sent.",
                                local_name, peer_name
                            );

                            state = if uncommitted_index != LogIndex::MAX {
                                State::AppendLogs {
                                    pending_changes: vec![],
                                }
                            } else {
                                State::Wait
                            };
                        }
                    }
                    AppendEntriesResponse::Synchronize { match_indexes } => {
                        let matched_log = if !match_indexes.is_empty() {
                            let matched_indexes =
                                match RoaringTreemap::deserialize_from(&match_indexes[..]) {
                                    Ok(match_terms) => match_terms,
                                    Err(err) => {
                                        error!("Error deserializing match list: {:?}", err);
                                        break;
                                    }
                                };

                            if matched_indexes.is_empty() {
                                error!("Log sync failed: match list is empty");
                                break;
                            }

                            match core
                                .get_raft_match_indexes(matched_indexes.min().unwrap())
                                .await
                            {
                                Ok((match_term, mut local_match_indexes)) => {
                                    if local_match_indexes.is_empty() {
                                        error!(
                                            "Log sync failed: Could not find a raft index match."
                                        );
                                        state = State::BecomeLeader;
                                        continue;
                                    }

                                    local_match_indexes &= matched_indexes;

                                    if local_match_indexes.is_empty() {
                                        error!("Log sync failed: Invalid intersection result.");
                                        state = State::BecomeLeader;
                                        continue;
                                    }

                                    debug!(
                                        "[{}] Matched indexes {:?} ({}), term {} for peer {}.",
                                        local_name,
                                        local_match_indexes,
                                        local_match_indexes.max().unwrap(),
                                        match_term,
                                        peer_name
                                    );

                                    RaftId::new(match_term, local_match_indexes.max().unwrap())
                                }
                                Err(err) => {
                                    error!("Error getting local match indexes: {:?}", err);
                                    break;
                                }
                            }
                        } else {
                            RaftId::none()
                        };

                        follower_last_index = matched_log.index;
                        state = State::Merge { matched_log };
                    }
                    AppendEntriesResponse::Continue => (),
                    AppendEntriesResponse::Done { up_to_index } => {
                        // Advance commit index
                        if up_to_index != LogIndex::MAX {
                            main_tx
                                .send(crate::cluster::Event::AdvanceCommitIndex {
                                    peer_id,
                                    commit_index: up_to_index,
                                })
                                .await
                                .ok();

                            if up_to_index == last_log.index {
                                debug!(
                                    "[{}] Follower {} is up to date with leader's commit index {}.",
                                    local_name, peer_name, last_log.index
                                );
                            } else {
                                debug!(
                                    concat!(
                                        "[{}] Updating follower {} index to {} ",
                                        "and sending remaining entries up to index {}."
                                    ),
                                    local_name, peer_name, up_to_index, last_log.index
                                );
                            }
                        } else {
                            debug!(
                                "[{}] Resuming append logs for peer {}.",
                                local_name, peer_name
                            );
                        }

                        state = if up_to_index != uncommitted_index {
                            State::AppendLogs {
                                pending_changes: vec![],
                            }
                        } else {
                            State::Wait
                        };
                    }
                    AppendEntriesResponse::Update {
                        account_id,
                        collection,
                        changes,
                        is_rollback,
                    } => {
                        let mut changes = if let Some(changes) = MergedChanges::from_bytes(&changes)
                        {
                            changes
                        } else {
                            error!("Failed to deserialize changes bitmap.");
                            break;
                        };

                        // Restore deletions.
                        if !changes.deletes.is_empty() {
                            changes.inserts = changes.deletes;
                            changes.deletes = RoaringBitmap::new();
                        }

                        debug!(
                            concat!(
                                "[{}] Peer {} requested {} insertions, ",
                                "{} updates for account {}, collection {:?}."
                            ),
                            local_name,
                            peer_name,
                            changes.inserts.len(),
                            changes.updates.len(),
                            account_id,
                            collection
                        );

                        state = State::AppendChanges {
                            account_id,
                            collection,
                            changes,
                            is_rollback,
                        };
                    }
                    AppendEntriesResponse::FetchBlobs { blob_ids } => {
                        state = State::AppendBlobs {
                            pending_blob_ids: blob_ids,
                        };
                    }
                }
            }
        });
    }
}
