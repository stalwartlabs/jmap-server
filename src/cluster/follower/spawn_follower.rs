use super::Cluster;
use super::IPC_CHANNEL_BUFFER;
use crate::cluster::follower::{RaftIndexes, State};
use crate::cluster::log::{AppendEntriesRequest, Event};
use store::ahash::AHashMap;
use store::log::raft::LogIndex;
use store::tracing::{debug, error};
use store::Store;
use tokio::sync::mpsc;

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn spawn_raft_follower(&self) -> mpsc::Sender<Event> {
        let (tx, mut rx) = mpsc::channel::<Event>(IPC_CHANNEL_BUFFER);
        let core = self.core.clone();
        let local_name = self.addr.to_string();

        debug!("[{}] Starting raft follower process.", local_name);

        tokio::spawn(async move {
            if let Err(err) = core.commit_leader(LogIndex::MAX, true).await {
                error!("Failed to rollback uncommitted entries: {:?}", err);
                return;
            }

            if let Err(err) = core.commit_follower(LogIndex::MAX, true).await {
                error!("Failed to commit pending updates: {:?}", err);
                return;
            }

            let mut indexes = {
                let commit_index = match core.set_follower_commit_index().await {
                    Ok(commit_index) => commit_index,
                    Err(err) => {
                        error!("Failed to set follower commit index: {:?}", err);
                        return;
                    }
                };
                RaftIndexes {
                    leader_commit_index: LogIndex::MAX,
                    commit_index,
                    uncommitted_index: commit_index,
                    merge_index: LogIndex::MAX,
                    sequence_id: 0,
                }
            };

            let mut state = match core.next_rollback_change().await {
                Ok(Some((account_id, collection, changes))) => State::Rollback {
                    account_id,
                    collection,
                    changes,
                },
                Ok(None) => State::default(),
                Err(err) => {
                    error!("Failed to obtain rollback changes: {:?}", err);
                    return;
                }
            };

            while let Some(event) = rx.recv().await {
                let response = match (event.request, state) {
                    (AppendEntriesRequest::Match { last_log }, State::Synchronize) => {
                        if let Some(response) = core.handle_match_log(last_log).await {
                            state = State::Synchronize;
                            response
                        } else {
                            break;
                        }
                    }

                    (AppendEntriesRequest::Synchronize { match_terms }, State::Synchronize) => {
                        if let Some(response) = core.handle_synchronize_log(match_terms).await {
                            state = State::Synchronize;
                            response
                        } else {
                            break;
                        }
                    }

                    (AppendEntriesRequest::Merge { matched_log }, State::Synchronize) => {
                        if let Some((next_state, response)) =
                            core.handle_merge_log(matched_log).await
                        {
                            state = next_state;
                            response
                        } else {
                            break;
                        }
                    }
                    (
                        AppendEntriesRequest::Update {
                            commit_index,
                            updates,
                        },
                        State::Synchronize,
                    ) => {
                        debug!(
                            "[{}] Received {} log entries with commit index {} (sync state).",
                            local_name,
                            updates.len(),
                            commit_index,
                        );
                        indexes.leader_commit_index = commit_index;
                        indexes.merge_index = LogIndex::MAX;
                        core.set_up_to_date(false);

                        if let Some((next_state, response)) = core
                            .handle_update_log(&mut indexes, AHashMap::default(), updates)
                            .await
                        {
                            state = next_state;
                            response
                        } else {
                            break;
                        }
                    }

                    (
                        AppendEntriesRequest::Update {
                            commit_index,
                            updates,
                        },
                        State::AppendEntries { changed_accounts },
                    ) => {
                        debug!(
                            concat!(
                                "[{}] Received {} log entries with commit index {}: ",
                                "{} pending accounts."
                            ),
                            local_name,
                            updates.len(),
                            commit_index,
                            changed_accounts.len()
                        );

                        core.set_up_to_date(false);
                        indexes.leader_commit_index = commit_index;

                        if let Some((next_state, response)) = core
                            .handle_update_log(&mut indexes, changed_accounts, updates)
                            .await
                        {
                            state = next_state;
                            response
                        } else {
                            break;
                        }
                    }

                    (
                        AppendEntriesRequest::Update {
                            commit_index,
                            updates,
                        },
                        State::AppendChanges { changed_accounts },
                    ) => {
                        debug!(
                            concat!(
                                "[{}] Received {} changes with commit index {}: ",
                                "{} pending accounts."
                            ),
                            local_name,
                            updates.len(),
                            commit_index,
                            changed_accounts.len()
                        );
                        indexes.leader_commit_index = commit_index;

                        if let Some((next_state, response)) = core
                            .check_pending_updates(&mut indexes, changed_accounts, updates)
                            .await
                        {
                            state = next_state;
                            response
                        } else {
                            break;
                        }
                    }

                    (
                        AppendEntriesRequest::Update {
                            commit_index,
                            updates,
                        },
                        State::AppendBlobs {
                            pending_blobs,
                            pending_updates,
                            changed_accounts,
                        },
                    ) => {
                        debug!(
                            concat!(
                                "[{}] Received {} blobs with commit index {}: ",
                                "{} pending accounts."
                            ),
                            local_name,
                            updates.len(),
                            commit_index,
                            changed_accounts.len()
                        );
                        indexes.leader_commit_index = commit_index;

                        if let Some((next_state, response)) = core
                            .handle_missing_blobs(
                                &mut indexes,
                                changed_accounts,
                                pending_blobs,
                                pending_updates,
                                updates,
                            )
                            .await
                        {
                            state = next_state;
                            response
                        } else {
                            break;
                        }
                    }

                    (
                        AppendEntriesRequest::Update {
                            updates,
                            commit_index,
                        },
                        State::Rollback {
                            account_id,
                            collection,
                            changes,
                        },
                    ) => {
                        debug!(
                            concat!(
                                "[{}] Received {} rollback entries for account {}, ",
                                "collection {:?}."
                            ),
                            local_name,
                            updates.len(),
                            account_id,
                            collection
                        );
                        indexes.leader_commit_index = commit_index;

                        if let Some((next_state, response)) = core
                            .handle_rollback_updates(account_id, collection, changes, updates)
                            .await
                        {
                            state = next_state;
                            response
                        } else {
                            break;
                        }
                    }

                    (AppendEntriesRequest::AdvanceCommitIndex { commit_index }, prev_state) => {
                        indexes.leader_commit_index = commit_index;
                        if let Some((_, response)) = core.commit_updates(&mut indexes).await {
                            state = prev_state;
                            response
                        } else {
                            break;
                        }
                    }

                    (
                        _,
                        State::Rollback {
                            account_id,
                            collection,
                            changes,
                        },
                    ) => {
                        debug!(
                            concat!(
                                "[{}] Resuming rollback for account {}, ",
                                "collection {:?}."
                            ),
                            local_name, account_id, collection
                        );

                        // Resume rollback process when a new leader is elected.
                        if let Some((next_state, response)) = core
                            .handle_rollback_updates(account_id, collection, changes, vec![])
                            .await
                        {
                            state = next_state;
                            response
                        } else {
                            break;
                        }
                    }
                    (_, _) => {
                        unreachable!("Invalid state.");
                    }
                };

                event
                    .response_tx
                    .send(response)
                    .unwrap_or_else(|_| error!("Oneshot response channel closed."));
            }

            debug!("[{}] Raft follower process ended.", local_name);
        });
        tx
    }
}
