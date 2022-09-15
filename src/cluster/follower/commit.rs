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

use crate::cluster::log::update_apply::RaftStoreApplyUpdate;
use crate::cluster::log::{PendingUpdate, PendingUpdates};
use crate::JMAPServer;
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::log::entry::Entry;
use store::log::raft::{LogIndex, RaftId, TermId};
use store::serialize::key::{LogKey, FOLLOWER_COMMIT_INDEX_KEY};
use store::serialize::{DeserializeBigEndian, StoreDeserialize, StoreSerialize};
use store::write::batch::WriteBatch;
use store::write::operation::WriteOperation;
use store::{tracing::debug, AccountId, ColumnFamily, Direction, Store};

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn set_follower_commit_index(&self) -> store::Result<LogIndex> {
        let store = self.store.clone();
        self.spawn_worker(move || {
            let last_index = store
                .get_prev_raft_id(RaftId::new(TermId::MAX, LogIndex::MAX))?
                .map(|v| v.index)
                .unwrap_or(LogIndex::MAX);
            store.db.set(
                ColumnFamily::Values,
                FOLLOWER_COMMIT_INDEX_KEY,
                &last_index.serialize().unwrap(),
            )?;
            Ok(last_index)
        })
        .await
    }

    pub async fn commit_follower(
        &self,
        apply_up_to: LogIndex,
        do_reset: bool,
    ) -> store::Result<Option<RaftId>> {
        let store = self.store.clone();
        self.spawn_worker(move || {
            let apply_up_to: LogIndex = if apply_up_to != LogIndex::MAX {
                store.db.set(
                    ColumnFamily::Values,
                    FOLLOWER_COMMIT_INDEX_KEY,
                    &apply_up_to.serialize().unwrap(),
                )?;
                apply_up_to
            } else if let Some(apply_up_to) = store
                .db
                .get(ColumnFamily::Values, FOLLOWER_COMMIT_INDEX_KEY)?
            {
                apply_up_to
            } else {
                return Ok(None);
            };

            debug!(
                "Applying pending follower updates up to index {}.",
                apply_up_to
            );

            let mut log_batch = Vec::new();
            for (key, value) in store.db.iterator(
                ColumnFamily::Logs,
                &[LogKey::PENDING_UPDATES_KEY_PREFIX],
                Direction::Forward,
            )? {
                if !key.starts_with(&[LogKey::PENDING_UPDATES_KEY_PREFIX]) {
                    break;
                }
                let index = (&key[..]).deserialize_be_u64(1).ok_or_else(|| {
                    StoreError::InternalError(format!(
                        "Failed to deserialize index from changelog key: [{:?}]",
                        key
                    ))
                })?;

                if apply_up_to != LogIndex::MAX && index <= apply_up_to {
                    let mut write_batch = WriteBatch::new(AccountId::MAX);
                    let mut account_id = AccountId::MAX;
                    let mut collection = Collection::None;

                    for update in PendingUpdates::deserialize(&value)
                        .ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Failed to deserialize pending updates for key [{:?}]",
                                key
                            ))
                        })?
                        .updates
                    {
                        match update {
                            PendingUpdate::Begin {
                                account_id: update_account_id,
                                collection: update_collection,
                            } => {
                                account_id = update_account_id;
                                collection = update_collection;
                            }
                            PendingUpdate::Update { update } => {
                                debug_assert!(
                                    account_id != AccountId::MAX && collection != Collection::None
                                );

                                if account_id != write_batch.account_id {
                                    if !write_batch.is_empty() {
                                        store.write(write_batch)?;
                                        write_batch = WriteBatch::new(account_id);
                                    } else {
                                        write_batch.account_id = account_id;
                                    }
                                }
                                store.apply_update(&mut write_batch, collection, update)?;
                            }
                            PendingUpdate::Delete { document_ids } => {
                                debug_assert!(
                                    account_id != AccountId::MAX && collection != Collection::None
                                );

                                if account_id != write_batch.account_id {
                                    if !write_batch.is_empty() {
                                        store.write(write_batch)?;
                                        write_batch = WriteBatch::new(account_id);
                                    } else {
                                        write_batch.account_id = account_id;
                                    }
                                }

                                for document_id in document_ids {
                                    match store.delete_document(
                                        &mut write_batch,
                                        collection,
                                        document_id,
                                    ) {
                                        Ok(_) | Err(StoreError::NotFound(_)) => {}
                                        Err(e) => return Err(e),
                                    }
                                }
                            }
                        }
                    }

                    if !write_batch.is_empty() {
                        store.write(write_batch)?;
                    }

                    store.db.delete(ColumnFamily::Logs, &key)?;
                } else if do_reset {
                    log_batch.push(WriteOperation::Delete {
                        cf: ColumnFamily::Logs,
                        key: key.to_vec(),
                    });
                } else {
                    break;
                }
            }

            if !do_reset {
                debug_assert!(apply_up_to != LogIndex::MAX);
                if let Some((key, _)) = store
                    .db
                    .iterator(
                        ColumnFamily::Logs,
                        &LogKey::serialize_raft(&RaftId::new(0, apply_up_to)),
                        Direction::Forward,
                    )?
                    .next()
                {
                    if key.starts_with(&[LogKey::RAFT_KEY_PREFIX]) {
                        let raft_id = LogKey::deserialize_raft(&key).ok_or_else(|| {
                            StoreError::InternalError(format!("Corrupted raft key for [{:?}]", key))
                        })?;
                        if raft_id.index == apply_up_to {
                            return Ok(raft_id.into());
                        }
                    }
                }
            } else {
                let key = LogKey::serialize_raft(&RaftId::new(
                    0,
                    if apply_up_to != LogIndex::MAX {
                        apply_up_to
                    } else {
                        0
                    },
                ));
                log_batch.push(WriteOperation::Delete {
                    cf: ColumnFamily::Values,
                    key: FOLLOWER_COMMIT_INDEX_KEY.to_vec(),
                });

                for (key, value) in
                    store
                        .db
                        .iterator(ColumnFamily::Logs, &key, Direction::Forward)?
                {
                    if !key.starts_with(&[LogKey::RAFT_KEY_PREFIX]) {
                        break;
                    }
                    let raft_id = LogKey::deserialize_raft(&key).ok_or_else(|| {
                        StoreError::InternalError(format!("Corrupted raft key for [{:?}]", key))
                    })?;
                    if apply_up_to == LogIndex::MAX || raft_id.index > apply_up_to {
                        match Entry::deserialize(&value).ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Corrupted raft entry for [{:?}]",
                                key
                            ))
                        })? {
                            Entry::Item {
                                account_id,
                                changed_collections,
                            } => {
                                for changed_collection in changed_collections {
                                    log_batch.push(WriteOperation::Delete {
                                        cf: ColumnFamily::Logs,
                                        key: LogKey::serialize_change(
                                            account_id,
                                            changed_collection,
                                            raft_id.index,
                                        ),
                                    });
                                }
                            }
                            Entry::Snapshot { changed_accounts } => {
                                for (changed_collections, changed_accounts_ids) in changed_accounts
                                {
                                    for changed_collection in changed_collections {
                                        for changed_account_id in &changed_accounts_ids {
                                            log_batch.push(WriteOperation::Delete {
                                                cf: ColumnFamily::Logs,
                                                key: LogKey::serialize_change(
                                                    *changed_account_id,
                                                    changed_collection,
                                                    raft_id.index,
                                                ),
                                            });
                                        }
                                    }
                                }
                            }
                        };

                        log_batch.push(WriteOperation::Delete {
                            cf: ColumnFamily::Logs,
                            key: key.to_vec(),
                        });
                    }
                }

                if !log_batch.is_empty() {
                    store.db.write(log_batch)?;
                }
            }

            Ok(None)
        })
        .await
    }
}
