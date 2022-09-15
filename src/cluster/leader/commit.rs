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

use crate::cluster::log::rollback_prepare::RaftStoreRollbackPrepare;
use crate::JMAPServer;
use store::bincode;
use store::core::document::Document;
use store::core::error::StoreError;
use store::log::raft::LogIndex;
use store::serialize::key::{LogKey, LEADER_COMMIT_INDEX_KEY};
use store::serialize::{DeserializeBigEndian, StoreSerialize};
use store::write::batch::WriteBatch;
use store::write::operation::WriteOperation;
use store::{tracing::debug, ColumnFamily, Direction, Store};

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn set_leader_commit_index(&self, commit_index: LogIndex) -> store::Result<()> {
        let store = self.store.clone();
        self.spawn_worker(move || {
            store.db.set(
                ColumnFamily::Values,
                LEADER_COMMIT_INDEX_KEY,
                &commit_index.serialize().unwrap(),
            )
        })
        .await
    }

    pub async fn commit_leader(&self, apply_up_to: LogIndex, do_reset: bool) -> store::Result<()> {
        let store = self.store.clone();
        self.spawn_worker(move || {
            let apply_up_to: LogIndex = if apply_up_to != LogIndex::MAX {
                store.db.set(
                    ColumnFamily::Values,
                    LEADER_COMMIT_INDEX_KEY,
                    &apply_up_to.serialize().unwrap(),
                )?;
                apply_up_to
            } else if let Some(apply_up_to) = store
                .db
                .get(ColumnFamily::Values, LEADER_COMMIT_INDEX_KEY)?
            {
                apply_up_to
            } else {
                return Ok(());
            };

            debug!(
                "Applying pending leader changes up to index {}.",
                apply_up_to
            );

            let mut log_batch = Vec::new();
            for (key, value) in store.db.iterator(
                ColumnFamily::Logs,
                &[LogKey::TOMBSTONE_KEY_PREFIX],
                Direction::Forward,
            )? {
                if !key.starts_with(&[LogKey::TOMBSTONE_KEY_PREFIX]) {
                    break;
                }
                let index = (&key[..])
                    .deserialize_be_u64(LogKey::TOMBSTONE_INDEX_POS)
                    .ok_or_else(|| {
                        StoreError::InternalError(format!(
                            "Failed to deserialize index from tombstone key: [{:?}]",
                            key
                        ))
                    })?;

                if apply_up_to != LogIndex::MAX && index <= apply_up_to {
                    let mut write_batch = WriteBatch::new(
                        (&key[..])
                            .deserialize_be_u32(LogKey::TOMBSTONE_ACCOUNT_POS)
                            .ok_or_else(|| {
                                StoreError::InternalError(format!(
                                    "Failed to deserialize account id from tombstone key: [{:?}]",
                                    key
                                ))
                            })?,
                    );

                    for document in bincode::deserialize::<Vec<Document>>(&value).map_err(|_| {
                        StoreError::SerializeError("Failed to deserialize tombstones".to_string())
                    })? {
                        write_batch.delete_document(document);
                    }

                    if !write_batch.is_empty() {
                        store.commit_write(write_batch)?;
                    }

                    log_batch.push(WriteOperation::Delete {
                        cf: ColumnFamily::Logs,
                        key: key.to_vec(),
                    });
                } else if do_reset {
                    log_batch.push(WriteOperation::Delete {
                        cf: ColumnFamily::Logs,
                        key: key.to_vec(),
                    });
                } else {
                    break;
                }
            }

            if !log_batch.is_empty() {
                store.db.write(log_batch)?;
            }

            if do_reset {
                store.prepare_rollback_changes(apply_up_to, false)?;
                store
                    .db
                    .delete(ColumnFamily::Values, LEADER_COMMIT_INDEX_KEY)?;
            }

            Ok(())
        })
        .await
    }
}
