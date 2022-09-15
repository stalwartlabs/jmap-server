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

use super::changes_get::RaftStoreGet;
use super::Update;
use store::core::bitmap::Bitmap;
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::log::entry::Entry;
use store::log::raft::{LogIndex, RaftId};
use store::serialize::key::LogKey;
use store::serialize::StoreDeserialize;
use store::{AccountId, ColumnFamily, Direction, JMAPStore, Store};

pub trait RaftStoreEntries {
    #[allow(clippy::type_complexity)]
    fn get_log_entries(
        &self,
        last_index: LogIndex,
        to_index: LogIndex,
        pending_changes: Vec<(Bitmap<Collection>, Vec<AccountId>)>,
        batch_size: usize,
    ) -> store::Result<(
        Vec<Update>,
        Vec<(Bitmap<Collection>, Vec<AccountId>)>,
        LogIndex,
    )>;
}

impl<T> RaftStoreEntries for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get_log_entries(
        &self,
        mut last_index: LogIndex,
        to_index: LogIndex,
        mut pending_changes: Vec<(Bitmap<Collection>, Vec<AccountId>)>,
        batch_size: usize,
    ) -> store::Result<(
        Vec<Update>,
        Vec<(Bitmap<Collection>, Vec<AccountId>)>,
        LogIndex,
    )> {
        let mut entries = Vec::new();
        let start_index = last_index;
        let key = if start_index != LogIndex::MAX {
            LogKey::serialize_raft(&RaftId::new(0, start_index))
        } else {
            LogKey::serialize_raft(&RaftId::new(0, 0))
        };
        let prefix = &[LogKey::RAFT_KEY_PREFIX];
        let mut entries_size = 0;

        if pending_changes.is_empty() && start_index != to_index {
            for (key, value) in self
                .db
                .iterator(ColumnFamily::Logs, &key, Direction::Forward)?
            {
                if !key.starts_with(prefix) {
                    break;
                }

                let raft_id = LogKey::deserialize_raft(&key).ok_or_else(|| {
                    StoreError::InternalError(format!("Corrupted raft entry for [{:?}]", key))
                })?;

                if raft_id.index > start_index || start_index == LogIndex::MAX {
                    last_index = raft_id.index;
                    entries_size += value.len() + std::mem::size_of::<RaftId>();
                    entries.push(Update::Log {
                        raft_id,
                        log: value.to_vec(),
                    });

                    match Entry::deserialize(&value).ok_or_else(|| {
                        StoreError::InternalError(format!("Corrupted raft entry for [{:?}]", key))
                    })? {
                        Entry::Item {
                            account_id,
                            changed_collections,
                        } => {
                            entries_size += self.get_log_changes(
                                &mut entries,
                                account_id,
                                changed_collections,
                                raft_id.index,
                            )?;
                        }
                        Entry::Snapshot { changed_accounts } => {
                            debug_assert!(pending_changes.is_empty());
                            pending_changes = changed_accounts;
                            break;
                        }
                    };

                    if raft_id.index == to_index || entries_size >= batch_size {
                        break;
                    }
                }
            }
        }

        if !pending_changes.is_empty() {
            while let Some((collections, account_ids)) = pending_changes.last_mut() {
                if let Some(account_id) = account_ids.pop() {
                    entries_size += self.get_log_changes(
                        &mut entries,
                        account_id,
                        collections.clone(),
                        last_index,
                    )?;
                    if entries_size >= batch_size {
                        break;
                    }
                } else {
                    pending_changes.pop();
                }
            }
        }

        if last_index == to_index && pending_changes.is_empty() {
            entries.push(Update::Eof);
        }

        Ok((entries, pending_changes, last_index))
    }
}
