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

use super::rpc::Response;
use super::{RaftIndexes, State};
use crate::cluster::log::AppendEntriesResponse;
use crate::cluster::log::Update;
use crate::JMAPServer;
use store::ahash::AHashMap;
use store::core::bitmap::Bitmap;
use store::core::collection::Collection;
use store::log::raft::LogIndex;
use store::serialize::key::LogKey;
use store::tracing::debug;
use store::write::operation::WriteOperation;
use store::{AccountId, ColumnFamily, Store};

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn handle_update_log(
        &self,
        mut indexes: &mut RaftIndexes,
        mut changed_accounts: AHashMap<AccountId, Bitmap<Collection>>,
        updates: Vec<Update>,
    ) -> Option<(State, Response)> {
        let store = self.store.clone();
        let mut last_index = indexes.uncommitted_index;
        let mut merge_index = indexes.merge_index;

        match self
            .spawn_worker(move || {
                let mut log_batch = Vec::with_capacity(updates.len());
                let mut is_done = updates.is_empty();
                let mut account_id = AccountId::MAX;
                let mut collection = Collection::None;

                for update in updates {
                    match update {
                        Update::Begin {
                            account_id: update_account_id,
                            collection: update_collection,
                        } => {
                            account_id = update_account_id;
                            collection = update_collection;
                        }
                        Update::Change { change } => {
                            #[cfg(test)]
                            {
                                assert!(last_index != LogIndex::MAX);
                                let existing_change = store
                                    .db
                                    .get::<Vec<u8>>(
                                        ColumnFamily::Logs,
                                        &LogKey::serialize_change(
                                            account_id, collection, last_index,
                                        ),
                                    )
                                    .unwrap();
                                assert!(
                                    existing_change.is_none(),
                                    "{} -> {:?}",
                                    last_index,
                                    existing_change
                                );
                            }
                            debug_assert!(
                                account_id != AccountId::MAX && collection != Collection::None
                            );

                            log_batch.push(WriteOperation::set(
                                ColumnFamily::Logs,
                                LogKey::serialize_change(account_id, collection, last_index),
                                change,
                            ));
                            changed_accounts
                                .entry(account_id)
                                .or_insert_with(Bitmap::default)
                                .insert(collection);
                        }
                        Update::Log { raft_id, log } => {
                            #[cfg(test)]
                            {
                                use store::log::{self};
                                use store::serialize::StoreDeserialize;
                                let existing_log = store
                                    .db
                                    .get::<log::entry::Entry>(
                                        ColumnFamily::Logs,
                                        &LogKey::serialize_raft(&raft_id),
                                    )
                                    .unwrap();
                                assert!(
                                    existing_log.is_none(),
                                    "{} -> existing: {:?} new: {:?}",
                                    raft_id.index,
                                    existing_log.unwrap(),
                                    log::entry::Entry::deserialize(&log).unwrap()
                                );
                            }

                            last_index = raft_id.index;
                            if merge_index == LogIndex::MAX {
                                merge_index = raft_id.index;
                            }

                            log_batch.push(WriteOperation::set(
                                ColumnFamily::Logs,
                                LogKey::serialize_raft(&raft_id),
                                log,
                            ));
                        }
                        Update::Eof => {
                            is_done = true;
                        }
                        _ => {
                            debug_assert!(false, "Invalid update: {:?}", update);
                        }
                    }
                }

                if !log_batch.is_empty() {
                    store.db.write(log_batch)?;
                }

                Ok((last_index, merge_index, changed_accounts, is_done))
            })
            .await
        {
            Ok((last_index, merge_index, changed_accounts, is_done)) => {
                indexes.uncommitted_index = last_index;
                indexes.merge_index = merge_index;

                if is_done {
                    self.request_updates(indexes, changed_accounts.into_iter().collect::<Vec<_>>())
                        .await
                } else {
                    (
                        State::AppendEntries { changed_accounts },
                        Response::AppendEntries(AppendEntriesResponse::Continue),
                    )
                        .into()
                }
            }
            Err(err) => {
                debug!("handle_update_log failed: {:?}", err);
                None
            }
        }
    }
}
