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

use super::Update;
use crate::cluster::log::update_apply::RaftStoreApplyUpdate;
use crate::JMAPServer;
use store::core::collection::Collection;
use store::write::batch::WriteBatch;
use store::{tracing::debug, AccountId, Store};

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn apply_rollback_updates(&self, updates: Vec<Update>) -> store::Result<bool> {
        let store = self.store.clone();
        self.spawn_worker(move || {
            let mut write_batch = WriteBatch::new(AccountId::MAX);

            debug!("Inserting {} rollback changes...", updates.len(),);
            let mut is_done = false;
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
                    Update::Document { update } => {
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
                    Update::Eof => {
                        is_done = true;
                    }
                    _ => debug_assert!(false, "Invalid update type: {:?}", update),
                }
            }
            if !write_batch.is_empty() {
                store.write(write_batch)?;
            }

            Ok(is_done)
        })
        .await
    }
}
