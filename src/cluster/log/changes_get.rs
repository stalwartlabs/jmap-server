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
use store::core::bitmap::Bitmap;
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::log::changes::ChangeId;
use store::serialize::key::LogKey;
use store::{AccountId, ColumnFamily, JMAPStore, Store};

pub trait RaftStoreGet {
    fn get_log_changes(
        &self,
        entries: &mut Vec<Update>,
        account_id: AccountId,
        changed_collections: Bitmap<Collection>,
        change_id: ChangeId,
    ) -> store::Result<usize>;
}

impl<T> RaftStoreGet for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get_log_changes(
        &self,
        entries: &mut Vec<Update>,
        account_id: AccountId,
        changed_collections: Bitmap<Collection>,
        change_id: ChangeId,
    ) -> store::Result<usize> {
        let mut entries_size = 0;
        for changed_collection in changed_collections {
            let change = self
                .db
                .get::<Vec<u8>>(
                    ColumnFamily::Logs,
                    &LogKey::serialize_change(account_id, changed_collection, change_id),
                )?
                .ok_or_else(|| {
                    StoreError::InternalError(format!(
                        "Missing change for change {}/{:?}/{}",
                        account_id, changed_collection, change_id
                    ))
                })?;
            entries_size += change.len() + std::mem::size_of::<AccountId>() + 1;
            entries.push(Update::Begin {
                account_id,
                collection: changed_collection,
            });
            entries.push(Update::Change { change });
        }
        Ok(entries_size)
    }
}
