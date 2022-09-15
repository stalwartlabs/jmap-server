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

use crate::JMAPServer;
use store::core::collection::Collection;
use store::serialize::key::LogKey;
use store::{AccountId, ColumnFamily, JMAPStore, Store};

pub trait RaftStoreRollbackRemove {
    fn remove_rollback_change(
        &self,
        account_id: AccountId,
        collection: Collection,
    ) -> store::Result<()>;
}

impl<T> RaftStoreRollbackRemove for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn remove_rollback_change(
        &self,
        account_id: AccountId,
        collection: Collection,
    ) -> store::Result<()> {
        self.db.delete(
            ColumnFamily::Logs,
            &LogKey::serialize_rollback(account_id, collection),
        )
    }
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn remove_rollback_change(
        &self,
        account_id: AccountId,
        collection: Collection,
    ) -> store::Result<()> {
        let store = self.store.clone();
        self.spawn_worker(move || store.remove_rollback_change(account_id, collection))
            .await
    }
}
