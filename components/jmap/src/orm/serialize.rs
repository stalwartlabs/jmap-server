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

use store::serialize::{StoreDeserialize, StoreSerialize};
use store::{AccountId, DocumentId, JMAPStore, Store};

use super::{Object, TinyORM};

impl<T> StoreSerialize for TinyORM<T>
where
    T: Object,
{
    fn serialize(&self) -> Option<Vec<u8>> {
        store::bincode::serialize(self).ok()
    }
}

impl<T> StoreDeserialize for TinyORM<T>
where
    T: Object,
{
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        store::bincode::deserialize(bytes).ok()
    }
}

pub trait JMAPOrm {
    fn get_orm<U>(
        &self,
        account: AccountId,
        document: DocumentId,
    ) -> store::Result<Option<TinyORM<U>>>
    where
        U: Object + 'static;
}

impl<T> JMAPOrm for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get_orm<U>(
        &self,
        account: AccountId,
        document: DocumentId,
    ) -> store::Result<Option<TinyORM<U>>>
    where
        U: Object + 'static,
    {
        self.get_document_value::<TinyORM<U>>(
            account,
            U::collection(),
            document,
            TinyORM::<U>::FIELD_ID,
        )
    }
}
