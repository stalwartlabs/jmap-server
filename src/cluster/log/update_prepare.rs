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

use super::DocumentUpdate;
use jmap::{jmap_store::RaftObject, orm::serialize::JMAPOrm};
use store::serialize::StoreSerialize;
use store::{core::error::StoreError, AccountId, DocumentId, JMAPStore, Store};

pub trait RaftStorePrepareUpdate<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn raft_prepare_update<U>(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
        as_insert: bool,
    ) -> store::Result<Option<DocumentUpdate>>
    where
        U: RaftObject<T> + 'static;
}

impl<T> RaftStorePrepareUpdate<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn raft_prepare_update<U>(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
        as_insert: bool,
    ) -> store::Result<Option<DocumentUpdate>>
    where
        U: RaftObject<T> + 'static,
    {
        Ok(
            if let (Some(fields), Some(jmap_id)) = (
                self.get_orm::<U>(account_id, document_id)?,
                U::get_jmap_id(self, account_id, document_id)?,
            ) {
                let fields = fields.serialize().ok_or_else(|| {
                    StoreError::SerializeError("Failed to serialize ORM.".to_string())
                })?;

                Some(if as_insert {
                    DocumentUpdate::Insert {
                        blobs: U::get_blobs(self, account_id, document_id)?,
                        term_index: self.get_term_index_id(
                            account_id,
                            U::collection(),
                            document_id,
                        )?,
                        jmap_id,
                        fields,
                    }
                } else {
                    DocumentUpdate::Update { jmap_id, fields }
                })
            } else {
                None
            },
        )
    }
}
