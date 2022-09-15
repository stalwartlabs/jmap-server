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

use store::{
    blob::BlobId, write::batch::WriteBatch, AccountId, DocumentId, JMAPId, JMAPStore, Store,
};

use crate::jmap_store::RaftObject;

use super::schema::Principal;

impl<T> RaftObject<T> for Principal
where
    T: for<'x> Store<'x> + 'static,
{
    fn on_raft_update(
        _store: &JMAPStore<T>,
        _write_batch: &mut WriteBatch,
        _document: &mut store::core::document::Document,
        _jmap_id: store::JMAPId,
        _as_insert: Option<Vec<BlobId>>,
    ) -> store::Result<()> {
        Ok(())
    }

    fn get_jmap_id(
        _store: &JMAPStore<T>,
        _account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Option<store::JMAPId>> {
        Ok((document_id as JMAPId).into())
    }

    fn get_blobs(
        _store: &JMAPStore<T>,
        _account_id: AccountId,
        _document_id: DocumentId,
    ) -> store::Result<Vec<store::blob::BlobId>> {
        Ok(Vec::with_capacity(0))
    }
}
