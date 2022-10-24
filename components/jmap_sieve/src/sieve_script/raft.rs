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

use jmap::{jmap_store::RaftObject, orm::serialize::JMAPOrm};
use store::{
    blob::BlobId,
    core::error::StoreError,
    write::{batch::WriteBatch, options::IndexOptions},
    AccountId, DocumentId, JMAPId, JMAPStore, Store,
};

use super::schema::{Property, SieveScript, Value};

impl<T> RaftObject<T> for SieveScript
where
    T: for<'x> Store<'x> + 'static,
{
    fn on_raft_update(
        _store: &JMAPStore<T>,
        _write_batch: &mut WriteBatch,
        document: &mut store::core::document::Document,
        _jmap_id: store::JMAPId,
        as_insert: Option<Vec<BlobId>>,
    ) -> store::Result<()> {
        if let Some(blobs) = as_insert {
            // First blobId contains the email
            let sieve_blob_id = blobs.into_iter().next().ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to get sieve blob for {}.",
                    document.document_id
                ))
            })?;

            // Link metadata blob
            document.blob(sieve_blob_id, IndexOptions::new());
        }
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
        store: &JMAPStore<T>,
        account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Vec<store::blob::BlobId>> {
        Ok(
            if let Some(Value::BlobId { value }) = store
                .get_orm::<SieveScript>(account_id, document_id)?
                .ok_or_else(|| StoreError::NotFound("SieveScript data not found".to_string()))?
                .remove(&Property::BlobId)
            {
                vec![value.id]
            } else {
                vec![]
            },
        )
    }
}
