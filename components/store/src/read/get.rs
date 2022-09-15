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

use roaring::RoaringBitmap;

use crate::{
    blob::BlobId,
    core::tag::Tag,
    nlp::term_index::TermIndex,
    serialize::{
        key::{BitmapKey, ValueKey},
        StoreDeserialize,
    },
    AccountId, Collection, ColumnFamily, DocumentId, FieldId, JMAPStore, Store, StoreError,
};

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn get_document_value<U>(
        &self,
        account_id: AccountId,
        collection: Collection,
        document: DocumentId,
        field: FieldId,
    ) -> crate::Result<Option<U>>
    where
        U: StoreDeserialize + 'static,
    {
        self.db.get(
            ColumnFamily::Values,
            &ValueKey::serialize_value(account_id, collection, document, field),
        )
    }

    pub fn get_multi_document_value<U>(
        &self,
        account_id: AccountId,
        collection: Collection,
        documents: impl Iterator<Item = DocumentId>,
        field: FieldId,
    ) -> crate::Result<Vec<Option<U>>>
    where
        U: StoreDeserialize + 'static,
    {
        self.db.multi_get(
            ColumnFamily::Values,
            documents
                .map(|document| ValueKey::serialize_value(account_id, collection, document, field))
                .collect::<Vec<_>>(),
        )
    }

    pub fn get_tag(
        &self,
        account_id: AccountId,
        collection: Collection,
        field: FieldId,
        tag: Tag,
    ) -> crate::Result<Option<RoaringBitmap>> {
        if let Some(tagged_docs) = self.db.get::<RoaringBitmap>(
            ColumnFamily::Bitmaps,
            &BitmapKey::serialize_tag(account_id, collection, field, &tag),
        )? {
            if !tagged_docs.is_empty() {
                return Ok(Some(tagged_docs));
            }
        }

        /*if let Some(document_ids) = self.get_document_ids(account_id, collection)? {
            if let Some(mut tagged_docs) = self.db.get::<RoaringBitmap>(
                ColumnFamily::Bitmaps,
                &BitmapKey::serialize_tag(account_id, collection, field, &tag),
            )? {
                tagged_docs &= &document_ids;
                if !tagged_docs.is_empty() {
                    return Ok(Some(tagged_docs));
                }
            }
        }*/

        Ok(None)
    }

    pub fn get_tags(
        &self,
        account_id: AccountId,
        collection: Collection,
        field: FieldId,
        tags: &[Tag],
    ) -> crate::Result<Vec<Option<RoaringBitmap>>> {
        let mut result = Vec::with_capacity(tags.len());
        for tagged_docs in self.db.multi_get::<RoaringBitmap, _>(
            ColumnFamily::Bitmaps,
            tags.iter()
                .map(|tag| BitmapKey::serialize_tag(account_id, collection, field, tag))
                .collect(),
        )? {
            if let Some(tagged_docs) = tagged_docs {
                if !tagged_docs.is_empty() {
                    result.push(Some(tagged_docs));
                    continue;
                }
            }
            result.push(None);
        }

        /*if let Some(document_ids) = self.get_document_ids(account_id, collection)? {
            for tagged_docs in self.db.multi_get::<RoaringBitmap, _>(
                ColumnFamily::Bitmaps,
                tags.iter()
                    .map(|tag| BitmapKey::serialize_tag(account_id, collection, field, tag))
                    .collect(),
            )? {
                if let Some(mut tagged_docs) = tagged_docs {
                    tagged_docs &= &document_ids;
                    if !tagged_docs.is_empty() {
                        result.push(Some(tagged_docs));
                        continue;
                    }
                }
                result.push(None);
            }
        }*/

        Ok(result)
    }

    pub fn get_term_index(
        &self,
        account_id: AccountId,
        collection: Collection,
        document_id: DocumentId,
    ) -> crate::Result<Option<TermIndex>> {
        if let Some(blob_id) = self.db.get::<BlobId>(
            ColumnFamily::Values,
            &ValueKey::serialize_term_index(account_id, collection, document_id),
        )? {
            Ok(TermIndex::deserialize(
                &self
                    .blob_get(&blob_id)?
                    .ok_or_else(|| StoreError::NotFound("Term Index Blob not found.".into()))?,
            )
            .ok_or_else(|| StoreError::InternalError("Failed to deserialize Term Index.".into()))?
            .into())
        } else {
            Ok(None)
        }
    }

    pub fn get_term_index_id(
        &self,
        account_id: AccountId,
        collection: Collection,
        document_id: DocumentId,
    ) -> crate::Result<Option<BlobId>> {
        self.db.get::<BlobId>(
            ColumnFamily::Values,
            &ValueKey::serialize_term_index(account_id, collection, document_id),
        )
    }
}
