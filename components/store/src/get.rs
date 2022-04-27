use roaring::RoaringBitmap;

use crate::{
    serialize::{BitmapKey, StoreDeserialize, ValueKey},
    AccountId, Collection, ColumnFamily, DocumentId, FieldId, JMAPStore, Store, Tag,
};

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn get_document_value<U>(
        &self,
        account: AccountId,
        collection: Collection,
        document: DocumentId,
        field: FieldId,
    ) -> crate::Result<Option<U>>
    where
        U: StoreDeserialize + 'static,
    {
        self.db.get(
            ColumnFamily::Values,
            &ValueKey::serialize_value(account, collection, document, field),
        )
    }

    pub fn get_multi_document_value<U>(
        &self,
        account: AccountId,
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
                .map(|document| ValueKey::serialize_value(account, collection, document, field))
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
        if let Some(document_ids) = self.get_document_ids(account_id, collection)? {
            if let Some(mut tagged_docs) = self.db.get::<RoaringBitmap>(
                ColumnFamily::Bitmaps,
                &BitmapKey::serialize_tag(account_id, collection, field, &tag),
            )? {
                tagged_docs &= &document_ids;
                if !tagged_docs.is_empty() {
                    return Ok(Some(tagged_docs));
                }
            }
        }

        Ok(None)
    }

    pub fn get_tags(
        &self,
        account: AccountId,
        collection: Collection,
        field: FieldId,
        tags: &[Tag],
    ) -> crate::Result<Vec<Option<RoaringBitmap>>> {
        let mut result = Vec::with_capacity(tags.len());
        if let Some(document_ids) = self.get_document_ids(account, collection)? {
            for tagged_docs in self.db.multi_get::<RoaringBitmap, _>(
                ColumnFamily::Bitmaps,
                tags.iter()
                    .map(|tag| BitmapKey::serialize_tag(account, collection, field, tag))
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
        }

        Ok(result)
    }
}
