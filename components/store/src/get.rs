use std::ops::BitAndAssign;

use roaring::RoaringBitmap;

use crate::{
    serialize::{serialize_bm_tag_key, serialize_stored_key, StoreDeserialize},
    AccountId, CollectionId, ColumnFamily, DocumentId, FieldId, JMAPStore, Store, StoreError, Tag,
};

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn get<U>(&self, cf: ColumnFamily, key: Vec<u8>) -> crate::Result<Option<U>>
    where
        U: StoreDeserialize + 'static,
    {
        let db = self.db.clone();
        self.spawn_blocking(move || db.get(cf, key)).await
    }

    pub async fn multi_get<U>(
        &self,
        cf: ColumnFamily,
        keys: Vec<Vec<u8>>,
    ) -> crate::Result<Vec<Option<U>>>
    where
        U: StoreDeserialize + 'static,
    {
        let db = self.db.clone();
        self.spawn_blocking(move || db.multi_get(cf, keys)).await
    }

    pub async fn exists(&self, cf: ColumnFamily, key: Vec<u8>) -> crate::Result<bool> {
        let db = self.db.clone();
        self.spawn_blocking(move || db.exists(cf, key)).await
    }

    pub async fn get_document_value<U>(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
    ) -> crate::Result<Option<U>>
    where
        U: StoreDeserialize + 'static,
    {
        match self.get_tombstoned_ids(account, collection).await? {
            Some(tombstoned_ids) if tombstoned_ids.contains(document) => Ok(None),
            _ => {
                self.get(
                    ColumnFamily::Values,
                    serialize_stored_key(account, collection, document, field),
                )
                .await
            }
        }
    }

    pub async fn get_multi_document_value<U>(
        &self,
        account: AccountId,
        collection: CollectionId,
        documents: impl Iterator<Item = DocumentId>,
        field: FieldId,
    ) -> crate::Result<Vec<Option<U>>>
    where
        U: StoreDeserialize + 'static,
    {
        self.multi_get(
            ColumnFamily::Values,
            documents
                .map(|document| serialize_stored_key(account, collection, document, field))
                .collect::<Vec<_>>(),
        )
        .await
    }

    pub async fn get_tag(
        &self,
        account_id: AccountId,
        collection_id: CollectionId,
        field: FieldId,
        tag: Tag,
    ) -> crate::Result<Option<RoaringBitmap>> {
        if let Some(document_ids) = self.get_document_ids(account_id, collection_id).await? {
            if let Some(mut tagged_docs) = self
                .get::<RoaringBitmap>(
                    ColumnFamily::Bitmaps,
                    serialize_bm_tag_key(account_id, collection_id, field, &tag),
                )
                .await?
            {
                tagged_docs &= &document_ids;
                if !tagged_docs.is_empty() {
                    return Ok(Some(tagged_docs));
                }
            }
        }

        Ok(None)
    }

    pub async fn get_tags(
        &self,
        account: AccountId,
        collection: CollectionId,
        field: FieldId,
        tags: &[Tag],
    ) -> crate::Result<Vec<Option<RoaringBitmap>>> {
        let mut result = Vec::with_capacity(tags.len());
        if let Some(document_ids) = self.get_document_ids(account, collection).await? {
            for tagged_docs in self
                .multi_get::<RoaringBitmap>(
                    ColumnFamily::Bitmaps,
                    tags.iter()
                        .map(|tag| serialize_bm_tag_key(account, collection, field, tag))
                        .collect(),
                )
                .await?
            {
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
