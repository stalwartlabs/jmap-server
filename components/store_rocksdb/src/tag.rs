use std::ops::BitAndAssign;

use store::{
    serialize::serialize_bm_tag_key, AccountId, CollectionId, DocumentId, FieldId,
    StoreDocumentSet, StoreError, StoreTag, StoreTombstone, Tag,
};

use crate::{
    bitmaps::{clear_bit, has_bit, into_bitmap, set_bit, RocksDBDocumentSet},
    RocksDBStore,
};

impl StoreDocumentSet for RocksDBStore {
    type Set = RocksDBDocumentSet;
}

impl StoreTag for RocksDBStore {
    fn set_tag(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
        tag: Tag,
    ) -> crate::Result<()> {
        self.db
            .merge_cf(
                &self.get_handle("bitmaps")?,
                &serialize_bm_tag_key(account, collection, field, &tag),
                &set_bit(document),
            )
            .map_err(|e| StoreError::InternalError(e.into_string()))
    }

    fn clear_tag(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
        tag: Tag,
    ) -> crate::Result<()> {
        self.db
            .merge_cf(
                &self.get_handle("bitmaps")?,
                &serialize_bm_tag_key(account, collection, field, &tag),
                &clear_bit(document),
            )
            .map_err(|e| StoreError::InternalError(e.into_string()))
    }

    fn has_tag(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
        tag: Tag,
    ) -> crate::Result<bool> {
        let cf_bitmaps = self.get_handle("bitmaps")?;
        self.db
            .get_cf(
                &cf_bitmaps,
                &serialize_bm_tag_key(account, collection, field, &tag),
            )
            .map_err(|e| StoreError::InternalError(e.into_string()))?
            .map_or(Ok(false), |b| {
                if has_bit(&b, document)? {
                    match self.get_tombstoned_ids(account, collection)? {
                        Some(tombstone_ids) if tombstone_ids.contains(document) => Ok(false),
                        _ => Ok(true),
                    }
                } else {
                    Ok(false)
                }
            })
    }

    fn get_tags(
        &self,
        account: AccountId,
        collection: CollectionId,
        field: FieldId,
        tags: &[Tag],
    ) -> store::Result<Vec<Option<Self::Set>>> {
        let cf_bitmaps = self.get_handle("bitmaps")?;
        let mut result = Vec::with_capacity(tags.len());
        if let Some(document_ids) = self.get_document_ids(account, collection)? {
            let mut keys = Vec::with_capacity(tags.len());

            for tag in tags {
                keys.push((
                    &cf_bitmaps,
                    serialize_bm_tag_key(account, collection, field, tag),
                ));
            }

            for bytes in self.db.multi_get_cf(keys) {
                if let Some(bytes) =
                    bytes.map_err(|e| StoreError::InternalError(e.into_string()))?
                {
                    let mut tagged_docs = into_bitmap(&bytes)?;
                    tagged_docs.bitand_assign(&document_ids);
                    if !tagged_docs.is_empty() {
                        result.push(Some(RocksDBDocumentSet::from_roaring(tagged_docs)));
                        continue;
                    }
                }
                result.push(None);
            }
        }

        Ok(result)
    }
}
