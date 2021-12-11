use store::{
    serialize::serialize_bm_tag_key, AccountId, CollectionId, DocumentId, FieldId, StoreError,
    StoreTag, Tag,
};

use crate::{
    bitmaps::{clear_bit, has_bit, set_bit},
    RocksDBStore,
};

impl StoreTag for RocksDBStore {
    fn set_tag(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
        tag: &Tag,
    ) -> crate::Result<()> {
        self.db
            .merge_cf(
                &self.get_handle("bitmaps")?,
                &serialize_bm_tag_key(account, collection, field, tag),
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
        tag: &Tag,
    ) -> crate::Result<()> {
        self.db
            .merge_cf(
                &self.get_handle("bitmaps")?,
                &serialize_bm_tag_key(account, collection, field, tag),
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
        tag: &Tag,
    ) -> crate::Result<bool> {
        let cf_bitmaps = self.get_handle("bitmaps")?;
        self.db
            .get_cf(
                &cf_bitmaps,
                &serialize_bm_tag_key(account, collection, field, tag),
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
}
