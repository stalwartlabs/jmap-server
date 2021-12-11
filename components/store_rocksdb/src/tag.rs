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
                &self.db.cf_handle("bitmaps").ok_or_else(|| {
                    StoreError::InternalError("No bitmaps column family found.".into())
                })?,
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
                &self.db.cf_handle("bitmaps").ok_or_else(|| {
                    StoreError::InternalError("No bitmaps column family found.".into())
                })?,
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
        self.db
            .get_cf(
                &self.db.cf_handle("bitmaps").ok_or_else(|| {
                    StoreError::InternalError("No bitmaps column family found.".into())
                })?,
                &serialize_bm_tag_key(account, collection, field, tag),
            )
            .map_err(|e| StoreError::InternalError(e.into_string()))?
            .map_or(Ok(false), |b| has_bit(&b, document))
    }
}
