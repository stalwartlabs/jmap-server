use std::ops::BitAndAssign;

use roaring::RoaringBitmap;
use store::{
    serialize::serialize_bm_tag_key, AccountId, CollectionId, FieldId, StoreDocumentSet,
    StoreError, StoreTag, Tag,
};

use crate::{
    bitmaps::{into_bitmap, RocksDBDocumentSet},
    RocksDBStore,
};

impl StoreDocumentSet for RocksDBStore {
    type Set = RocksDBDocumentSet;

    fn get_document_ids(
        &self,
        account: AccountId,
        collection: CollectionId,
    ) -> crate::Result<Self::Set> {
        Ok(RocksDBDocumentSet::from_roaring(
            self.get_document_ids(account, collection)?
                .unwrap_or_else(RoaringBitmap::new),
        ))
    }
}

impl StoreTag for RocksDBStore {
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
