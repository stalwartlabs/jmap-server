use std::{collections::HashSet, ops::BitXorAssign};

use roaring::RoaringBitmap;
use store::{serialize::serialize_collection_key, AccountId, CollectionId, DocumentId, StoreError};

use crate::RocksDBStore;

impl RocksDBStore {
    pub fn reserve_document_id(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        reuse_ids: bool,
    ) -> crate::Result<DocumentId> {
        let mut entry = self
            .reserved_ids
            .entry((*account, *collection))
            .or_insert_with(HashSet::new);
        let reserved_ids = entry.value_mut();
        let used_ids = self.get_document_ids(account, collection)?;
        let mut id: DocumentId = 0;

        if !used_ids.is_empty() {
            if reuse_ids {
                id = 0;
                for used_id in &used_ids {
                    if (used_id - id) > 0 {
                        for available_id in id..used_id {
                            if !reserved_ids.contains(&available_id) {
                                reserved_ids.insert(available_id);
                                //println!("reserved freed id {}", available_id);
                                return Ok(available_id);
                            }
                        }
                    }
                    id = used_id + 1;
                }
            } else {
                id = used_ids.max().unwrap() + 1;
            }
        }
        loop {
            if !reserved_ids.contains(&id) {
                reserved_ids.insert(id);
                //println!("reserved new id {}", id);
                return Ok(id);
            }
            id += 1;
        }
    }

    pub fn release_document_id(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        id: &DocumentId,
    ) -> bool {
        self.reserved_ids
            .entry((*account, *collection))
            .or_insert_with(HashSet::new)
            .value_mut()
            .remove(id)
    }

    pub fn get_document_ids(
        &self,
        account: &AccountId,
        collection: &CollectionId,
    ) -> crate::Result<RoaringBitmap> {
        Ok(self
            .get_bitmap(
                &self.db.cf_handle("bitmaps").ok_or_else(|| {
                    StoreError::InternalError("No bitmaps column family found.".into())
                })?,
                &serialize_collection_key(account, collection),
            )?
            .unwrap_or_else(RoaringBitmap::new))
    }
}
