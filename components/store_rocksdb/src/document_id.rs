use std::ops::BitXorAssign;

use roaring::RoaringBitmap;
use store::{
    serialize::{serialize_bm_internal, BM_FREED_IDS, BM_USED_IDS},
    AccountId, CollectionId, DocumentId, StoreTombstone, UncommittedDocumentId,
};

use crate::RocksDBStore;

#[derive(Clone)]
pub enum AssignedDocumentId {
    New(DocumentId),
    Freed(DocumentId),
}

impl UncommittedDocumentId for AssignedDocumentId {
    fn get_document_id(&self) -> DocumentId {
        match self {
            AssignedDocumentId::New(id) | AssignedDocumentId::Freed(id) => *id,
        }
    }
}

impl<'x> RocksDBStore {
    pub fn get_document_ids_used(
        &self,
        account: AccountId,
        collection: CollectionId,
    ) -> crate::Result<Option<RoaringBitmap>> {
        self.get_bitmap(
            &self.get_handle("bitmaps")?,
            &serialize_bm_internal(account, collection, BM_USED_IDS),
        )
    }

    pub fn get_document_ids_freed(
        &self,
        account: AccountId,
        collection: CollectionId,
    ) -> crate::Result<Option<RoaringBitmap>> {
        self.get_bitmap(
            &self.get_handle("bitmaps")?,
            &serialize_bm_internal(account, collection, BM_FREED_IDS),
        )
    }

    pub fn get_document_ids(
        &self,
        account: AccountId,
        collection: CollectionId,
    ) -> crate::Result<Option<RoaringBitmap>> {
        if let Some(mut docs) = self.get_document_ids_used(account, collection)? {
            if let Some(tombstoned_docs) = self.get_tombstoned_ids(account, collection)? {
                docs.bitxor_assign(tombstoned_docs.bitmap);
            }
            Ok(Some(docs))
        } else {
            Ok(None)
        }
    }

    #[cfg(test)]
    pub fn set_document_ids(
        &self,
        account: AccountId,
        collection: CollectionId,
        bitmap: RoaringBitmap,
    ) -> crate::Result<()> {
        use store::StoreError;

        use crate::bitmaps::IS_BITMAP;

        let mut bytes = Vec::with_capacity(bitmap.serialized_size() + 1);
        bytes.push(IS_BITMAP);
        bitmap
            .serialize_into(&mut bytes)
            .map_err(|e| StoreError::InternalError(e.to_string()))?;

        self.db
            .put_cf(
                &self.get_handle("bitmaps")?,
                &serialize_bm_internal(account, collection, BM_USED_IDS),
                bytes,
            )
            .map_err(|e| StoreError::InternalError(e.to_string()))
    }
}

/*
#[cfg(test)]
mod tests {
    use std::{ops::BitXorAssign, sync::Arc, thread};

    use roaring::RoaringBitmap;

    use crate::RocksDBStore;

    #[test]
    fn id_assigner() {
        rayon::ThreadPoolBuilder::new()
            .num_threads(20)
            .build()
            .unwrap()
            .scope(|s| {
                let mut temp_dir = std::env::temp_dir();
                temp_dir.push("strdb_id_test");
                if temp_dir.exists() {
                    std::fs::remove_dir_all(&temp_dir).unwrap();
                }

                let db = Arc::new(RocksDBStore::open(temp_dir.to_str().unwrap()).unwrap());

                for _ in 0..10 {
                    let db = db.clone();
                    s.spawn(move |_| {
                        let mut uncommited_ids = Vec::new();
                        for _ in 0..100 {
                            uncommited_ids.push(db.get_next_document_id(0, 0).unwrap());
                        }
                        thread::sleep(std::time::Duration::from_millis(100));
                    });
                }
                thread::sleep(std::time::Duration::from_millis(200));

                // Uncommitted ids should be released
                assert_eq!(
                    db.remove_id_assigner(0, 0).unwrap().get_available_ids(),
                    &(0..1000).collect::<RoaringBitmap>()
                );

                // Deleted ids should be made available
                let mut used_ids = RoaringBitmap::new();
                let mut x = (1, 1);
                for _ in 0..10 {
                    used_ids.insert(x.0);
                    x = (x.1, x.0 + x.1)
                }
                for i in 56..=60 {
                    used_ids.insert(i);
                }
                let mut expected_ids = (0..=60).collect::<RoaringBitmap>();
                expected_ids.bitxor_assign(&used_ids);
                expected_ids.insert_range(61..=63);
                db.set_document_ids(0, 0, used_ids).unwrap();

                for _ in 0..50 {
                    let mut doc_id = db.get_next_document_id(0, 0).unwrap();
                    assert!(
                        expected_ids.contains(doc_id.get_id()),
                        "Unexpected id {}",
                        doc_id.get_id()
                    );
                    expected_ids.remove(doc_id.get_id());
                    doc_id.commit();
                }

                assert!(expected_ids.is_empty(), "Missing ids: {:?}", expected_ids);

                std::fs::remove_dir_all(&temp_dir).unwrap();
            });
    }
}*/
