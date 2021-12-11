use std::{ops::BitXorAssign, time::Instant};

use dashmap::DashMap;
use roaring::RoaringBitmap;
use store::{serialize::serialize_ac_key_leb128, AccountId, CollectionId, DocumentId, StoreError};

use crate::RocksDBStore;

pub struct DocumentIdAssigner {
    available_ids: RoaringBitmap,
    next_id: DocumentId,
    last_access: Instant,
}

impl DocumentIdAssigner {
    pub fn get_next_id(&self) -> DocumentId {
        self.next_id
    }

    pub fn get_last_access(&self) -> &Instant {
        &self.last_access
    }

    pub fn get_available_ids(&self) -> &RoaringBitmap {
        &self.available_ids
    }
}

pub struct UncommittedDocumentId<'x> {
    account: AccountId,
    collection: CollectionId,
    id: DocumentId,
    committed: bool,
    id_assigner: &'x DashMap<(AccountId, CollectionId), DocumentIdAssigner>,
}

impl<'x> Drop for UncommittedDocumentId<'x> {
    fn drop(&mut self) {
        if !self.committed {
            if let Some(mut id_assigner) =
                self.id_assigner.get_mut(&(self.account, self.collection))
            {
                id_assigner.available_ids.insert(self.id);
            }
        }
    }
}

impl<'x> UncommittedDocumentId<'x> {
    pub fn commit(&mut self) -> DocumentId {
        self.committed = true;
        self.id
    }

    pub fn get_id(&self) -> DocumentId {
        self.id
    }
}

impl<'x> RocksDBStore {
    pub fn reserve_document_id(
        &'x self,
        account: AccountId,
        collection: CollectionId,
    ) -> crate::Result<UncommittedDocumentId<'x>> {
        let mut id_assigner_entry = self
            .id_assigner
            .entry((account, collection))
            .or_try_insert_with(|| {
                let (available_ids, next_id) =
                    if let Some(used_ids) = self.get_document_ids(account, collection)? {
                        let next_id = used_ids.max().unwrap() + 1;
                        let mut available_ids: RoaringBitmap = (0..next_id).collect();
                        available_ids.bitxor_assign(used_ids);
                        (available_ids, next_id)
                    } else {
                        (RoaringBitmap::new(), 0)
                    };
                Ok(DocumentIdAssigner {
                    available_ids,
                    next_id,
                    last_access: Instant::now(),
                })
            })?;

        let id_assigner = id_assigner_entry.value_mut();
        id_assigner.last_access = Instant::now();

        let document_id = if !id_assigner.available_ids.is_empty() {
            let document_id = id_assigner.available_ids.min().unwrap();
            id_assigner.available_ids.remove(document_id);
            document_id
        } else {
            let document_id = id_assigner.next_id;
            id_assigner.next_id += 1;
            document_id
        };

        Ok(UncommittedDocumentId {
            id: document_id,
            committed: false,
            id_assigner: &self.id_assigner,
            account,
            collection,
        })
    }

    pub fn get_document_ids(
        &self,
        account: AccountId,
        collection: CollectionId,
    ) -> crate::Result<Option<RoaringBitmap>> {
        self.get_bitmap(
            &self.db.cf_handle("bitmaps").ok_or_else(|| {
                StoreError::InternalError("No bitmaps column family found.".into())
            })?,
            &serialize_ac_key_leb128(account, collection),
        )
    }

    #[cfg(test)]
    pub fn set_document_ids(
        &self,
        account: AccountId,
        collection: CollectionId,
        bitmap: RoaringBitmap,
    ) -> crate::Result<()> {
        let mut bytes = Vec::with_capacity(bitmap.serialized_size());
        bitmap
            .serialize_into(&mut bytes)
            .map_err(|e| StoreError::InternalError(e.to_string()))?;

        self.db
            .put_cf(
                &self.db.cf_handle("bitmaps").ok_or_else(|| {
                    StoreError::InternalError("No bitmaps column family found.".into())
                })?,
                &serialize_ac_key_leb128(account, collection),
                bytes,
            )
            .map_err(|e| StoreError::InternalError(e.to_string()))
    }

    pub fn remove_id_assigner(
        &self,
        account: AccountId,
        collection: CollectionId,
    ) -> Option<DocumentIdAssigner> {
        self.id_assigner.remove(&(account, collection))?.1.into()
    }
}

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
                            uncommited_ids.push(db.reserve_document_id(0, 0).unwrap());
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
                    let mut doc_id = db.reserve_document_id(0, 0).unwrap();
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
}
