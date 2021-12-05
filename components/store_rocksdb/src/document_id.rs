use std::{borrow::BorrowMut, collections::HashSet, ops::BitXorAssign, time::Instant};

use dashmap::{mapref::entry::Entry, DashMap};
use roaring::RoaringBitmap;
use store::{serialize::serialize_collection_key, AccountId, CollectionId, DocumentId, StoreError};

use crate::RocksDBStore;

pub struct DocumentIdAssigner {
    available_ids: RoaringBitmap,
    next_id: DocumentId,
    last_access: Instant,
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
        let mut failed = Ok(());
        let mut id_assigner_entry = self
            .id_assigner
            .entry((account, collection))
            .or_insert_with(|| {
                let (available_ids, next_id) = match self.get_document_ids(account, collection) {
                    Ok(Some(used_ids)) => {
                        let next_id = used_ids.max().unwrap() + 1;
                        let mut available_ids: RoaringBitmap = (0..next_id).collect();
                        available_ids.bitxor_assign(used_ids);
                        (available_ids, next_id)
                    }
                    Ok(None) => (RoaringBitmap::new(), 0),
                    Err(err) => {
                        failed = Err(err);
                        (RoaringBitmap::new(), 0)
                    }
                };
                DocumentIdAssigner {
                    available_ids,
                    next_id,
                    last_access: Instant::now(),
                }
            });

        failed?;

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
            &serialize_collection_key(account, collection),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::RocksDBStore;

    use super::UncommittedDocumentId;

    #[test]
    fn id_generator() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_id_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        let db = Arc::new(RocksDBStore::open(temp_dir.to_str().unwrap()).unwrap());

        rayon::ThreadPoolBuilder::new()
            .num_threads(10)
            .build()
            .unwrap()
            .scope(|s| {
                //let uncommited_ids = Arc::new(Mutex::new(Vec::new()));

                for _ in 0..=1000 {
                    let db = db.clone();
                    //let uncommited_ids = uncommited_ids.clone();
                    /*s.spawn(move |_| {
                        uncommited_ids.lock().unwrap().push(db.reserve_document_id(0, 0).unwrap());
                    });*/
                }
        });

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }
}
