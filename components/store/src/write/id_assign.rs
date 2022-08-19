use std::sync::Arc;

use parking_lot::Mutex;
use roaring::RoaringBitmap;

use crate::{
    serialize::key::BitmapKey, AccountId, Collection, DocumentId, JMAPStore, Store, StoreError,
};

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct IdCacheKey {
    pub account_id: AccountId,
    pub collection: Collection,
}

impl IdCacheKey {
    pub fn new(account_id: AccountId, collection: Collection) -> Self {
        Self {
            account_id,
            collection,
        }
    }
}

#[derive(Clone)]
pub struct IdAssigner {
    pub freed_ids: Option<RoaringBitmap>,
    pub next_id: DocumentId,
}

impl IdAssigner {
    pub fn new(used_ids: Option<RoaringBitmap>) -> Self {
        let (next_id, freed_ids) = if let Some(used_ids) = used_ids {
            let next_id = used_ids.max().unwrap() + 1;
            let mut freed_ids = RoaringBitmap::from_sorted_iter(0..next_id).unwrap();
            freed_ids ^= used_ids;
            (
                next_id,
                if !freed_ids.is_empty() {
                    Some(freed_ids)
                } else {
                    None
                },
            )
        } else {
            (0, None)
        };
        Self { freed_ids, next_id }
    }

    pub fn assign_document_id(&mut self) -> DocumentId {
        if let Some(freed_ids) = &mut self.freed_ids {
            let id = freed_ids.min().unwrap();
            freed_ids.remove(id);
            if freed_ids.is_empty() {
                self.freed_ids = None;
            }
            id
        } else {
            let id = self.next_id;
            self.next_id += 1;
            id
        }
    }
}

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn get_id_assigner(
        &self,
        account_id: AccountId,
        collection: Collection,
    ) -> crate::Result<Arc<Mutex<IdAssigner>>> {
        self.id_assigner
            .try_get_with::<_, StoreError>(IdCacheKey::new(account_id, collection), || {
                Ok(Arc::new(Mutex::new(IdAssigner::new(
                    self.get_document_ids(account_id, collection)?,
                ))))
            })
            .map_err(|e| e.as_ref().clone())
    }

    pub fn assign_document_id(
        &self,
        account_id: AccountId,
        collection: Collection,
    ) -> crate::Result<DocumentId> {
        Ok(self
            .get_id_assigner(account_id, collection)?
            .lock()
            .assign_document_id())
    }

    pub fn get_document_ids(
        &self,
        account_id: AccountId,
        collection: Collection,
    ) -> crate::Result<Option<RoaringBitmap>> {
        self.get_bitmap(&BitmapKey::serialize_document_ids(account_id, collection))
    }
}

#[cfg(test)]
mod tests {
    use roaring::RoaringBitmap;

    use super::IdAssigner;

    #[test]
    fn id_assigner() {
        let mut assigner = IdAssigner::new(None);
        assert_eq!(assigner.assign_document_id(), 0);
        assert_eq!(assigner.assign_document_id(), 1);
        assert_eq!(assigner.assign_document_id(), 2);

        let mut assigner = IdAssigner::new(
            RoaringBitmap::from_sorted_iter([0, 2, 4, 6])
                .unwrap()
                .into(),
        );
        assert_eq!(assigner.assign_document_id(), 1);
        assert_eq!(assigner.assign_document_id(), 3);
        assert_eq!(assigner.assign_document_id(), 5);
        assert_eq!(assigner.assign_document_id(), 7);
        assert_eq!(assigner.assign_document_id(), 8);
    }
}
