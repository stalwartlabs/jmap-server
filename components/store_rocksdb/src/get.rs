use store::{
    serialize::serialize_stored_key, AccountId, CollectionId, DocumentId, FieldId, FieldNumber,
    StoreError, StoreGet,
};

use crate::RocksDBStore;

impl StoreGet for RocksDBStore {
    fn get_stored_value(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
        pos: FieldNumber,
    ) -> crate::Result<Option<Vec<u8>>> {
        match self.get_tombstoned_ids(account, collection)? {
            Some(tombstoned_ids) if tombstoned_ids.contains(document) => Ok(None),
            _ => self
                .db
                .get_cf(
                    &self.get_handle("values")?,
                    &serialize_stored_key(account, collection, document, field, pos),
                )
                .map_err(|e| StoreError::InternalError(e.into_string())),
        }
    }
}
