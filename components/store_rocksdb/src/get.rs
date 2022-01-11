use store::{
    serialize::{serialize_stored_key, serialize_stored_key_global, StoreDeserialize},
    AccountId, CollectionId, DocumentId, DocumentSet, FieldId, StoreError, StoreGet,
    StoreTombstone,
};

use crate::RocksDBStore;

impl StoreGet for RocksDBStore {
    fn get_value<T>(
        &self,
        account: Option<AccountId>,
        collection: Option<CollectionId>,
        field: Option<FieldId>,
    ) -> store::Result<Option<T>>
    where
        T: StoreDeserialize,
    {
        if let Some(bytes) = self.get_raw_value(account, collection, field)? {
            Ok(Some(T::deserialize(bytes)?))
        } else {
            Ok(None)
        }
    }

    fn get_document_value<T>(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
    ) -> store::Result<Option<T>>
    where
        T: StoreDeserialize,
    {
        if let Some(bytes) = self.get_document_raw_value(account, collection, document, field)? {
            Ok(Some(T::deserialize(bytes)?))
        } else {
            Ok(None)
        }
    }

    fn get_multi_document_value<T>(
        &self,
        account: AccountId,
        collection: CollectionId,
        documents: impl Iterator<Item = DocumentId>,
        field: FieldId,
    ) -> store::Result<Vec<Option<T>>>
    where
        T: StoreDeserialize,
    {
        let mut result = Vec::with_capacity(documents.size_hint().0);
        for bytes in self.get_multi_document_raw_value(account, collection, documents, field)? {
            if let Some(bytes) = bytes {
                result.push(Some(T::deserialize(bytes)?));
            } else {
                result.push(None);
            }
        }
        Ok(result)
    }
}

impl RocksDBStore {
    fn get_raw_value(
        &self,
        account: Option<AccountId>,
        collection: Option<CollectionId>,
        field: Option<FieldId>,
    ) -> crate::Result<Option<Vec<u8>>> {
        self.db
            .get_cf(
                &self.get_handle("values")?,
                &serialize_stored_key_global(account, collection, field),
            )
            .map_err(|e| StoreError::InternalError(e.into_string()))
    }

    fn get_document_raw_value(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
    ) -> crate::Result<Option<Vec<u8>>> {
        match self.get_tombstoned_ids(account, collection)? {
            Some(tombstoned_ids) if tombstoned_ids.contains(document) => Ok(None),
            _ => self
                .db
                .get_cf(
                    &self.get_handle("values")?,
                    &serialize_stored_key(account, collection, document, field),
                )
                .map_err(|e| StoreError::InternalError(e.into_string())),
        }
    }

    fn get_multi_document_raw_value(
        &self,
        account: AccountId,
        collection: CollectionId,
        documents: impl Iterator<Item = DocumentId>,
        field: FieldId,
    ) -> store::Result<Vec<Option<Vec<u8>>>> {
        let cf_values = self.get_handle("values")?;
        let mut result = Vec::with_capacity(documents.size_hint().0);

        let query = documents
            .map(|document| {
                (
                    &cf_values,
                    serialize_stored_key(account, collection, document, field),
                )
            })
            .collect::<Vec<_>>();

        if !query.is_empty() {
            for value in self.db.multi_get_cf(query) {
                result.push(value.map_err(|e| StoreError::InternalError(e.into_string()))?);
            }
        }

        Ok(result)
    }
}
