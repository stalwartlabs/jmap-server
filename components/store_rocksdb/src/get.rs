use store::{
    serialize::{serialize_stored_key, serialize_stored_key_global, StoreDeserialize},
    AccountId, CollectionId, DocumentId, FieldId, FieldNumber, StoreError, StoreGet,
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
        Vec<u8>: StoreDeserialize<T>,
    {
        if let Some(bytes) = self.get_raw_value(account, collection, field)? {
            Ok(Some(bytes.deserialize()?))
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
        field_num: FieldNumber,
    ) -> store::Result<Option<T>>
    where
        Vec<u8>: StoreDeserialize<T>,
    {
        if let Some(bytes) =
            self.get_document_raw_value(account, collection, document, field, field_num)?
        {
            Ok(Some(bytes.deserialize()?))
        } else {
            Ok(None)
        }
    }

    fn get_multi_document_value<T>(
        &self,
        account: AccountId,
        collection: CollectionId,
        documents: &[DocumentId],
        field: FieldId,
    ) -> store::Result<Vec<Option<T>>>
    where
        Vec<u8>: StoreDeserialize<T>,
    {
        let mut result = Vec::with_capacity(documents.len());
        for item in self.get_multi_document_raw_value(account, collection, documents, field, 0)? {
            if let Some(bytes) = item {
                result.push(Some(bytes.deserialize()?));
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

    fn get_multi_document_raw_value(
        &self,
        account: AccountId,
        collection: CollectionId,
        documents: &[DocumentId],
        field: FieldId,
        pos: FieldNumber,
    ) -> store::Result<Vec<Option<Vec<u8>>>> {
        let cf_values = self.get_handle("values")?;
        let mut query = Vec::new();
        let mut query_result_pos = Vec::new();
        let mut result = vec![None; documents.len()];

        if let Some(tombstoned_ids) = self.get_tombstoned_ids(account, collection)? {
            for (list_pos, &document) in documents.iter().enumerate() {
                if !tombstoned_ids.contains(document) {
                    query.push((
                        &cf_values,
                        serialize_stored_key(account, collection, document, field, pos),
                    ));
                    query_result_pos.push(list_pos);
                }
            }
        } else {
            for (list_pos, &document) in documents.iter().enumerate() {
                query.push((
                    &cf_values,
                    serialize_stored_key(account, collection, document, field, pos),
                ));
                query_result_pos.push(list_pos);
            }
        }

        if !query.is_empty() {
            for (value, list_pos) in self
                .db
                .multi_get_cf(query)
                .into_iter()
                .zip(query_result_pos.into_iter())
            {
                if let Some(value) =
                    value.map_err(|e| StoreError::InternalError(e.into_string()))?
                {
                    result[list_pos] = Some(value);
                }
            }
        }

        Ok(result)
    }
}
