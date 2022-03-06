use std::ops::BitAndAssign;

use crate::{
    serialize::{
        deserialize_document_id_from_leb128, deserialize_index_document_id, serialize_a_key_be,
        serialize_a_key_leb128, serialize_ac_key_be, serialize_ac_key_leb128, serialize_blob_key,
        serialize_bm_internal, BLOB_KEY, BM_TOMBSTONED_IDS, BM_USED_IDS,
    },
    AccountId, CollectionId, ColumnFamily, DocumentId, JMAPStore, Store, StoreError,
};

const DELETE_BATCH_SIZE: usize = 1000;

impl<'x, T> JMAPStore<T>
where
    T: Store<'x> + 'static,
{
    pub async fn delete(&self, cf: ColumnFamily, key: Vec<u8>) -> crate::Result<()> {
        let db = self.db.clone();
        self.spawn_blocking(move || db.delete(cf, key)).await
    }

    /*
    //TODO delete blobs
    fn delete_account(&self, account: AccountId) -> crate::Result<()> {
        let mut batch = WriteBatch::default();
        let mut batch_size = 0;

        for (cf, prefix) in [
            (self.get_handle("values")?, serialize_a_key_leb128(account)),
            (self.get_handle("indexes")?, serialize_a_key_be(account)),
            (self.get_handle("bitmaps")?, serialize_a_key_be(account)),
        ] {
            for (key, _) in self
                .db
                .iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward))
            {
                if key.starts_with(&prefix) {
                    batch.delete_cf(&cf, key);
                    batch_size += 1;

                    if batch_size == DELETE_BATCH_SIZE {
                        self.db
                            .write(batch)
                            .map_err(|e| StoreError::InternalError(e.to_string()))?;
                        batch = WriteBatch::default();
                        batch_size = 0;
                    }
                } else {
                    break;
                }
            }
        }

        if batch_size > 0 {
            self.db
                .write(batch)
                .map_err(|e| StoreError::InternalError(e.to_string()))?;
        }

        Ok(())
    }

    fn delete_collection(&self, account: AccountId, collection: CollectionId) -> crate::Result<()> {
        let mut batch = WriteBatch::default();
        let mut batch_size = 0;

        for (cf, prefix) in [
            (
                self.get_handle("values")?,
                serialize_ac_key_leb128(account, collection),
            ),
            (
                self.get_handle("indexes")?,
                serialize_ac_key_be(account, collection),
            ),
        ] {
            for (key, _) in self
                .db
                .iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward))
            {
                if key.starts_with(&prefix) {
                    batch.delete_cf(&cf, key);
                    batch_size += 1;

                    if batch_size == DELETE_BATCH_SIZE {
                        self.db
                            .write(batch)
                            .map_err(|e| StoreError::InternalError(e.to_string()))?;
                        batch = WriteBatch::default();
                        batch_size = 0;
                    }
                } else {
                    break;
                }
            }
        }

        let cf_bitmaps = self.get_handle("bitmaps")?;
        let doc_list_key =
            serialize_bm_internal(account, collection, BM_USED_IDS).into_boxed_slice();
        let tombstone_key =
            serialize_bm_internal(account, collection, BM_TOMBSTONED_IDS).into_boxed_slice();
        let prefix = serialize_a_key_leb128(account);

        for (key, _) in self
            .db
            .iterator_cf(&cf_bitmaps, IteratorMode::From(&prefix, Direction::Forward))
        {
            if !key.starts_with(&prefix) {
                break;
            } else if (key.len() > 3 && key[key.len() - 3] == collection)
                || key == doc_list_key
                || key == tombstone_key
            {
                batch.delete_cf(&cf_bitmaps, key);
                batch_size += 1;

                if batch_size == DELETE_BATCH_SIZE {
                    self.db
                        .write(batch)
                        .map_err(|e| StoreError::InternalError(e.to_string()))?;
                    batch = WriteBatch::default();
                    batch_size = 0;
                }
            }
        }

        if batch_size > 0 {
            self.db
                .write(batch)
                .map_err(|e| StoreError::InternalError(e.to_string()))?;
        }

        Ok(())
    }

    fn purge_tombstoned(&self, account: AccountId, collection: CollectionId) -> crate::Result<()> {
        let documents = if let Some(documents) = self.get_tombstoned_ids(account, collection)? {
            documents.bitmap
        } else {
            return Ok(());
        };
        let mut batch = WriteBatch::default();
        let mut batch_size = 0;

        for (cf, prefix, deserialize_fnc, delete_blobs) in [
            (
                self.get_handle("values")?,
                serialize_ac_key_leb128(account, collection),
                deserialize_document_id_from_leb128 as fn(&[u8]) -> Option<DocumentId>,
                true,
            ),
            (
                self.get_handle("indexes")?,
                serialize_ac_key_be(account, collection),
                deserialize_index_document_id as fn(&[u8]) -> Option<DocumentId>,
                false,
            ),
        ] {
            for (key, value) in self
                .db
                .iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward))
            {
                if !key.starts_with(&prefix) {
                    break;
                }
                if key.len() > prefix.len() {
                    if let Some(doc_id) = deserialize_fnc(&key[prefix.len()..]) {
                        if documents.contains(doc_id) {
                            if delete_blobs
                                && key.ends_with(BLOB_KEY)
                                && serialize_blob_key(account, collection, doc_id)[..] == key[..]
                            {
                                serialize_blob_keys_from_value(&value)
                                    .ok_or(StoreError::DataCorruption)?
                                    .into_iter()
                                    .for_each(|key| {
                                        batch.merge_cf(&cf, &key, (-1i64).to_le_bytes());
                                    });

                                batch_size += 1;
                            }

                            batch.delete_cf(&cf, key);
                            batch_size += 1;

                            if batch_size >= DELETE_BATCH_SIZE {
                                self.db
                                    .write(batch)
                                    .map_err(|e| StoreError::InternalError(e.to_string()))?;
                                batch = WriteBatch::default();
                                batch_size = 0;
                            }
                        }
                    } else {
                        return Err(StoreError::InternalError(
                            "Failed to deserialize document id".into(),
                        ));
                    }
                }
            }
        }

        let cf_bitmaps = self.get_handle("bitmaps")?;
        let prefix = serialize_a_key_leb128(account);

        // TODO delete files using a separate process
        // TODO delete empty bitmaps

        for (key, value) in self
            .db
            .iterator_cf(&cf_bitmaps, IteratorMode::From(&prefix, Direction::Forward))
        {
            if !key.starts_with(&prefix) {
                break;
            } else if key.len() > 3 && key[key.len() - 3] == collection {
                let mut bm = into_bitmap(&value)?;
                bm.bitand_assign(&documents);

                if !bm.is_empty() {
                    batch.merge_cf(&cf_bitmaps, key, clear_bits(bm.iter()));
                    batch_size += 1;

                    if batch_size == DELETE_BATCH_SIZE {
                        self.db
                            .write(batch)
                            .map_err(|e| StoreError::InternalError(e.to_string()))?;
                        batch = WriteBatch::default();
                        batch_size = 0;
                    }
                }
            }
        }

        batch.merge_cf(
            &cf_bitmaps,
            serialize_bm_internal(account, collection, BM_USED_IDS),
            clear_bits(documents.iter()),
        );

        batch.merge_cf(
            &cf_bitmaps,
            serialize_bm_internal(account, collection, BM_TOMBSTONED_IDS),
            clear_bits(documents.iter()),
        );

        self.db
            .write(batch)
            .map_err(|e| StoreError::InternalError(e.to_string()))?;

        self.doc_id_cache
            .invalidate(&IdCacheKey::new(account, collection));

        Ok(())
    }*/
}
