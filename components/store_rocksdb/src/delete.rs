use std::ops::BitAndAssign;

use rocksdb::{Direction, IteratorMode, WriteBatch};
use store::{
    serialize::{
        deserialize_document_id_from_leb128, deserialize_index_document_id, serialize_a_key_be,
        serialize_a_key_leb128, serialize_ac_key_be, serialize_ac_key_leb128,
        serialize_bm_internal, BM_FREED_IDS, BM_TOMBSTONED_IDS, BM_USED_IDS,
    },
    AccountId, CollectionId, DocumentId, StoreDelete, StoreError, StoreTombstone,
};

use crate::{
    bitmaps::{clear_bits, into_bitmap, set_bits, RocksDBDocumentSet},
    RocksDBStore,
};

const DELETE_BATCH_SIZE: usize = 1000;

impl StoreDelete for RocksDBStore {
    fn delete_account(&self, account: AccountId) -> store::Result<()> {
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

    fn delete_collection(&self, account: AccountId, collection: CollectionId) -> store::Result<()> {
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

    fn delete_document_bulk(
        &self,
        account: AccountId,
        collection: CollectionId,
        documents: &[DocumentId],
    ) -> store::Result<()> {
        self.db
            .merge_cf(
                &self.get_handle("bitmaps")?,
                &serialize_bm_internal(account, collection, BM_TOMBSTONED_IDS),
                &set_bits(documents.iter().copied()),
            )
            .map_err(|e| StoreError::InternalError(e.into_string()))
    }
}

impl StoreTombstone for RocksDBStore {
    fn get_tombstoned_ids(
        &self,
        account: AccountId,
        collection: CollectionId,
    ) -> crate::Result<Option<RocksDBDocumentSet>> {
        self.get_bitmap(
            &self.get_handle("bitmaps")?,
            &serialize_bm_internal(account, collection, BM_TOMBSTONED_IDS),
        )
        .map(|bm| match bm {
            Some(bm) if !bm.is_empty() => RocksDBDocumentSet::from_roaring(bm).into(),
            _ => None,
        })
    }

    fn purge_tombstoned(&self, account: AccountId, collection: CollectionId) -> store::Result<()> {
        let documents = if let Some(documents) = self.get_tombstoned_ids(account, collection)? {
            documents.bitmap
        } else {
            return Ok(());
        };
        let mut batch = WriteBatch::default();
        let mut batch_size = 0;

        for (cf, prefix, deserialize_fnc) in [
            (
                self.get_handle("values")?,
                serialize_ac_key_leb128(account, collection),
                deserialize_document_id_from_leb128 as fn(&[u8]) -> Option<DocumentId>,
            ),
            (
                self.get_handle("indexes")?,
                serialize_ac_key_be(account, collection),
                deserialize_index_document_id as fn(&[u8]) -> Option<DocumentId>,
            ),
        ] {
            for (key, _) in self
                .db
                .iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward))
            {
                if !key.starts_with(&prefix) {
                    break;
                }
                if key.len() > prefix.len() {
                    if let Some(doc_id) = deserialize_fnc(&key[prefix.len()..]) {
                        if documents.contains(doc_id) {
                            batch.delete_cf(&cf, key);
                            batch_size += 1;

                            if batch_size == DELETE_BATCH_SIZE {
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

        // Lock collection before modifying bitmaps
        let _collection_lock = self.lock_collection(account, collection)?;

        for (key, value) in self
            .db
            .iterator_cf(&cf_bitmaps, IteratorMode::From(&prefix, Direction::Forward))
        {
            if !key.starts_with(&prefix) {
                break;
            } else if key.len() > 3 && key[key.len() - 3] == collection {
                let mut bm = into_bitmap(&value)?;
                let total_docs = bm.len();
                bm.bitand_assign(&documents);
                let matched_docs = bm.len();

                if matched_docs > 0 {
                    if matched_docs == total_docs {
                        // Bitmap is empty, delete key
                        batch.delete_cf(&cf_bitmaps, key);
                    } else {
                        batch.merge_cf(&cf_bitmaps, key, clear_bits(bm.iter()));
                    }
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
            serialize_bm_internal(account, collection, BM_FREED_IDS),
            set_bits(documents.iter()),
        );

        batch.merge_cf(
            &cf_bitmaps,
            serialize_bm_internal(account, collection, BM_TOMBSTONED_IDS),
            clear_bits(documents.iter()),
        );

        self.db
            .write(batch)
            .map_err(|e| StoreError::InternalError(e.to_string()))?;

        Ok(())
    }
}
