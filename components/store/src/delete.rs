use std::ops::BitAndAssign;

use roaring::RoaringBitmap;

use crate::leb128::Leb128;
use crate::serialize::BitmapKey;
use crate::{
    bitmap::clear_bits,
    blob::BlobEntries,
    id::IdCacheKey,
    serialize::{IndexKey, StoreDeserialize, StoreSerialize, ValueKey, BLOB_KEY},
    AccountId, Collection, ColumnFamily, Direction, DocumentId, JMAPStore, Store, StoreError,
    WriteOperation,
};

const DELETE_BATCH_SIZE: usize = 1000;

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
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

    fn delete_collection(&self, account: AccountId, collection: Collection) -> crate::Result<()> {
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
    }*/

    pub fn purge_tombstoned(
        &self,
        account: AccountId,
        collection: Collection,
    ) -> crate::Result<()> {
        let documents = if let Some(documents) = self.get_tombstoned_ids(account, collection)? {
            documents
        } else {
            return Ok(());
        };

        let mut batch = vec![];

        for (cf, prefix, deserialize_fnc, delete_blobs) in [
            (
                ColumnFamily::Values,
                ValueKey::serialize_collection(account, collection),
                (|bytes| DocumentId::from_leb128_bytes(bytes)?.0.into())
                    as fn(&[u8]) -> Option<DocumentId>,
                true,
            ),
            (
                ColumnFamily::Indexes,
                IndexKey::serialize_collection(account, collection),
                IndexKey::deserialize_document_id as fn(&[u8]) -> Option<DocumentId>,
                false,
            ),
        ] {
            for (key, value) in self.db.iterator(cf, &prefix, Direction::Forward)? {
                if !key.starts_with(&prefix) {
                    break;
                }
                if key.len() > prefix.len() {
                    if let Some(doc_id) = deserialize_fnc(&key[prefix.len()..]) {
                        if documents.contains(doc_id) {
                            if delete_blobs
                                && key.ends_with(BLOB_KEY)
                                && ValueKey::serialize_document_blob(account, collection, doc_id)[..]
                                    == key[..]
                            {
                                BlobEntries::deserialize(&value)
                                    .ok_or(StoreError::DataCorruption)?
                                    .items
                                    .into_iter()
                                    .for_each(|key| {
                                        batch.push(WriteOperation::merge(
                                            cf,
                                            key.hash,
                                            (-1i64).serialize().unwrap(),
                                        ));
                                    });
                            }
                            batch.push(WriteOperation::delete(cf, key.to_vec()));

                            if batch.len() >= DELETE_BATCH_SIZE {
                                self.db.write(batch)?;
                                batch = vec![];
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

        let prefix = BitmapKey::serialize_account(account);

        // TODO delete files using a separate process
        // TODO delete empty bitmaps

        let collection_u8 = collection as u8;
        for (key, value) in self
            .db
            .iterator(ColumnFamily::Bitmaps, &prefix, Direction::Forward)?
        {
            if !key.starts_with(&prefix) {
                break;
            } else if key.len() > 3 && key[key.len() - 3] == collection_u8 {
                let mut bm =
                    RoaringBitmap::deserialize(&value).ok_or(StoreError::DataCorruption)?;
                bm.bitand_assign(&documents);

                if !bm.is_empty() {
                    batch.push(WriteOperation::merge(
                        ColumnFamily::Bitmaps,
                        key.to_vec(),
                        clear_bits(bm.iter()),
                    ));

                    if batch.len() == DELETE_BATCH_SIZE {
                        self.db.write(batch)?;
                        batch = vec![];
                    }
                }
            }
        }

        batch.push(WriteOperation::merge(
            ColumnFamily::Bitmaps,
            BitmapKey::serialize_used_ids(account, collection),
            clear_bits(documents.iter()),
        ));
        batch.push(WriteOperation::merge(
            ColumnFamily::Bitmaps,
            BitmapKey::serialize_tombstoned_ids(account, collection),
            clear_bits(documents.iter()),
        ));

        self.db.write(batch)?;

        self.doc_id_cache
            .invalidate(&IdCacheKey::new(account, collection));

        Ok(())
    }
}
