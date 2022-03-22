use crate::{JMAPStore, Store};

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
}
