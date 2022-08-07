use roaring::RoaringBitmap;

use crate::blob::BLOB_HASH_LEN;
use crate::serialize::key::BitmapKey;
use crate::serialize::leb128::Leb128;
use crate::{AccountId, ColumnFamily, Direction, JMAPStore, Store};

use super::operation::WriteOperation;

const DELETE_BATCH_SIZE: usize = 500;

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn delete_accounts(&self, account_ids: &RoaringBitmap) -> crate::Result<()> {
        let mut batch = Vec::with_capacity(64);

        // Delete values
        for (key, _) in self
            .db
            .iterator(ColumnFamily::Values, &[], Direction::Forward)?
        {
            let mut bytes = key.iter();
            if let Some(account_id) = AccountId::from_leb128_it(&mut bytes) {
                let do_delete = if account_ids.contains(account_id) {
                    true
                } else if matches!(bytes.next(), Some(collection) if *collection == u8::MAX) {
                    // Shared account
                    matches!(AccountId::from_leb128_it(&mut bytes), Some(shared_account_id) if account_ids.contains(shared_account_id))
                } else {
                    false
                };

                if do_delete {
                    batch.push(WriteOperation::Delete {
                        cf: ColumnFamily::Values,
                        key: key.to_vec(),
                    });
                    if batch.len() == DELETE_BATCH_SIZE {
                        self.db.write(batch)?;
                        batch = Vec::with_capacity(64);
                    }
                }
            }
        }

        // Delete indexes
        for (key, _) in self
            .db
            .iterator(ColumnFamily::Indexes, &[], Direction::Forward)?
        {
            if let Some((account_id, _)) = AccountId::from_leb128_bytes(&key) {
                if account_ids.contains(account_id) {
                    batch.push(WriteOperation::Delete {
                        cf: ColumnFamily::Indexes,
                        key: key.to_vec(),
                    });
                    if batch.len() == DELETE_BATCH_SIZE {
                        self.db.write(batch)?;
                        batch = Vec::with_capacity(64);
                    }
                }
            }
        }

        // Delete linked blobs
        for (key, _) in self
            .db
            .iterator(ColumnFamily::Blobs, &[], Direction::Forward)?
        {
            if let Some((account_id, _)) = key
                .get(BLOB_HASH_LEN + 1..)
                .and_then(AccountId::from_leb128_bytes)
            {
                if account_ids.contains(account_id) {
                    batch.push(WriteOperation::Delete {
                        cf: ColumnFamily::Blobs,
                        key: key.to_vec(),
                    });
                    if batch.len() == DELETE_BATCH_SIZE {
                        self.db.write(batch)?;
                        batch = Vec::with_capacity(64);
                    }
                }
            }
        }

        // Delete bitmaps
        for (key, _) in self
            .db
            .iterator(ColumnFamily::Bitmaps, &[], Direction::Forward)?
        {
            if matches!(BitmapKey::deserialize_account_id(&key), Some(account_id) if account_ids.contains(account_id))
            {
                batch.push(WriteOperation::Delete {
                    cf: ColumnFamily::Bitmaps,
                    key: key.to_vec(),
                });
                if batch.len() == DELETE_BATCH_SIZE {
                    self.db.write(batch)?;
                    batch = Vec::with_capacity(64);
                }
            }
        }

        if !batch.is_empty() {
            self.db.write(batch)?;
        }

        Ok(())
    }
}
