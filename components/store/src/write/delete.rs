/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use roaring::RoaringBitmap;

use crate::blob::BLOB_HASH_LEN;
use crate::serialize::key::BitmapKey;
use crate::serialize::leb128::{Leb128Iterator, Leb128Reader};
use crate::serialize::DeserializeBigEndian;
use crate::{ColumnFamily, Direction, JMAPStore, Store};

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
            if let Some(account_id) = bytes.next_leb128() {
                let do_delete = if account_ids.contains(account_id) {
                    true
                } else if matches!(bytes.next(), Some(collection) if *collection == u8::MAX) {
                    // Shared account
                    matches!(bytes.next_leb128(), Some(shared_account_id) if account_ids.contains(shared_account_id))
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
            if let Some(account_id) = (&key[..]).deserialize_be_u32(0) {
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
            if let Some((account_id, _)) =
                key.get(BLOB_HASH_LEN + 1..).and_then(|b| b.read_leb128())
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
