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

use store::core::collection::Collection;
use store::core::error::StoreError;
use store::core::JMAPIdPrefix;
use store::log::changes::ChangeId;
use store::roaring::{RoaringBitmap, RoaringTreemap};
use store::serialize::key::LogKey;
use store::serialize::leb128::{Leb128Iterator, Leb128Reader, Leb128Vec};
use store::write::batch::{self};
use store::{AccountId, ColumnFamily, Direction, JMAPId, JMAPStore, Store};

#[derive(Debug)]
pub struct MergedChanges {
    pub inserts: RoaringBitmap,
    pub updates: RoaringBitmap,
    pub deletes: RoaringBitmap,
}

pub trait RaftStoreMerge {
    fn merge_changes(
        &self,
        account: AccountId,
        collection: Collection,
        from_id: ChangeId,
        to_id: ChangeId,
    ) -> store::Result<MergedChanges>;
}

impl<T> RaftStoreMerge for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn merge_changes(
        &self,
        account: AccountId,
        collection: Collection,
        from_id: ChangeId,
        to_id: ChangeId,
    ) -> store::Result<MergedChanges> {
        let mut changes = MergedChanges::new();

        let key = LogKey::serialize_change(
            account,
            collection,
            if from_id != ChangeId::MAX { from_id } else { 0 },
        );
        let prefix = &key[0..LogKey::CHANGE_ID_POS];

        for (key, value) in self
            .db
            .iterator(ColumnFamily::Logs, &key, Direction::Forward)?
        {
            if !key.starts_with(prefix) {
                break;
            }
            let change_id = LogKey::deserialize_change_id(&key).ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to deserialize changelog key for [{}/{:?}]: [{:?}]",
                    account, collection, key
                ))
            })?;

            if (change_id >= from_id || from_id == ChangeId::MAX) && change_id <= to_id {
                changes.deserialize_changes(&value).ok_or_else(|| {
                    StoreError::InternalError(format!(
                        "Failed to deserialize raft changes for [{}/{:?}]",
                        account, collection
                    ))
                })?;
            }
        }

        Ok(changes)
    }
}

impl Default for MergedChanges {
    fn default() -> Self {
        Self::new()
    }
}

impl MergedChanges {
    pub fn new() -> Self {
        Self {
            inserts: RoaringBitmap::new(),
            updates: RoaringBitmap::new(),
            deletes: RoaringBitmap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inserts.is_empty() && self.updates.is_empty() && self.deletes.is_empty()
    }

    pub fn deserialize_changes(&mut self, bytes: &[u8]) -> Option<()> {
        match *bytes.first()? {
            batch::Change::ENTRY => {
                let mut bytes_it = bytes.get(1..)?.iter();
                let total_inserts: usize = bytes_it.next_leb128()?;
                let total_updates: usize = bytes_it.next_leb128()?;
                let total_child_updates: usize = bytes_it.next_leb128()?;
                let total_deletes: usize = bytes_it.next_leb128()?;

                let mut inserted_ids = Vec::with_capacity(total_inserts);

                for _ in 0..total_inserts {
                    inserted_ids.push(bytes_it.next_leb128::<JMAPId>()?);
                }

                for _ in 0..total_updates {
                    let document_id = bytes_it.next_leb128::<JMAPId>()?.get_document_id();
                    if !self.inserts.contains(document_id) {
                        self.updates.insert(document_id);
                    }
                }

                // Skip child updates
                for _ in 0..total_child_updates {
                    bytes_it.skip_leb128()?;
                }

                for _ in 0..total_deletes {
                    let deleted_id = bytes_it.next_leb128::<JMAPId>()?;
                    let document_id = deleted_id.get_document_id();
                    let prefix_id = deleted_id.get_prefix_id();
                    if let Some(pos) = inserted_ids.iter().position(|&inserted_id| {
                        inserted_id.get_document_id() == document_id
                            && inserted_id.get_prefix_id() != prefix_id
                    }) {
                        // There was a prefix change, count this change as an update.

                        inserted_ids.remove(pos);
                        if !self.inserts.contains(document_id) {
                            self.updates.insert(document_id);
                        }
                    } else {
                        // This change is an actual deletion
                        if !self.inserts.remove(document_id) {
                            self.deletes.insert(document_id);
                        }
                        self.updates.remove(document_id);
                    }
                }

                for inserted_id in inserted_ids {
                    self.inserts.insert(inserted_id.get_document_id());
                }
            }
            batch::Change::SNAPSHOT => {
                debug_assert!(self.is_empty());
                RoaringTreemap::deserialize_unchecked_from(bytes.get(1..)?)
                    .ok()?
                    .into_iter()
                    .for_each(|id| {
                        self.inserts.insert(id.get_document_id());
                    });
            }
            _ => {
                return None;
            }
        }

        Some(())
    }

    pub fn serialize(&self) -> Option<Vec<u8>> {
        let insert_size = if !self.inserts.is_empty() {
            self.inserts.serialized_size()
        } else {
            0
        };
        let update_size = if !self.updates.is_empty() {
            self.updates.serialized_size()
        } else {
            0
        };
        let delete_size = if !self.deletes.is_empty() {
            self.deletes.serialized_size()
        } else {
            0
        };

        let mut bytes = Vec::with_capacity(
            insert_size + update_size + delete_size + (3 * std::mem::size_of::<usize>()),
        );

        bytes.push_leb128(insert_size);
        bytes.push_leb128(update_size);
        bytes.push_leb128(delete_size);

        if !self.inserts.is_empty() {
            self.inserts.serialize_into(&mut bytes).ok()?;
        }
        if !self.updates.is_empty() {
            self.updates.serialize_into(&mut bytes).ok()?;
        }
        if !self.deletes.is_empty() {
            self.deletes.serialize_into(&mut bytes).ok()?;
        }

        Some(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let (insert_size, mut read_bytes) = bytes.read_leb128::<usize>()?;
        let (update_size, read_bytes_) = bytes.get(read_bytes..)?.read_leb128::<usize>()?;
        read_bytes += read_bytes_;
        let (delete_size, read_bytes_) = bytes.get(read_bytes..)?.read_leb128::<usize>()?;
        read_bytes += read_bytes_;

        // This function is also called from the raft module using network data,
        // for that reason deserialize_from is used instead of deserialized_unchecked_from.
        let inserts = if insert_size > 0 {
            RoaringBitmap::deserialize_from(bytes.get(read_bytes..read_bytes + insert_size)?)
                .ok()?
        } else {
            RoaringBitmap::new()
        };
        read_bytes += insert_size;

        let updates = if update_size > 0 {
            RoaringBitmap::deserialize_from(bytes.get(read_bytes..read_bytes + update_size)?)
                .ok()?
        } else {
            RoaringBitmap::new()
        };
        read_bytes += update_size;

        let deletes = if delete_size > 0 {
            RoaringBitmap::deserialize_from(bytes.get(read_bytes..read_bytes + delete_size)?)
                .ok()?
        } else {
            RoaringBitmap::new()
        };

        Some(Self {
            inserts,
            updates,
            deletes,
        })
    }
}
