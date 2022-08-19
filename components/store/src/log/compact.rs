use super::changes::ChangeId;
use super::raft::LogIndex;
use crate::core::bitmap::Bitmap;
use crate::log::entry::Entry;
use crate::log::raft::{RaftId, TermId};
use crate::serialize::key::LogKey;
use crate::serialize::leb128::{Leb128Iterator, Leb128Vec};
use crate::serialize::{DeserializeBigEndian, StoreDeserialize};
use crate::write::batch;
use crate::{
    AccountId, Collection, ColumnFamily, Direction, JMAPStore, Store, StoreError, WriteOperation,
};
use ahash::AHashMap;
use roaring::{RoaringBitmap, RoaringTreemap};
use tracing::debug;

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn compact_bitmaps(&self) -> crate::Result<()> {
        // This function is necessary as RocksDB does not call compaction filter on
        // values that inserted using a merge.
        for (key, value) in self
            .db
            .iterator(ColumnFamily::Bitmaps, &[], Direction::Forward)?
        {
            match RoaringBitmap::deserialize(&value) {
                Some(bm) if bm.is_empty() => {
                    self.db.delete(ColumnFamily::Bitmaps, &key)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn compact_log(&self, max_changes: u64) -> crate::Result<()> {
        if let (Some(first_index), Some(last_index)) = (
            self.get_next_raft_id(RaftId::new(0, 0))?.map(|v| v.index),
            self.get_prev_raft_id(RaftId::new(TermId::MAX, LogIndex::MAX))?
                .map(|v| v.index),
        ) {
            if last_index > first_index && last_index - first_index > max_changes {
                debug!(
                    "Compacting {} entries up to id {}.",
                    last_index - first_index - max_changes,
                    last_index - max_changes + 1
                );
                self.compact_log_up_to(last_index - max_changes + 1)?;
            } else {
                debug!(
                    "No need to compact log, {} entries found.",
                    last_index - first_index
                );
            }
        } else {
            debug!("No logs found to compact.");
        }

        Ok(())
    }

    pub fn compact_log_up_to(&self, up_to: ChangeId) -> crate::Result<()> {
        let mut current_account_id = 0;
        let mut current_collection = Collection::None;

        let mut inserted_ids = RoaringTreemap::new();
        let mut write_batch = Vec::new();
        let mut has_changes = false;

        for (key, value) in self.db.iterator(
            ColumnFamily::Logs,
            &[LogKey::CHANGE_KEY_PREFIX],
            Direction::Forward,
        )? {
            if !key.starts_with(&[LogKey::CHANGE_KEY_PREFIX]) {
                break;
            }
            let account_id = (&key[..])
                .deserialize_be_u32(LogKey::ACCOUNT_POS)
                .ok_or_else(|| {
                    StoreError::InternalError(format!(
                        "Failed to deserialize account id from changelog key: [{:?}]",
                        key
                    ))
                })?;
            let collection: Collection = (*key.get(LogKey::COLLECTION_POS).ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to deserialize collection from changelog key: [{:?}]",
                    key
                ))
            })?)
            .into();

            if account_id != current_account_id || collection != current_collection {
                if !write_batch.is_empty() {
                    self.db.write(serialize_snapshot(
                        write_batch,
                        &mut inserted_ids,
                        current_account_id,
                        current_collection,
                        up_to,
                    )?)?;
                    write_batch = Vec::new();
                }
                current_account_id = account_id;
                current_collection = collection;
            }

            let change_id = LogKey::deserialize_change_id(&key).ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to deserialize changelog key for [{}/{:?}]: [{:?}]",
                    account_id, collection, key
                ))
            })?;

            if change_id > up_to {
                continue;
            } else if change_id != up_to {
                write_batch.push(WriteOperation::delete(ColumnFamily::Logs, key.to_vec()));
            } else {
                has_changes = true;
            }

            deserialize_inserts(&mut inserted_ids, &value).ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to deserialize changelog value for [{}/{:?}]: [{:?}]",
                    account_id, collection, key
                ))
            })?;
        }

        if !has_changes {
            return Ok(());
        } else if !write_batch.is_empty() {
            self.db.write(serialize_snapshot(
                write_batch,
                &mut inserted_ids,
                current_account_id,
                current_collection,
                up_to,
            )?)?;
            write_batch = Vec::new();
        }

        let mut last_term = TermId::MAX;
        let mut changed_accounts = AHashMap::default();

        for (key, value) in self.db.iterator(
            ColumnFamily::Logs,
            &[LogKey::RAFT_KEY_PREFIX],
            Direction::Forward,
        )? {
            if !key.starts_with(&[LogKey::RAFT_KEY_PREFIX]) {
                break;
            }

            let raft_id = LogKey::deserialize_raft(&key).ok_or_else(|| {
                StoreError::InternalError(format!("Corrupted raft entry for [{:?}]", key))
            })?;

            if raft_id.index <= up_to {
                match Entry::deserialize(&value).ok_or_else(|| {
                    StoreError::InternalError(format!("Corrupted raft entry for [{:?}]", key))
                })? {
                    Entry::Item {
                        account_id,
                        changed_collections,
                    } => {
                        changed_accounts
                            .entry(account_id)
                            .or_insert_with(Bitmap::default)
                            .union(&changed_collections);
                    }
                    Entry::Snapshot {
                        changed_accounts: new_changed_accounts,
                    } => {
                        debug_assert!(changed_accounts.is_empty());
                        for (new_changed_collection, new_changed_accounts) in new_changed_accounts {
                            for new_changed_account_id in new_changed_accounts {
                                changed_accounts
                                    .entry(new_changed_account_id)
                                    .or_insert_with(Bitmap::default)
                                    .union(&new_changed_collection);
                            }
                        }
                    }
                };

                if raft_id.index != up_to {
                    write_batch.push(WriteOperation::delete(ColumnFamily::Logs, key.to_vec()));
                } else {
                    last_term = raft_id.term;
                }
            } else {
                break;
            }
        }

        debug_assert_ne!(last_term, ChangeId::MAX);

        // Serialize raft snapshot
        let mut changed_collections = AHashMap::default();
        let total_accounts = changed_accounts.len();
        for (account_id, collections) in changed_accounts {
            changed_collections
                .entry(collections)
                .or_insert_with(Vec::new)
                .push(account_id);
        }
        let mut bytes = Vec::with_capacity(
            (total_accounts * std::mem::size_of::<AccountId>())
                + (changed_collections.len()
                    * (std::mem::size_of::<Collection>() + std::mem::size_of::<usize>()))
                + 1
                + std::mem::size_of::<usize>(),
        );
        bytes.push(batch::Change::SNAPSHOT);
        bytes.push_leb128(changed_collections.len());
        for (collections, account_ids) in changed_collections {
            bytes.push_leb128(collections.bitmap);
            bytes.push_leb128(account_ids.len());
            for account_id in account_ids {
                bytes.push_leb128(account_id);
            }
        }
        write_batch.push(WriteOperation::set(
            ColumnFamily::Logs,
            LogKey::serialize_raft(&RaftId::new(last_term, up_to)),
            bytes,
        ));
        self.db.write(write_batch)?;

        Ok(())
    }
}

fn serialize_snapshot(
    mut write_batch: Vec<WriteOperation>,
    inserted_ids: &mut RoaringTreemap,
    current_account_id: AccountId,
    current_collection: Collection,
    last_change_id: ChangeId,
) -> crate::Result<Vec<WriteOperation>> {
    let mut bytes = Vec::with_capacity(1 + inserted_ids.serialized_size());
    bytes.push(batch::Change::SNAPSHOT);
    inserted_ids.serialize_into(&mut bytes).map_err(|err| {
        StoreError::InternalError(format!(
            "Failed to serialize inserted ids for [{}/{:?}]: [{:?}]",
            current_account_id, current_collection, err
        ))
    })?;
    write_batch.push(WriteOperation::set(
        ColumnFamily::Logs,
        LogKey::serialize_change(current_account_id, current_collection, last_change_id),
        bytes,
    ));
    inserted_ids.clear();
    Ok(write_batch)
}

fn deserialize_inserts(inserted_ids: &mut RoaringTreemap, bytes: &[u8]) -> Option<()> {
    match *bytes.first()? {
        batch::Change::ENTRY => {
            let mut bytes_it = bytes.get(1..)?.iter();
            let total_inserts: usize = bytes_it.next_leb128()?;
            let total_updates: usize = bytes_it.next_leb128()?;
            let total_child_updates: usize = bytes_it.next_leb128()?;
            let total_deletes: usize = bytes_it.next_leb128()?;

            for _ in 0..total_inserts {
                inserted_ids.insert(bytes_it.next_leb128()?);
            }

            // Skip updates
            for _ in 0..total_updates {
                bytes_it.skip_leb128()?;
            }

            // Skip child updates
            for _ in 0..total_child_updates {
                bytes_it.skip_leb128()?;
            }

            for _ in 0..total_deletes {
                inserted_ids.remove(bytes_it.next_leb128()?);
            }
        }
        batch::Change::SNAPSHOT => {
            debug_assert!(inserted_ids.is_empty());
            *inserted_ids = RoaringTreemap::deserialize_unchecked_from(bytes.get(1..)?).ok()?;
        }
        _ => {
            return None;
        }
    }
    Some(())
}
