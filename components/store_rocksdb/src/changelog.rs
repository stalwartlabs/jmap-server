use std::{array::TryFromSliceError, convert::TryInto};

use rocksdb::{Direction, IteratorMode};
use store::{
    leb128::Leb128,
    serialize::{
        serialize_changelog_key, serialize_stored_key_global, StoreDeserialize,
        COLLECTION_PREFIX_LEN,
    },
    AccountId, ChangeLog, ChangeLogEntry, ChangeLogId, ChangeLogQuery, CollectionId,
    StoreChangeLog, StoreError,
};

use crate::RocksDBStore;

#[derive(Default)]
pub struct ChangeLogWriter {
    pub inserts: Vec<ChangeLogId>,
    pub updates: Vec<ChangeLogId>,
    pub deletes: Vec<ChangeLogId>,
}

impl ChangeLogWriter {
    pub fn new() -> Self {
        ChangeLogWriter::default()
    }

    pub fn serialize(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            (self.inserts.len() + self.updates.len() + self.deletes.len() + 3)
                * std::mem::size_of::<usize>(),
        );
        self.inserts.len().to_leb128_bytes(&mut buf);
        self.updates.len().to_leb128_bytes(&mut buf);
        self.deletes.len().to_leb128_bytes(&mut buf);
        for list in [self.inserts, self.updates, self.deletes] {
            for id in list {
                id.to_leb128_bytes(&mut buf);
            }
        }
        buf
    }
}

fn deserialize(changelog: &mut ChangeLog, bytes: &[u8]) -> store::Result<()> {
    let mut bytes_it = bytes.iter();
    let total_inserts = usize::from_leb128_it(&mut bytes_it).ok_or_else(|| {
        StoreError::DeserializeError(format!(
            "Failed to deserialize total inserts from bytes: {:?}",
            bytes
        ))
    })?;
    let total_updates = usize::from_leb128_it(&mut bytes_it).ok_or_else(|| {
        StoreError::DeserializeError(format!(
            "Failed to deserialize total updates from bytes: {:?}",
            bytes
        ))
    })?;
    let total_deletes = usize::from_leb128_it(&mut bytes_it).ok_or_else(|| {
        StoreError::DeserializeError(format!(
            "Failed to deserialize total deletes from bytes: {:?}",
            bytes
        ))
    })?;

    if total_inserts > 0 {
        for _ in 0..total_inserts {
            let id = ChangeLogId::from_leb128_it(&mut bytes_it).ok_or_else(|| {
                StoreError::DeserializeError(format!(
                    "Failed to deserialize change id from bytes: {:?}",
                    bytes
                ))
            })?;
            changelog.changes.push(ChangeLogEntry::Insert(id));
        }
    }

    if total_updates > 0 {
        'update_outer: for _ in 0..total_updates {
            let id = ChangeLogId::from_leb128_it(&mut bytes_it).ok_or_else(|| {
                StoreError::DeserializeError(format!(
                    "Failed to deserialize change id from bytes: {:?}",
                    bytes
                ))
            })?;

            if !changelog.changes.is_empty() {
                let mut update_idx = None;
                for (idx, change) in changelog.changes.iter().enumerate() {
                    match change {
                        ChangeLogEntry::Insert(insert_id) => {
                            if *insert_id == id {
                                // Item updated after inserted, no need to count this change.
                                continue 'update_outer;
                            }
                        }
                        ChangeLogEntry::Update(update_id) => {
                            if *update_id == id {
                                update_idx = Some(idx);
                                break;
                            }
                        }
                        _ => (),
                    }
                }

                // Move update to the front
                if let Some(idx) = update_idx {
                    changelog.changes.remove(idx);
                }
            }

            changelog.changes.push(ChangeLogEntry::Update(id));
        }
    }

    if total_deletes > 0 {
        'delete_outer: for _ in 0..total_deletes {
            let id = ChangeLogId::from_leb128_it(&mut bytes_it).ok_or_else(|| {
                StoreError::DeserializeError(format!(
                    "Failed to deserialize change id from bytes: {:?}",
                    bytes
                ))
            })?;

            if !changelog.changes.is_empty() {
                let mut update_idx = None;
                for (idx, change) in changelog.changes.iter().enumerate() {
                    match change {
                        ChangeLogEntry::Insert(insert_id) => {
                            if *insert_id == id {
                                changelog.changes.remove(idx);
                                continue 'delete_outer;
                            }
                        }
                        ChangeLogEntry::Update(update_id) => {
                            if *update_id == id {
                                update_idx = Some(idx);
                                break;
                            }
                        }
                        _ => (),
                    }
                }
                if let Some(idx) = update_idx {
                    changelog.changes.remove(idx);
                }
            }

            changelog.changes.push(ChangeLogEntry::Delete(id));
        }
    }

    Ok(())
}

impl From<ChangeLogWriter> for Vec<u8> {
    fn from(writer: ChangeLogWriter) -> Self {
        writer.serialize()
    }
}

impl StoreChangeLog for RocksDBStore {
    fn get_last_change_id(
        &self,
        account: AccountId,
        collection: CollectionId,
    ) -> store::Result<Option<ChangeLogId>> {
        Ok(
            if let Some(change_id) = self
                .db
                .get_cf(
                    &self.get_handle("values")?,
                    &serialize_stored_key_global(account.into(), collection.into(), None),
                )
                .map_err(|e| StoreError::InternalError(e.into_string()))?
            {
                Some(change_id.deserialize()?)
            } else {
                None
            },
        )
    }

    fn get_changes(
        &self,
        account: AccountId,
        collection: CollectionId,
        query: ChangeLogQuery,
    ) -> store::Result<Option<ChangeLog>> {
        let mut changelog = ChangeLog::default();
        let (is_inclusive, mut match_from_change_id, from_change_id, to_change_id) = match query {
            ChangeLogQuery::All => (false, false, 0, 0),
            ChangeLogQuery::Since(change_id) => (false, true, change_id, 0),
            ChangeLogQuery::SinceInclusive(change_id) => (true, true, change_id, 0),
            ChangeLogQuery::RangeInclusive(from_change_id, to_change_id) => {
                (true, true, from_change_id, to_change_id)
            }
        };
        let key = serialize_changelog_key(account, collection, changelog.from_change_id);
        let prefix = &key[0..COLLECTION_PREFIX_LEN];
        let mut is_first = true;

        for (key, value) in self.db.iterator_cf(
            &self.get_handle("log")?,
            IteratorMode::From(&key, Direction::Forward),
        ) {
            if !key.starts_with(prefix) {
                break;
            }
            let change_id = ChangeLogId::from_be_bytes(
                key.get(COLLECTION_PREFIX_LEN..)
                    .ok_or_else(|| {
                        StoreError::InternalError(format!("Failed to changelog key: {:?}", key))
                    })?
                    .try_into()
                    .map_err(|e: TryFromSliceError| StoreError::InternalError(e.to_string()))?,
            );

            if match_from_change_id {
                if change_id != from_change_id {
                    return Ok(None);
                } else {
                    match_from_change_id = false;
                }
            }
            if change_id > from_change_id || (is_inclusive && change_id == from_change_id) {
                if to_change_id > 0 && change_id > to_change_id {
                    break;
                }
                if is_first {
                    changelog.from_change_id = change_id;
                    is_first = false;
                }
                changelog.to_change_id = change_id;
                deserialize(&mut changelog, value.as_ref())?;
            }
        }

        if is_first {
            changelog.from_change_id = from_change_id;
            changelog.to_change_id = if to_change_id > 0 {
                to_change_id
            } else {
                from_change_id
            };
        }

        Ok(Some(changelog))
    }
}
