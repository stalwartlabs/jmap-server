use roaring::{RoaringBitmap, RoaringTreemap};

use crate::leb128::{skip_leb128_it, Leb128};

use crate::raft::{Entry, RaftId};
use crate::serialize::{DeserializeBigEndian, LogKey};
use crate::{
    batch, AccountId, Collection, ColumnFamily, Direction, JMAPId, JMAPStore, Store, StoreError,
    WriteOperation,
};

pub type ChangeId = u64;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Change {
    Insert(JMAPId),
    Update(JMAPId),
    ChildUpdate(JMAPId),
    Delete(JMAPId),
}

pub struct Changes {
    pub changes: Vec<Change>,
    pub from_change_id: ChangeId,
    pub to_change_id: ChangeId,
}

#[derive(Debug)]
pub enum Query {
    All,
    Since(ChangeId),
    SinceInclusive(ChangeId),
    RangeInclusive(ChangeId, ChangeId),
}

impl Default for Changes {
    fn default() -> Self {
        Self {
            changes: Vec::with_capacity(10),
            from_change_id: 0,
            to_change_id: 0,
        }
    }
}

impl Changes {
    pub fn deserialize(&mut self, bytes: &[u8]) -> Option<()> {
        match *bytes.get(0)? {
            batch::Change::ENTRY => {
                let mut bytes_it = bytes.get(1..)?.iter();
                let total_inserts = usize::from_leb128_it(&mut bytes_it)?;
                let total_updates = usize::from_leb128_it(&mut bytes_it)?;
                let total_child_updates = usize::from_leb128_it(&mut bytes_it)?;
                let total_deletes = usize::from_leb128_it(&mut bytes_it)?;

                if total_inserts > 0 {
                    for _ in 0..total_inserts {
                        self.changes
                            .push(Change::Insert(JMAPId::from_leb128_it(&mut bytes_it)?));
                    }
                }

                if total_updates > 0 || total_child_updates > 0 {
                    'update_outer: for change_pos in 0..(total_updates + total_child_updates) {
                        let id = JMAPId::from_leb128_it(&mut bytes_it)?;
                        let mut is_child_update = change_pos >= total_updates;

                        for (idx, change) in self.changes.iter().enumerate() {
                            match change {
                                Change::Insert(insert_id) if *insert_id == id => {
                                    // Item updated after inserted, no need to count this change.
                                    continue 'update_outer;
                                }
                                Change::Update(update_id) if *update_id == id => {
                                    // Move update to the front
                                    is_child_update = false;
                                    self.changes.remove(idx);
                                    break;
                                }
                                Change::ChildUpdate(update_id) if *update_id == id => {
                                    // Move update to the front
                                    self.changes.remove(idx);
                                    break;
                                }
                                _ => (),
                            }
                        }

                        self.changes.push(if !is_child_update {
                            Change::Update(id)
                        } else {
                            Change::ChildUpdate(id)
                        });
                    }
                }

                if total_deletes > 0 {
                    'delete_outer: for _ in 0..total_deletes {
                        let id = JMAPId::from_leb128_it(&mut bytes_it)?;

                        'delete_inner: for (idx, change) in self.changes.iter().enumerate() {
                            match change {
                                Change::Insert(insert_id) if *insert_id == id => {
                                    self.changes.remove(idx);
                                    continue 'delete_outer;
                                }
                                Change::Update(update_id) | Change::ChildUpdate(update_id)
                                    if *update_id == id =>
                                {
                                    self.changes.remove(idx);
                                    break 'delete_inner;
                                }
                                _ => (),
                            }
                        }

                        self.changes.push(Change::Delete(id));
                    }
                }
            }
            batch::Change::SNAPSHOT => {
                debug_assert!(self.changes.is_empty());
                RoaringTreemap::deserialize_unchecked_from(bytes.get(1..)?)
                    .ok()?
                    .into_iter()
                    .for_each(|id| self.changes.push(Change::Insert(id)));
            }
            _ => {
                return None;
            }
        }

        Some(())
    }
}

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn get_last_change_id(
        &self,
        account: AccountId,
        collection: Collection,
    ) -> crate::Result<Option<ChangeId>> {
        let match_key = LogKey::serialize_change(account, collection, ChangeId::MAX);

        if let Some((key, _)) = self
            .db
            .iterator(ColumnFamily::Logs, &match_key, Direction::Backward)?
            .into_iter()
            .next()
        {
            if key.starts_with(&match_key[0..LogKey::CHANGE_ID_POS]) {
                return Ok(Some(LogKey::deserialize_change_id(&key).ok_or_else(
                    || {
                        StoreError::InternalError(format!(
                            "Failed to deserialize changelog key for [{}/{:?}]: [{:?}]",
                            account, collection, key
                        ))
                    },
                )?));
            }
        }
        Ok(None)
    }

    pub fn get_changes(
        &self,
        account: AccountId,
        collection: Collection,
        query: Query,
    ) -> crate::Result<Option<Changes>> {
        let mut changelog = Changes::default();
        let (is_inclusive, from_change_id, to_change_id) = match query {
            Query::All => (true, 0, 0),
            Query::Since(change_id) => (false, change_id, 0),
            Query::SinceInclusive(change_id) => (true, change_id, 0),
            Query::RangeInclusive(from_change_id, to_change_id) => {
                (true, from_change_id, to_change_id)
            }
        };
        let key = LogKey::serialize_change(account, collection, from_change_id);
        let prefix = &key[0..LogKey::CHANGE_ID_POS];
        let mut is_first = true;

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

            if change_id > from_change_id || (is_inclusive && change_id == from_change_id) {
                if to_change_id > 0 && change_id > to_change_id {
                    break;
                }
                if is_first {
                    changelog.from_change_id = change_id;
                    is_first = false;
                }
                changelog.to_change_id = change_id;
                changelog.deserialize(&value).ok_or_else(|| {
                    StoreError::InternalError(format!(
                        "Failed to deserialize changelog for [{}/{:?}]: [{:?}]",
                        account, collection, query
                    ))
                })?;
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

    pub fn compact_log(&self, up_to: ChangeId) -> crate::Result<()> {
        let mut current_account_id = 0;
        let mut current_collection = Collection::None;

        let mut inserted_ids = RoaringTreemap::new();
        let mut write_batch = Vec::new();

        let mut last_change_id = ChangeId::MAX;

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
                        last_change_id,
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
            }

            last_change_id = change_id;

            write_batch.push(WriteOperation::delete(ColumnFamily::Logs, key.to_vec()));

            deserialize_inserts(&mut inserted_ids, &value).ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to deserialize changelog value for [{}/{:?}]: [{:?}]",
                    account_id, collection, key
                ))
            })?;
        }

        if last_change_id == ChangeId::MAX {
            return Ok(());
        } else if !write_batch.is_empty() {
            self.db.write(serialize_snapshot(
                write_batch,
                &mut inserted_ids,
                current_account_id,
                current_collection,
                last_change_id,
            )?)?;
            write_batch = Vec::new();
        }

        last_change_id = ChangeId::MAX;
        let mut last_term = 0;
        let mut changed_accounts = RoaringBitmap::new();

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
                last_change_id = raft_id.index;
                last_term = raft_id.term;

                match Entry::deserialize(&value).ok_or_else(|| {
                    StoreError::InternalError(format!("Corrupted raft entry for [{:?}]", key))
                })? {
                    Entry::Item { account_id, .. } => {
                        changed_accounts.insert(account_id);
                    }
                    Entry::Snapshot {
                        changed_accounts: new_changed_accounts,
                    } => {
                        debug_assert!(changed_accounts.is_empty());
                        changed_accounts = new_changed_accounts;
                    }
                };

                write_batch.push(WriteOperation::delete(ColumnFamily::Logs, key.to_vec()));
            } else {
                break;
            }
        }

        debug_assert_ne!(last_change_id, ChangeId::MAX);

        // Serialize raft snapshot
        let mut bytes = Vec::with_capacity(changed_accounts.serialized_size() + 1);
        bytes.push(batch::Change::SNAPSHOT);
        changed_accounts.serialize_into(&mut bytes).map_err(|err| {
            StoreError::InternalError(format!(
                "Failed to serialize inserted ids for [{}/{:?}]: [{:?}]",
                current_account_id, current_collection, err
            ))
        })?;
        write_batch.pop();
        write_batch.push(WriteOperation::set(
            ColumnFamily::Logs,
            LogKey::serialize_raft(&RaftId::new(last_term, last_change_id)),
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
    write_batch.pop();
    write_batch.push(WriteOperation::set(
        ColumnFamily::Logs,
        LogKey::serialize_change(current_account_id, current_collection, last_change_id),
        bytes,
    ));
    inserted_ids.clear();
    Ok(write_batch)
}

fn deserialize_inserts(inserted_ids: &mut RoaringTreemap, bytes: &[u8]) -> Option<()> {
    match *bytes.get(0)? {
        batch::Change::ENTRY => {
            let mut bytes_it = bytes.get(1..)?.iter();
            let total_inserts = usize::from_leb128_it(&mut bytes_it)?;
            let total_updates = usize::from_leb128_it(&mut bytes_it)?;
            let total_child_updates = usize::from_leb128_it(&mut bytes_it)?;
            let total_deletes = usize::from_leb128_it(&mut bytes_it)?;

            for _ in 0..total_inserts {
                inserted_ids.insert(JMAPId::from_leb128_it(&mut bytes_it)?);
            }

            // Skip updates
            for _ in 0..total_updates {
                skip_leb128_it(&mut bytes_it)?;
            }

            // Skip child updates
            for _ in 0..total_child_updates {
                skip_leb128_it(&mut bytes_it)?;
            }

            for _ in 0..total_deletes {
                inserted_ids.remove(JMAPId::from_leb128_it(&mut bytes_it)?);
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
