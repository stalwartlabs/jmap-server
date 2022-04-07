use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::atomic::Ordering;

use roaring::RoaringTreemap;

use crate::leb128::{skip_leb128_it, Leb128};

use crate::serialize::{DeserializeBigEndian, LogKey, StoreDeserialize, StoreSerialize};
use crate::{
    batch, AccountId, Collection, Collections, ColumnFamily, Direction, JMAPId, JMAPStore, Store,
    StoreError, WriteOperation,
};

pub type ChangeId = u64;
pub type TermId = u64;
pub type LogIndex = u64;

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

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct RaftId {
    pub term: TermId,
    pub index: LogIndex,
}

impl RaftId {
    pub fn new(term: TermId, index: LogIndex) -> Self {
        Self { term, index }
    }

    pub fn none() -> Self {
        Self {
            term: 0,
            index: LogIndex::MAX,
        }
    }

    pub fn is_none(&self) -> bool {
        self.index == LogIndex::MAX
    }
}

impl StoreSerialize for RaftId {
    fn serialize(&self) -> Option<Vec<u8>> {
        let mut bytes = Vec::with_capacity(std::mem::size_of::<RaftId>());
        self.term.to_leb128_writer(&mut bytes).ok()?;
        self.index.to_leb128_writer(&mut bytes).ok()?;
        bytes.into()
    }
}

impl StoreDeserialize for RaftId {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        let (term, bytes_read) = TermId::from_leb128_bytes(bytes)?;
        let (index, _) = TermId::from_leb128_bytes(bytes.get(bytes_read..)?)?;
        Some(Self { term, index })
    }
}

#[derive(Debug)]
pub enum Entry {
    Item {
        account_id: AccountId,
        changed_collections: Collections,
    },
    Snapshot {
        changed_accounts: Vec<(Collections, Vec<AccountId>)>,
    },
}

impl Entry {
    pub fn next_account(&mut self) -> Option<(AccountId, Collections)> {
        match self {
            Entry::Item {
                account_id,
                changed_collections,
            } => {
                if !changed_collections.is_empty() {
                    Some((*account_id, changed_collections.clear()))
                } else {
                    None
                }
            }
            Entry::Snapshot { changed_accounts } => loop {
                let (collections, account_ids) = changed_accounts.last_mut()?;
                if let Some(account_id) = account_ids.pop() {
                    return Some((account_id, collections.clone()));
                } else {
                    changed_accounts.pop();
                }
            },
        }
    }
}

impl StoreDeserialize for Entry {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        match *bytes.get(0)? {
            batch::Change::ENTRY => Entry::Item {
                account_id: AccountId::from_le_bytes(
                    bytes
                        .get(1..1 + std::mem::size_of::<AccountId>())?
                        .try_into()
                        .ok()?,
                ),
                changed_collections: u64::from_le_bytes(
                    bytes
                        .get(1 + std::mem::size_of::<AccountId>()..)?
                        .try_into()
                        .ok()?,
                )
                .into(),
            },
            batch::Change::SNAPSHOT => {
                let mut bytes_it = bytes.get(1..)?.iter();
                let total_collections = usize::from_leb128_it(&mut bytes_it)?;
                let mut changed_accounts = Vec::with_capacity(total_collections);

                for _ in 0..total_collections {
                    let collections = u64::from_leb128_it(&mut bytes_it)?.into();
                    let total_accounts = usize::from_leb128_it(&mut bytes_it)?;
                    let mut accounts = Vec::with_capacity(total_accounts);

                    for _ in 0..total_accounts {
                        accounts.push(AccountId::from_leb128_it(&mut bytes_it)?);
                    }

                    changed_accounts.push((collections, accounts));
                }

                Entry::Snapshot { changed_accounts }
            }
            _ => {
                return None;
            }
        }
        .into()
    }
}

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn assign_raft_id(&self) -> RaftId {
        RaftId {
            term: self.raft_term.load(Ordering::Relaxed),
            index: self
                .raft_index
                .fetch_add(1, Ordering::Relaxed)
                .wrapping_add(1),
        }
    }

    pub fn get_prev_raft_id(&self, key: RaftId) -> crate::Result<Option<RaftId>> {
        let key = LogKey::serialize_raft(&key);

        if let Some((key, _)) = self
            .db
            .iterator(ColumnFamily::Logs, &key, Direction::Backward)?
            .next()
        {
            if key.starts_with(&[LogKey::RAFT_KEY_PREFIX]) {
                return Ok(Some(LogKey::deserialize_raft(&key).ok_or_else(|| {
                    StoreError::InternalError(format!("Corrupted raft key for [{:?}]", key))
                })?));
            }
        }
        Ok(None)
    }

    pub fn get_next_raft_id(&self, key: RaftId) -> crate::Result<Option<RaftId>> {
        let key = LogKey::serialize_raft(&key);

        if let Some((key, _)) = self
            .db
            .iterator(ColumnFamily::Logs, &key, Direction::Forward)?
            .next()
        {
            if key.starts_with(&[LogKey::RAFT_KEY_PREFIX]) {
                return Ok(Some(LogKey::deserialize_raft(&key).ok_or_else(|| {
                    StoreError::InternalError(format!("Corrupted raft key for [{:?}]", key))
                })?));
            }
        }
        Ok(None)
    }

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
        let mut changed_accounts = HashMap::new();

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
                            .or_insert_with(Collections::default)
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
                                    .or_insert_with(Collections::default)
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
        let mut changed_collections = HashMap::new();
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
        changed_collections.len().to_leb128_bytes(&mut bytes);
        for (collections, account_ids) in changed_collections {
            collections.to_leb128_bytes(&mut bytes);
            account_ids.len().to_leb128_bytes(&mut bytes);
            for account_id in account_ids {
                account_id.to_leb128_bytes(&mut bytes);
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
