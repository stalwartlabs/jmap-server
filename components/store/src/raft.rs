use std::convert::TryInto;
use std::sync::atomic::Ordering;

use roaring::{RoaringBitmap, RoaringTreemap};

use crate::leb128::{skip_leb128_it, Leb128};
use crate::serialize::LogKey;
use crate::{batch, Collections, JMAPId, JMAPIdPrefix, WriteOperation};
use crate::{
    changes::ChangeId, AccountId, Collection, ColumnFamily, Direction, JMAPStore, Store, StoreError,
};
pub type TermId = u64;
pub type LogIndex = u64;

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

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct RawEntry {
    pub id: RaftId,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub enum Entry {
    Item {
        account_id: AccountId,
        changed_collections: Collections,
    },
    Snapshot {
        changed_accounts: RoaringBitmap,
    },
}

impl Entry {
    pub fn deserialize(value: &[u8]) -> Option<Self> {
        match *value.get(0)? {
            batch::Change::ENTRY => Entry::Item {
                account_id: AccountId::from_le_bytes(
                    value
                        .get(1..1 + std::mem::size_of::<AccountId>())?
                        .try_into()
                        .ok()?,
                ),
                changed_collections: u64::from_le_bytes(
                    value
                        .get(1 + std::mem::size_of::<AccountId>()..)?
                        .try_into()
                        .ok()?,
                )
                .into(),
            },
            batch::Change::SNAPSHOT => Entry::Snapshot {
                changed_accounts: RoaringBitmap::deserialize_from(value.get(1..)?).ok()?,
            },
            _ => {
                return None;
            }
        }
        .into()
    }

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
            Entry::Snapshot { changed_accounts } => {
                let account_id = changed_accounts.min()?;
                changed_accounts.remove(account_id);

                Some((account_id, Collections::all()))
            }
        }
    }
}

#[derive(Debug)]
pub struct PendingChanges {
    pub account_id: AccountId,
    pub collection: Collection,
    pub inserts: RoaringBitmap,
    pub updates: RoaringBitmap,
    pub deletes: RoaringBitmap,
    pub changes: RoaringTreemap,
}

impl PendingChanges {
    pub fn new(account_id: AccountId, collection: Collection) -> Self {
        Self {
            account_id,
            collection,
            inserts: RoaringBitmap::new(),
            updates: RoaringBitmap::new(),
            deletes: RoaringBitmap::new(),
            changes: RoaringTreemap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inserts.is_empty()
            && self.updates.is_empty()
            && self.deletes.is_empty()
            && self.changes.is_empty()
    }

    pub fn deserialize(&mut self, change_id: ChangeId, bytes: &[u8]) -> Option<()> {
        match *bytes.get(0)? {
            batch::Change::ENTRY => {
                let mut bytes_it = bytes.get(1..)?.iter();
                let total_inserts = usize::from_leb128_it(&mut bytes_it)?;
                let total_updates = usize::from_leb128_it(&mut bytes_it)?;
                let total_child_updates = usize::from_leb128_it(&mut bytes_it)?;
                let total_deletes = usize::from_leb128_it(&mut bytes_it)?;

                let mut inserted_ids = Vec::with_capacity(total_inserts);

                for _ in 0..total_inserts {
                    inserted_ids.push(JMAPId::from_leb128_it(&mut bytes_it)?);
                }

                for _ in 0..total_updates {
                    let document_id = JMAPId::from_leb128_it(&mut bytes_it)?.get_document_id();
                    if !self.inserts.contains(document_id) {
                        self.updates.insert(document_id);
                    }
                }

                // Skip child updates
                for _ in 0..total_child_updates {
                    skip_leb128_it(&mut bytes_it)?;
                }

                for _ in 0..total_deletes {
                    let deleted_id = JMAPId::from_leb128_it(&mut bytes_it)?;
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
                    let document_id = inserted_id.get_document_id();
                    self.inserts.insert(document_id);
                    // IDs can be reused
                    self.deletes.remove(document_id);
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

        self.changes.insert(change_id);

        Some(())
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

    pub fn get_raft_entries(
        &self,
        from_raft_id: RaftId,
        num_entries: usize,
    ) -> crate::Result<Vec<RawEntry>> {
        let mut entries = Vec::with_capacity(num_entries);
        let (is_inclusive, key) = if !from_raft_id.is_none() {
            (false, LogKey::serialize_raft(&from_raft_id))
        } else {
            (true, LogKey::serialize_raft(&RaftId::new(0, 0)))
        };
        let prefix = &[LogKey::RAFT_KEY_PREFIX];

        for (key, value) in self
            .db
            .iterator(ColumnFamily::Logs, &key, Direction::Forward)?
        {
            if key.starts_with(prefix) {
                let raft_id = LogKey::deserialize_raft(&key).ok_or_else(|| {
                    StoreError::InternalError(format!("Corrupted raft entry for [{:?}]", key))
                })?;
                if is_inclusive || raft_id != from_raft_id {
                    entries.push(RawEntry {
                        id: raft_id,
                        data: value.to_vec(),
                    });
                    if entries.len() == num_entries {
                        break;
                    }
                }
            } else {
                break;
            }
        }
        Ok(entries)
    }

    pub fn insert_raft_entries(&self, entries: Vec<RawEntry>) -> crate::Result<()> {
        self.db.write(
            entries
                .into_iter()
                .map(|entry| {
                    WriteOperation::set(
                        ColumnFamily::Logs,
                        LogKey::serialize_raft(&entry.id),
                        entry.data,
                    )
                })
                .collect(),
        )
    }

    pub fn get_pending_changes(
        &self,
        account: AccountId,
        collection: Collection,
        from_change_id: Option<ChangeId>,
        only_ids: bool,
    ) -> crate::Result<PendingChanges> {
        let mut changes = PendingChanges::new(account, collection);

        let (is_inclusive, from_change_id) = if let Some(from_change_id) = from_change_id {
            (false, from_change_id)
        } else {
            (true, 0)
        };

        let key = LogKey::serialize_change(account, collection, from_change_id);
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

            if change_id > from_change_id || (is_inclusive && change_id == from_change_id) {
                if !only_ids {
                    changes.deserialize(change_id, &value).ok_or_else(|| {
                        StoreError::InternalError(format!(
                            "Failed to deserialize raft changes for [{}/{:?}]",
                            account, collection
                        ))
                    })?;
                } else {
                    changes.changes.insert(change_id);
                }
            }
        }

        Ok(changes)
    }
}
