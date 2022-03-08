use std::collections::HashMap;
use std::sync::atomic::Ordering;

use crate::batch::LogAction;
use crate::leb128::Leb128;
use crate::serialize::{
    serialize_changelog_key, serialize_raftlog_key, DeserializeBigEndian, COLLECTION_PREFIX_LEN,
    INTERNAL_KEY_PREFIX,
};
use crate::{AccountId, CollectionId, ColumnFamily, Direction, JMAPStore, Store, StoreError};

pub type ChangeLogId = u64;

#[derive(Default)]
pub struct RaftId {
    pub term: ChangeLogId,
    pub index: ChangeLogId,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ChangeLogEntry {
    Insert(ChangeLogId),
    Update(ChangeLogId),
    Delete(ChangeLogId),
}

pub struct ChangeLog {
    pub changes: Vec<ChangeLogEntry>,
    pub from_change_id: ChangeLogId,
    pub to_change_id: ChangeLogId,
}

impl Default for ChangeLog {
    fn default() -> Self {
        Self {
            changes: Vec::with_capacity(10),
            from_change_id: 0,
            to_change_id: 0,
        }
    }
}

#[derive(Debug)]
pub enum ChangeLogQuery {
    All,
    Since(ChangeLogId),
    SinceInclusive(ChangeLogId),
    RangeInclusive(ChangeLogId, ChangeLogId),
}

impl ChangeLog {
    pub fn deserialize(&mut self, bytes: &[u8]) -> Option<()> {
        let mut bytes_it = bytes.iter();
        let total_inserts = usize::from_leb128_it(&mut bytes_it)?;
        let total_updates = usize::from_leb128_it(&mut bytes_it)?;
        let total_deletes = usize::from_leb128_it(&mut bytes_it)?;

        if total_inserts > 0 {
            for _ in 0..total_inserts {
                let id = ChangeLogId::from_leb128_it(&mut bytes_it)?;
                self.changes.push(ChangeLogEntry::Insert(id));
            }
        }

        if total_updates > 0 {
            'update_outer: for _ in 0..total_updates {
                let id = ChangeLogId::from_leb128_it(&mut bytes_it)?;

                if !self.changes.is_empty() {
                    let mut update_idx = None;
                    for (idx, change) in self.changes.iter().enumerate() {
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
                        self.changes.remove(idx);
                    }
                }

                self.changes.push(ChangeLogEntry::Update(id));
            }
        }

        if total_deletes > 0 {
            'delete_outer: for _ in 0..total_deletes {
                let id = ChangeLogId::from_leb128_it(&mut bytes_it)?;

                if !self.changes.is_empty() {
                    let mut update_idx = None;
                    for (idx, change) in self.changes.iter().enumerate() {
                        match change {
                            ChangeLogEntry::Insert(insert_id) => {
                                if *insert_id == id {
                                    self.changes.remove(idx);
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
                        self.changes.remove(idx);
                    }
                }

                self.changes.push(ChangeLogEntry::Delete(id));
            }
        }

        Some(())
    }
}

#[derive(Default)]
pub struct LogEntry {
    pub inserts: Vec<ChangeLogId>,
    pub updates: Vec<ChangeLogId>,
    pub deletes: Vec<ChangeLogId>,
}

impl From<LogEntry> for Vec<u8> {
    fn from(writer: LogEntry) -> Self {
        writer.serialize()
    }
}

//TODO delete old changelog entries
impl LogEntry {
    pub fn new() -> Self {
        LogEntry::default()
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

pub struct LogWriter {
    pub account_id: AccountId,
    pub raft_id: RaftId,
    pub changes: HashMap<(CollectionId, ChangeLogId), LogEntry>,
}

impl LogWriter {
    pub fn new(account_id: AccountId, raft_id: RaftId) -> Self {
        LogWriter {
            account_id,
            raft_id,
            changes: HashMap::new(),
        }
    }

    pub fn add_change(
        &mut self,
        collection_id: CollectionId,
        change_id: ChangeLogId,
        action: LogAction,
    ) {
        let log_entry = self
            .changes
            .entry((collection_id, change_id))
            .or_insert_with(LogEntry::new);

        match action {
            LogAction::Insert(id) => {
                log_entry.inserts.push(id);
            }
            LogAction::Update(id) => {
                log_entry.updates.push(id);
            }
            LogAction::Delete(id) => {
                log_entry.deletes.push(id);
            }
            LogAction::Move(old_id, id) => {
                log_entry.inserts.push(id);
                log_entry.deletes.push(old_id);
            }
        }
    }

    pub fn serialize(self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut entries = Vec::with_capacity(self.changes.len() + 1);
        let mut bytes = Vec::with_capacity(self.changes.len() * 256);

        self.changes.len().to_leb128_bytes(&mut bytes);

        for ((collection_id, change_id), log_entry) in self.changes {
            let entry_bytes = log_entry.serialize();
            self.account_id.to_leb128_bytes(&mut bytes);
            collection_id.to_leb128_bytes(&mut bytes);
            change_id.to_leb128_bytes(&mut bytes);
            bytes.extend_from_slice(&entry_bytes);
            entries.push((
                serialize_changelog_key(self.account_id, collection_id, change_id),
                entry_bytes,
            ));
        }
        entries.push((
            serialize_raftlog_key(self.raft_id.term, self.raft_id.index),
            bytes,
        ));

        entries
    }
}

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn next_raft_id(&self) -> RaftId {
        RaftId {
            term: self.raft_log_term.load(Ordering::Relaxed),
            index: self.raft_log_index.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub async fn get_last_change_id(
        &self,
        account: AccountId,
        collection: CollectionId,
    ) -> crate::Result<Option<ChangeLogId>> {
        let db = self.db.clone();
        self.spawn_worker(move || {
            let key = serialize_changelog_key(account, collection, ChangeLogId::MAX);
            let key_len = key.len();

            if let Some((key, _)) = db
                .iterator(ColumnFamily::Logs, key, Direction::Backward)?
                .into_iter()
                .next()
            {
                if key.starts_with(&key[0..COLLECTION_PREFIX_LEN]) && key.len() == key_len {
                    return Ok(Some(
                        key.as_ref()
                            .deserialize_be_u64(COLLECTION_PREFIX_LEN)
                            .ok_or_else(|| {
                                StoreError::InternalError(format!(
                                    "Corrupted changelog key for [{}/{}]: [{:?}]",
                                    account, collection, key
                                ))
                            })?,
                    ));
                }
            }
            Ok(None)
        })
        .await
    }

    pub async fn get_changes(
        &self,
        account: AccountId,
        collection: CollectionId,
        query: ChangeLogQuery,
    ) -> crate::Result<Option<ChangeLog>> {
        let db = self.db.clone();
        self.spawn_worker(move || {
            let mut changelog = ChangeLog::default();
            /*let (is_inclusive, mut match_from_change_id, from_change_id, to_change_id) = match query {
                ChangeLogQuery::All => (true, false, 0, 0),
                ChangeLogQuery::Since(change_id) => (false, true, change_id, 0),
                ChangeLogQuery::SinceInclusive(change_id) => (true, true, change_id, 0),
                ChangeLogQuery::RangeInclusive(from_change_id, to_change_id) => {
                    (true, true, from_change_id, to_change_id)
                }
            };*/
            let (is_inclusive, from_change_id, to_change_id) = match query {
                ChangeLogQuery::All => (true, 0, 0),
                ChangeLogQuery::Since(change_id) => (false, change_id, 0),
                ChangeLogQuery::SinceInclusive(change_id) => (true, change_id, 0),
                ChangeLogQuery::RangeInclusive(from_change_id, to_change_id) => {
                    (true, from_change_id, to_change_id)
                }
            };
            let key = serialize_changelog_key(account, collection, from_change_id);
            let key_len = key.len();
            let prefix = key[0..COLLECTION_PREFIX_LEN].to_vec();
            let mut is_first = true;

            for (key, value) in db.iterator(ColumnFamily::Logs, key, Direction::Forward)? {
                if !key.starts_with(&prefix) {
                    break;
                } else if key.len() != key_len {
                    //TODO avoid collisions with Raft keys
                    continue;
                }
                let change_id = key
                    .as_ref()
                    .deserialize_be_u64(COLLECTION_PREFIX_LEN)
                    .ok_or_else(|| {
                        StoreError::InternalError(format!(
                            "Failed to deserialize changelog key for [{}/{}]: [{:?}]",
                            account, collection, key
                        ))
                    })?;

                /*if match_from_change_id {
                    if change_id != from_change_id {
                        return Ok(None);
                    } else {
                        match_from_change_id = false;
                    }
                }*/

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
                            "Failed to deserialize changelog for [{}/{}]: [{:?}]",
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
        })
        .await
    }
}

pub fn get_last_raft_id<'y, T>(db: &'y T) -> crate::Result<Option<RaftId>>
where
    T: for<'x> Store<'x>,
{
    let key = serialize_raftlog_key(ChangeLogId::MAX, ChangeLogId::MAX);
    let key_len = key.len();

    if let Some((key, _)) = db
        .iterator(ColumnFamily::Logs, key, Direction::Backward)?
        .next()
    {
        if key.len() == key_len && key[0] == INTERNAL_KEY_PREFIX {
            let term = key.as_ref().deserialize_be_u64(1).ok_or_else(|| {
                StoreError::InternalError(format!("Corrupted raft key for [{:?}]", key))
            })?;
            let index = key
                .as_ref()
                .deserialize_be_u64(1 + std::mem::size_of::<ChangeLogId>())
                .ok_or_else(|| {
                    StoreError::InternalError(format!("Corrupted raft key for [{:?}]", key))
                })?;

            return Ok(Some(RaftId { term, index }));
        }
    }
    Ok(None)
}
