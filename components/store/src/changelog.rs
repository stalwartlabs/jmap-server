use std::collections::HashMap;

use crate::batch::LogAction;
use crate::leb128::Leb128;
use crate::serialize::{serialize_changelog_key, serialize_raftlog_key};
use crate::{AccountId, CollectionId};

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