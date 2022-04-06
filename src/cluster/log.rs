use std::collections::HashSet;

use jmap_mail::mailbox::Mailbox;
use store::leb128::Leb128;
use store::serialize::{DeserializeBigEndian, StoreDeserialize};
use store::WriteOperation;
use store::{
    batch,
    leb128::skip_leb128_it,
    log::ChangeId,
    log::{Entry, LogIndex, RaftId, TermId},
    roaring::{RoaringBitmap, RoaringTreemap},
    serialize::{LogKey, LEADER_COMMIT_INDEX_KEY},
    tracing::debug,
    AccountId, Collection, Collections, ColumnFamily, Direction, DocumentId, JMAPId, JMAPIdPrefix,
    JMAPStore, Store, StoreError, Tag,
};
use tokio::sync::oneshot;

use super::rpc;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Update {
    Document {
        account_id: AccountId,
        document_id: DocumentId,
        update: DocumentUpdate,
    },
    Change {
        account_id: AccountId,
        collection: Collection,
        change: Vec<u8>,
    },
    Log {
        raft_id: RaftId,
        log: Vec<u8>,
    },
    Eof,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum DocumentUpdate {
    InsertMail {
        thread_id: DocumentId,
        keywords: HashSet<Tag>,
        mailboxes: HashSet<Tag>,
        received_at: i64,
        body: Vec<u8>,
    },
    UpdateMail {
        thread_id: DocumentId,
        keywords: HashSet<Tag>,
        mailboxes: HashSet<Tag>,
    },
    UpdateMailbox {
        mailbox: Mailbox,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum AppendEntriesRequest {
    Match {
        last_log: RaftId,
    },
    Synchronize {
        match_terms: Vec<RaftId>,
    },
    Merge {
        matched_log: RaftId,
    },
    Update {
        commit_index: LogIndex,
        updates: Vec<Update>,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum AppendEntriesResponse {
    Match {
        match_log: RaftId,
    },
    Synchronize {
        match_indexes: Vec<u8>,
    },
    Update {
        account_id: AccountId,
        collection: Collection,
        changes: Vec<u8>,
    },
    Continue,
    Commit {
        commit_index: LogIndex,
    },
}

pub struct Event {
    pub response_tx: oneshot::Sender<rpc::Response>,
    pub request: AppendEntriesRequest,
}

#[derive(Debug)]
pub struct MergedChanges {
    pub inserts: RoaringBitmap,
    pub updates: RoaringBitmap,
    pub deletes: RoaringBitmap,
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

        insert_size.to_leb128_bytes(&mut bytes);
        update_size.to_leb128_bytes(&mut bytes);
        delete_size.to_leb128_bytes(&mut bytes);

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
        let (insert_size, mut read_bytes) = usize::from_leb128_bytes(bytes)?;
        let (update_size, read_bytes_) = usize::from_leb128_bytes(bytes.get(read_bytes..)?)?;
        read_bytes += read_bytes_;
        let (delete_size, read_bytes_) = usize::from_leb128_bytes(bytes.get(read_bytes..)?)?;
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

pub trait RaftStore {
    fn get_raft_match_terms(&self) -> store::Result<Vec<RaftId>>;
    fn get_raft_match_indexes(
        &self,
        start_log_index: LogIndex,
    ) -> store::Result<(TermId, RoaringTreemap)>;
    fn merge_changes(
        &self,
        account: AccountId,
        collection: Collection,
        from_id: ChangeId,
        to_id: ChangeId,
    ) -> store::Result<MergedChanges>;
    fn prepare_rollback_changes(&self, after_index: LogIndex) -> store::Result<()>;
    fn next_rollback_change(&self)
        -> store::Result<Option<(AccountId, Collection, MergedChanges)>>;
    fn remove_rollback_change(
        &self,
        account_id: AccountId,
        collection: Collection,
    ) -> store::Result<()>;

    #[allow(clippy::type_complexity)]
    fn get_log_entries(
        &self,
        last_index: LogIndex,
        to_index: LogIndex,
        pending_changes: Vec<(Collections, Vec<AccountId>)>,
        batch_size: usize,
    ) -> store::Result<(Vec<Update>, Vec<(Collections, Vec<AccountId>)>, LogIndex)>;

    fn get_log_changes(
        &self,
        entries: &mut Vec<Update>,
        account_id: AccountId,
        changed_collections: Collections,
        change_id: ChangeId,
    ) -> store::Result<usize>;

    fn has_pending_rollback(&self) -> store::Result<bool>;
}

impl<T> RaftStore for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get_raft_match_terms(&self) -> store::Result<Vec<RaftId>> {
        let mut list = Vec::new();
        let prefix = &[LogKey::RAFT_KEY_PREFIX];
        let mut last_term_id = TermId::MAX;

        for (key, _) in self
            .db
            .iterator(ColumnFamily::Logs, prefix, Direction::Forward)?
        {
            if key.starts_with(prefix) {
                let raft_id = LogKey::deserialize_raft(&key).ok_or_else(|| {
                    StoreError::InternalError(format!("Corrupted raft entry for [{:?}]", key))
                })?;
                if raft_id.term != last_term_id {
                    last_term_id = raft_id.term;
                    list.push(raft_id);
                }
            } else {
                break;
            }
        }
        Ok(list)
    }

    fn get_raft_match_indexes(
        &self,
        start_log_index: LogIndex,
    ) -> store::Result<(TermId, RoaringTreemap)> {
        let mut list = RoaringTreemap::new();
        let from_key = LogKey::serialize_raft(&RaftId::new(0, start_log_index));
        let prefix = &from_key[..LogKey::RAFT_TERM_POS];
        let mut term_id = TermId::MAX;

        for (key, _) in self
            .db
            .iterator(ColumnFamily::Logs, prefix, Direction::Forward)?
        {
            if key.starts_with(prefix) {
                let raft_id = LogKey::deserialize_raft(&key).ok_or_else(|| {
                    StoreError::InternalError(format!("Corrupted raft entry for [{:?}]", key))
                })?;
                if term_id == TermId::MAX {
                    term_id = raft_id.term;
                } else if term_id != raft_id.term {
                    break;
                }
                list.insert(raft_id.index);
            } else {
                break;
            }
        }
        Ok((term_id, list))
    }

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

    fn prepare_rollback_changes(&self, after_index: LogIndex) -> store::Result<()> {
        let mut current_account_id = AccountId::MAX;
        let mut current_collection = Collection::None;
        let mut changes = MergedChanges::new();
        let mut write_batch = Vec::new();

        for (key, value) in self.db.iterator(
            ColumnFamily::Logs,
            &[LogKey::CHANGE_KEY_PREFIX],
            Direction::Forward,
        )? {
            if !key.starts_with(&[LogKey::CHANGE_KEY_PREFIX]) {
                break;
            }

            if after_index != LogIndex::MAX {
                let change_id = LogKey::deserialize_change_id(&key).ok_or_else(|| {
                    StoreError::InternalError(format!(
                        "Failed to deserialize changelog key : [{:?}]",
                        key
                    ))
                })?;

                if change_id <= after_index {
                    continue;
                }
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
                    write_batch.push(WriteOperation::set(
                        ColumnFamily::Logs,
                        LogKey::serialize_rollback(current_account_id, current_collection),
                        changes.serialize().ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Failed to serialized merged changes for [{}/{:?}]",
                                account_id, collection
                            ))
                        })?,
                    ));
                    self.db.write(write_batch)?;
                    write_batch = Vec::new();
                }
                changes = MergedChanges::new();
                current_account_id = account_id;
                current_collection = collection;
            }

            changes.deserialize_changes(&value).ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to deserialize raft changes for [{}/{:?}]",
                    account_id, collection
                ))
            })?;

            write_batch.push(WriteOperation::delete(ColumnFamily::Logs, key.to_vec()));
        }

        if !write_batch.is_empty() {
            write_batch.push(WriteOperation::set(
                ColumnFamily::Logs,
                LogKey::serialize_rollback(current_account_id, current_collection),
                changes.serialize().ok_or_else(|| {
                    StoreError::InternalError(format!(
                        "Failed to serialized merged changes for [{}/{:?}]",
                        current_account_id, current_collection
                    ))
                })?,
            ));
            self.db.write(write_batch)?;
            write_batch = Vec::new();
        }

        for (key, _) in self.db.iterator(
            ColumnFamily::Logs,
            &[LogKey::RAFT_KEY_PREFIX],
            Direction::Forward,
        )? {
            if key.starts_with(&[LogKey::RAFT_KEY_PREFIX]) {
                if after_index == LogIndex::MAX
                    || LogKey::deserialize_raft(&key)
                        .ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Corrupted raft entry for [{:?}]",
                                key
                            ))
                        })?
                        .index
                        > after_index
                {
                    write_batch.push(WriteOperation::delete(ColumnFamily::Logs, key.to_vec()));
                }
            } else {
                break;
            }
        }

        if !write_batch.is_empty() {
            self.db.write(write_batch)?;
        }

        Ok(())
    }

    fn next_rollback_change(
        &self,
    ) -> store::Result<Option<(AccountId, Collection, MergedChanges)>> {
        Ok(
            if let Some((key, value)) = self
                .db
                .iterator(
                    ColumnFamily::Logs,
                    &[LogKey::ROLLBACK_KEY_PREFIX],
                    Direction::Forward,
                )?
                .next()
            {
                if key.starts_with(&[LogKey::ROLLBACK_KEY_PREFIX]) {
                    Some((
                        (&key[..])
                            .deserialize_be_u32(LogKey::ACCOUNT_POS)
                            .ok_or_else(|| {
                                StoreError::InternalError(format!(
                                    "Failed to deserialize account id from changelog key: [{:?}]",
                                    key
                                ))
                            })?,
                        (*key.get(LogKey::COLLECTION_POS).ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Failed to deserialize collection from changelog key: [{:?}]",
                                key
                            ))
                        })?)
                        .into(),
                        MergedChanges::from_bytes(&value).ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Failed to deserialize rollback change: [{:?}]",
                                key
                            ))
                        })?,
                    ))
                } else {
                    None
                }
            } else {
                None
            },
        )
    }

    fn remove_rollback_change(
        &self,
        account_id: AccountId,
        collection: Collection,
    ) -> store::Result<()> {
        self.db.delete(
            ColumnFamily::Logs,
            &LogKey::serialize_rollback(account_id, collection),
        )
    }

    fn get_log_entries(
        &self,
        mut last_index: LogIndex,
        to_index: LogIndex,
        mut pending_changes: Vec<(Collections, Vec<AccountId>)>,
        batch_size: usize,
    ) -> store::Result<(Vec<Update>, Vec<(Collections, Vec<AccountId>)>, LogIndex)> {
        let mut entries = Vec::new();
        let start_index = last_index;
        let key = if start_index != LogIndex::MAX {
            LogKey::serialize_raft(&RaftId::new(0, start_index))
        } else {
            LogKey::serialize_raft(&RaftId::new(0, 0))
        };
        let prefix = &[LogKey::RAFT_KEY_PREFIX];
        let mut entries_size = 0;

        if pending_changes.is_empty() && start_index != to_index {
            for (key, value) in self
                .db
                .iterator(ColumnFamily::Logs, &key, Direction::Forward)?
            {
                if !key.starts_with(prefix) {
                    break;
                }

                let raft_id = LogKey::deserialize_raft(&key).ok_or_else(|| {
                    StoreError::InternalError(format!("Corrupted raft entry for [{:?}]", key))
                })?;

                if raft_id.index > start_index || start_index == LogIndex::MAX {
                    last_index = raft_id.index;
                    entries_size += value.len() + std::mem::size_of::<RaftId>();
                    entries.push(Update::Log {
                        raft_id,
                        log: value.to_vec(),
                    });

                    match Entry::deserialize(&value).ok_or_else(|| {
                        StoreError::InternalError(format!("Corrupted raft entry for [{:?}]", key))
                    })? {
                        Entry::Item {
                            account_id,
                            changed_collections,
                        } => {
                            entries_size += self.get_log_changes(
                                &mut entries,
                                account_id,
                                changed_collections,
                                raft_id.index,
                            )?;
                        }
                        Entry::Snapshot { changed_accounts } => {
                            debug_assert!(pending_changes.is_empty());
                            pending_changes = changed_accounts;
                            break;
                        }
                    };

                    if raft_id.index == to_index || entries_size >= batch_size {
                        break;
                    }
                }
            }
        }

        if !pending_changes.is_empty() {
            while let Some((collections, account_ids)) = pending_changes.last_mut() {
                if let Some(account_id) = account_ids.pop() {
                    entries_size += self.get_log_changes(
                        &mut entries,
                        account_id,
                        collections.clone(),
                        last_index,
                    )?;
                    if entries_size >= batch_size {
                        break;
                    }
                } else {
                    pending_changes.pop();
                }
            }
        }

        if last_index == to_index && pending_changes.is_empty() {
            entries.push(Update::Eof);
        }

        Ok((entries, pending_changes, last_index))
    }

    fn get_log_changes(
        &self,
        entries: &mut Vec<Update>,
        account_id: AccountId,
        changed_collections: Collections,
        change_id: ChangeId,
    ) -> store::Result<usize> {
        let mut entries_size = 0;
        for changed_collection in changed_collections {
            let change = self
                .db
                .get::<Vec<u8>>(
                    ColumnFamily::Logs,
                    &LogKey::serialize_change(account_id, changed_collection, change_id),
                )?
                .ok_or_else(|| {
                    StoreError::InternalError(format!(
                        "Missing change for change {}/{:?}/{}",
                        account_id, changed_collection, change_id
                    ))
                })?;
            entries_size += change.len() + std::mem::size_of::<AccountId>() + 1;
            entries.push(Update::Change {
                account_id,
                collection: changed_collection,
                change,
            });
        }
        Ok(entries_size)
    }

    fn has_pending_rollback(&self) -> store::Result<bool> {
        if self
            .db
            .iterator(
                ColumnFamily::Logs,
                &[LogKey::ROLLBACK_KEY_PREFIX],
                Direction::Forward,
            )?
            .next()
            .is_some()
        {
            debug!("This node has pending a rollback and won't start a new election.");
            return Ok(true);
        } else if let Some(commit_index) = self
            .db
            .get::<LogIndex>(ColumnFamily::Values, LEADER_COMMIT_INDEX_KEY)?
        {
            let last_log = self
                .get_prev_raft_id(RaftId::new(TermId::MAX, LogIndex::MAX))?
                .unwrap_or_else(RaftId::none);
            if last_log.index != commit_index {
                debug!(
                    concat!(
                        "This node has uncommitted changes ({} != {}) ",
                        "requiring rollback and won't start a new election."
                    ),
                    last_log.index, commit_index
                );
                return Ok(true);
            }
        }

        Ok(false)
    }
}
