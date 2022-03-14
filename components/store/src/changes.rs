use std::collections::HashMap;

use crate::batch::LogAction;
use crate::leb128::Leb128;
use crate::raft::RaftId;
use crate::serialize::{DeserializeBigEndian, COLLECTION_PREFIX_LEN, FIELD_PREFIX_LEN};
use crate::{
    AccountId, ColumnFamily, Direction, Collection, JMAPId, JMAPStore, Store, StoreError,
    WriteOperation,
};

pub type ChangeId = u64;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Change {
    Insert(JMAPId),
    Update(JMAPId),
    Delete(JMAPId),
}

pub struct Changes {
    pub changes: Vec<Change>,
    pub from_change_id: ChangeId,
    pub to_change_id: ChangeId,
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

#[derive(Debug)]
pub enum Query {
    All,
    Since(ChangeId),
    SinceInclusive(ChangeId),
    RangeInclusive(ChangeId, ChangeId),
}

impl Changes {
    pub fn deserialize(&mut self, bytes: &[u8]) -> Option<()> {
        let mut bytes_it = bytes.iter();
        let total_inserts = usize::from_leb128_it(&mut bytes_it)?;
        let total_updates = usize::from_leb128_it(&mut bytes_it)?;
        let total_deletes = usize::from_leb128_it(&mut bytes_it)?;

        if total_inserts > 0 {
            for _ in 0..total_inserts {
                self.changes
                    .push(Change::Insert(ChangeId::from_leb128_it(&mut bytes_it)?));
            }
        }

        if total_updates > 0 {
            'update_outer: for _ in 0..total_updates {
                let id = ChangeId::from_leb128_it(&mut bytes_it)?;

                if !self.changes.is_empty() {
                    let mut update_idx = None;
                    for (idx, change) in self.changes.iter().enumerate() {
                        match change {
                            Change::Insert(insert_id) => {
                                if *insert_id == id {
                                    // Item updated after inserted, no need to count this change.
                                    continue 'update_outer;
                                }
                            }
                            Change::Update(update_id) => {
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

                self.changes.push(Change::Update(id));
            }
        }

        if total_deletes > 0 {
            'delete_outer: for _ in 0..total_deletes {
                let id = ChangeId::from_leb128_it(&mut bytes_it)?;

                if !self.changes.is_empty() {
                    //let mut update_idx = None;
                    'delete_inner: for (idx, change) in self.changes.iter().enumerate() {
                        match change {
                            Change::Insert(insert_id) => {
                                if *insert_id == id {
                                    self.changes.remove(idx);
                                    continue 'delete_outer;
                                }
                            }
                            Change::Update(update_id) => {
                                if *update_id == id {
                                    self.changes.remove(idx);
                                    //update_idx = Some(idx);
                                    break 'delete_inner;
                                }
                            }
                            _ => (),
                        }
                    }
                    /*if let Some(idx) = update_idx {
                        self.changes.remove(idx);
                    }*/
                }

                self.changes.push(Change::Delete(id));
            }
        }

        Some(())
    }
}

#[derive(Default)]
pub struct Entry {
    pub inserts: Vec<JMAPId>,
    pub updates: Vec<JMAPId>,
    pub deletes: Vec<JMAPId>,
}

impl From<Entry> for Vec<u8> {
    fn from(writer: Entry) -> Self {
        writer.serialize()
    }
}

//TODO delete old changelog entries
impl Entry {
    pub fn new() -> Self {
        Entry::default()
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

    pub fn serialize_key(
        account: AccountId,
        collection: Collection,
        change_id: ChangeId,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(FIELD_PREFIX_LEN + std::mem::size_of::<ChangeId>());
        bytes.extend_from_slice(&account.to_be_bytes());
        bytes.push(collection.into());
        bytes.extend_from_slice(&change_id.to_be_bytes());
        bytes
    }

    pub fn deserialize_change_id(bytes: &[u8]) -> Option<ChangeId> {
        bytes.deserialize_be_u64(COLLECTION_PREFIX_LEN)
    }
}

pub struct LogWriter {
    pub account_id: AccountId,
    pub raft_id: RaftId,
    pub changes: HashMap<(Collection, ChangeId), Entry>,
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
        collection: Collection,
        change_id: ChangeId,
        action: LogAction,
    ) {
        let log_entry = self
            .changes
            .entry((collection, change_id))
            .or_insert_with(Entry::new);

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

    pub fn serialize(self, batch: &mut Vec<WriteOperation>) {
        let mut raft_bytes = Vec::with_capacity(
            std::mem::size_of::<AccountId>()
                + std::mem::size_of::<usize>()
                + (self.changes.len()
                    * (std::mem::size_of::<ChangeId>() + std::mem::size_of::<Collection>())),
        );

        self.account_id.to_leb128_bytes(&mut raft_bytes);
        self.changes.len().to_leb128_bytes(&mut raft_bytes);

        for ((collection, change_id), log_entry) in self.changes {
            raft_bytes.push(collection.into());
            change_id.to_leb128_bytes(&mut raft_bytes);

            batch.push(WriteOperation::set(
                ColumnFamily::Logs,
                Entry::serialize_key(self.account_id, collection, change_id),
                log_entry.serialize(),
            ));
        }

        batch.push(WriteOperation::set(
            ColumnFamily::Logs,
            self.raft_id.serialize_key(),
            raft_bytes,
        ));
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
        let key = Entry::serialize_key(account, collection, ChangeId::MAX);
        let key_len = key.len();

        if let Some((key, _)) = self
            .db
            .iterator(ColumnFamily::Logs, &key, Direction::Backward)?
            .into_iter()
            .next()
        {
            if key.starts_with(&key[0..COLLECTION_PREFIX_LEN]) && key.len() == key_len {
                return Ok(Some(Entry::deserialize_change_id(&key).ok_or_else(
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
        /*let (is_inclusive, mut match_from_change_id, from_change_id, to_change_id) = match query {
            Query::All => (true, false, 0, 0),
            Query::Since(change_id) => (false, true, change_id, 0),
            Query::SinceInclusive(change_id) => (true, true, change_id, 0),
            Query::RangeInclusive(from_change_id, to_change_id) => {
                (true, true, from_change_id, to_change_id)
            }
        };*/
        let (is_inclusive, from_change_id, to_change_id) = match query {
            Query::All => (true, 0, 0),
            Query::Since(change_id) => (false, change_id, 0),
            Query::SinceInclusive(change_id) => (true, change_id, 0),
            Query::RangeInclusive(from_change_id, to_change_id) => {
                (true, from_change_id, to_change_id)
            }
        };
        let key = Entry::serialize_key(account, collection, from_change_id);
        let key_len = key.len();
        let prefix = &key[0..COLLECTION_PREFIX_LEN];
        let mut is_first = true;

        for (key, value) in self
            .db
            .iterator(ColumnFamily::Logs, &key, Direction::Forward)?
        {
            if !key.starts_with(prefix) {
                break;
            } else if key.len() != key_len {
                //TODO avoid collisions with Raft keys
                continue;
            }
            let change_id = Entry::deserialize_change_id(&key).ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to deserialize changelog key for [{}/{:?}]: [{:?}]",
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
}
