use std::sync::atomic::Ordering;

use serde::{Deserialize, Serialize};

use crate::leb128::Leb128;
use crate::{
    changelog::{ChangeLogId, LogEntry},
    serialize::{DeserializeBigEndian, INTERNAL_KEY_PREFIX},
    AccountId, CollectionId, ColumnFamily, Direction, JMAPStore, Store, StoreError,
};
pub type TermId = u64;
pub type LogIndex = u64;

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftId {
    pub term: TermId,
    pub index: LogIndex,
}

impl RaftId {
    pub fn new(term: TermId, index: LogIndex) -> Self {
        Self { term, index }
    }

    pub fn first() -> Self {
        Self { term: 0, index: 0 }
    }

    pub fn null() -> Self {
        Self {
            term: TermId::MAX,
            index: LogIndex::MAX,
        }
    }

    pub fn is_null(&self) -> bool {
        self.term == TermId::MAX && self.index == LogIndex::MAX
    }

    pub fn deserialize_key(bytes: &[u8]) -> Option<Self> {
        RaftId {
            term: bytes.deserialize_be_u64(1)?,
            index: bytes.deserialize_be_u64(1 + std::mem::size_of::<LogIndex>())?,
        }
        .into()
    }

    pub fn serialize_key(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity((std::mem::size_of::<LogIndex>() * 2) + 1);
        bytes.push(INTERNAL_KEY_PREFIX);
        bytes.extend_from_slice(&self.term.to_be_bytes());
        bytes.extend_from_slice(&self.index.to_be_bytes());
        bytes
    }
}

pub struct RaftEntry {
    pub raft_id: RaftId,
    pub account_id: AccountId,
    pub changes: Vec<RaftChange>,
}

pub struct RaftChange {
    pub change_id: ChangeLogId,
    pub collection_id: CollectionId,
}

impl RaftEntry {
    pub fn deserialize(key: &[u8], value: &[u8]) -> Option<Self> {
        let mut value_it = value.iter();

        let account_id = AccountId::from_leb128_it(&mut value_it)?;
        let mut total_changes = usize::from_leb128_it(&mut value_it)?;
        let mut changes = Vec::with_capacity(total_changes);

        while total_changes > 0 {
            changes.push(RaftChange {
                collection_id: CollectionId::from_leb128_it(&mut value_it)?,
                change_id: ChangeLogId::from_leb128_it(&mut value_it)?,
            });
            total_changes -= 1;
        }

        RaftEntry {
            account_id,
            raft_id: RaftId::deserialize_key(key)?,
            changes,
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
            term: self.raft_log_term.load(Ordering::Relaxed),
            index: self.raft_log_index.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub fn get_prev_raft_id(&self, key: RaftId) -> crate::Result<Option<RaftId>> {
        let key = key.serialize_key();
        let key_len = key.len();

        if let Some((key, _)) = self
            .db
            .iterator(ColumnFamily::Logs, &key, Direction::Backward)?
            .next()
        {
            if key.len() == key_len && key[0] == INTERNAL_KEY_PREFIX {
                return Ok(Some(RaftId::deserialize_key(&key).ok_or_else(|| {
                    StoreError::InternalError(format!("Corrupted raft key for [{:?}]", key))
                })?));
            }
        }
        Ok(None)
    }

    pub fn get_next_raft_id(&self, key: RaftId) -> crate::Result<Option<RaftId>> {
        let key = key.serialize_key();
        let key_len = key.len();

        if let Some((key, _)) = self
            .db
            .iterator(ColumnFamily::Logs, &key, Direction::Forward)?
            .next()
        {
            if key.len() == key_len && key[0] == INTERNAL_KEY_PREFIX {
                return Ok(Some(RaftId::deserialize_key(&key).ok_or_else(|| {
                    StoreError::InternalError(format!("Corrupted raft key for [{:?}]", key))
                })?));
            }
        }
        Ok(None)
    }

    pub fn get_raft_entry(&self, raft_id: RaftId) -> crate::Result<Option<RaftEntry>> {
        let key = raft_id.serialize_key();
        let key_len = key.len();

        if let Some((key, value)) = self
            .db
            .iterator(ColumnFamily::Logs, &key, Direction::Forward)?
            .next()
        {
            if key.len() == key_len && key[0] == INTERNAL_KEY_PREFIX {
                return Ok(Some(RaftEntry::deserialize(&key, &value).ok_or_else(
                    || StoreError::InternalError(format!("Corrupted raft entry for [{:?}]", key)),
                )?));
            }
        }
        Ok(None)
    }
}
