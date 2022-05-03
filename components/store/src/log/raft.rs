use crate::serialize::key::LogKey;
use crate::serialize::leb128::Leb128;
use crate::serialize::{StoreDeserialize, StoreSerialize};
use crate::{ColumnFamily, Direction, JMAPStore, Store, StoreError};
use std::sync::atomic::Ordering;
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
}
