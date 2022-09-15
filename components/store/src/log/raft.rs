/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use crate::serialize::key::LogKey;
use crate::serialize::leb128::{Leb128Iterator, Leb128Vec};
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
        bytes.push_leb128(self.term);
        bytes.push_leb128(self.index);
        bytes.into()
    }
}

impl StoreDeserialize for RaftId {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        let mut bytes = bytes.iter();
        Some(Self {
            term: bytes.next_leb128()?,
            index: bytes.next_leb128()?,
        })
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
