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

use store::core::error::StoreError;

use store::log::raft::{LogIndex, RaftId, TermId};
use store::roaring::RoaringTreemap;
use store::serialize::key::LogKey;

use store::{ColumnFamily, Direction, JMAPStore, Store};

use crate::JMAPServer;

pub trait RaftStoreMatch {
    fn get_raft_match_terms(&self) -> store::Result<Vec<RaftId>>;
    fn get_raft_match_indexes(
        &self,
        start_log_index: LogIndex,
    ) -> store::Result<(TermId, RoaringTreemap)>;
}

impl<T> RaftStoreMatch for JMAPStore<T>
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
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn get_raft_match_terms(&self) -> store::Result<Vec<RaftId>> {
        let store = self.store.clone();
        self.spawn_worker(move || store.get_raft_match_terms())
            .await
    }

    pub async fn get_raft_match_indexes(
        &self,
        start_log_index: LogIndex,
    ) -> store::Result<(TermId, RoaringTreemap)> {
        let store = self.store.clone();
        self.spawn_worker(move || store.get_raft_match_indexes(start_log_index))
            .await
    }
}
