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

use std::sync::Arc;

use jmap::{
    request::changes::ChangesRequest,
    types::{jmap::JMAPId, state::JMAPState},
};
use jmap_mail::mail::changes::JMAPMailChanges;
use store::{
    ahash::AHashSet,
    core::{acl::ACLToken, collection::Collection, error::StoreError},
    log::{entry::Entry, raft::RaftId},
    serialize::{key::LogKey, StoreDeserialize},
    write::batch::WriteBatch,
    AccountId, ColumnFamily, Direction, JMAPStore, Store,
};

pub fn test<T>(mail_store: Arc<JMAPStore<T>>)
where
    T: for<'x> Store<'x> + 'static,
{
    const NUM_ACCOUNTS: usize = 100;

    let mut expected_changed_accounts = AHashSet::default();
    let mut expected_inserted_ids: Vec<Vec<JMAPId>> = vec![Vec::new(); NUM_ACCOUNTS];

    for run in 0u64..10u64 {
        for (num, expected_inserted_id) in expected_inserted_ids.iter_mut().enumerate() {
            let account_id = (num * 3) as AccountId;
            let mut batch = WriteBatch::new(account_id);
            batch.log_insert(Collection::Mail, (run + 1) * 10);
            mail_store.write(batch).unwrap();
            let mut batch = WriteBatch::new(account_id);
            batch.log_insert(Collection::Mail, run + 1);
            mail_store.write(batch).unwrap();
            let mut batch = WriteBatch::new(account_id);
            batch.log_delete(Collection::Mail, (run + 1) * 10);
            mail_store.write(batch).unwrap();

            expected_changed_accounts.insert(account_id);
            expected_inserted_id.push(JMAPId::new(run + 1));
        }
        assert_compaction(&mail_store, NUM_ACCOUNTS);
    }

    match Entry::deserialize(
        &mail_store
            .get_raft_raw_entries(RaftId::none(), 1)
            .unwrap()
            .pop()
            .unwrap()
            .1,
    )
    .unwrap()
    {
        Entry::Item { .. } => panic!("Expected log entry to be a snapshot."),
        Entry::Snapshot {
            mut changed_accounts,
        } => {
            assert_eq!(changed_accounts.len(), 1);
            assert_eq!(
                changed_accounts
                    .pop()
                    .unwrap()
                    .1
                    .into_iter()
                    .collect::<AHashSet<_>>(),
                expected_changed_accounts
            );
        }
    }

    for (num, expected_inserted_id) in expected_inserted_ids.into_iter().enumerate() {
        let changes = mail_store
            .mail_changes(ChangesRequest {
                acl: Some(Arc::new(ACLToken {
                    member_of: vec![(num * 3) as AccountId],
                    access_to: vec![],
                })),
                account_id: JMAPId::new((num * 3) as u64),
                since_state: JMAPState::Initial,
                max_changes: None,
            })
            .unwrap();

        assert_eq!(changes.created, expected_inserted_id);
        assert_eq!(changes.updated, vec![]);
        assert_eq!(changes.destroyed, vec![]);
    }
}

pub fn assert_compaction<T>(mail_store: &JMAPStore<T>, num_accounts: usize)
where
    T: for<'x> Store<'x> + 'static,
{
    mail_store.compact_log(1).unwrap();

    /*mail_store
    .compact_log_up_to(
        mail_store
            .get_prev_raft_id(RaftId::new(TermId::MAX, LogIndex::MAX))
            .unwrap()
            .unwrap()
            .index,
    )
    .unwrap();*/

    // Make sure compaction happened
    let mut total_change_entries = 0;
    let mut total_raft_entries = 0;

    for (key, _) in mail_store
        .db
        .iterator(ColumnFamily::Logs, &[0], Direction::Forward)
        .unwrap()
    {
        match key[0] {
            LogKey::CHANGE_KEY_PREFIX => {
                total_change_entries += 1;
            }
            LogKey::RAFT_KEY_PREFIX => {
                total_raft_entries += 1;
            }
            _ => {
                panic!("Unexpected key: {:?}", key);
            }
        }
    }

    assert_eq!(total_change_entries, num_accounts);
    assert_eq!(total_raft_entries, 1);
}

trait JMAPRaftRawEntries {
    fn get_raft_raw_entries(
        &self,
        from_raft_id: RaftId,
        num_entries: usize,
    ) -> store::Result<Vec<(RaftId, Vec<u8>)>>;
}

impl<T> JMAPRaftRawEntries for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get_raft_raw_entries(
        &self,
        from_raft_id: RaftId,
        num_entries: usize,
    ) -> store::Result<Vec<(RaftId, Vec<u8>)>> {
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
                    entries.push((raft_id, value.to_vec()));
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
}
