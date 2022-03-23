use jmap::changes::{JMAPChanges, JMAPState};
use store::{
    batch::WriteBatch,
    raft::{Entry, LogIndex, RaftId, TermId},
    roaring::RoaringBitmap,
    serialize::LogKey,
    AccountId, Collection, ColumnFamily, Direction, JMAPStore, Store,
};

pub fn compact_log<T>(mail_store: JMAPStore<T>)
where
    T: for<'x> Store<'x> + 'static,
{
    const NUM_ACCOUNTS: usize = 100;

    let mut expected_changed_accounts = RoaringBitmap::new();
    let mut expected_inserted_ids = vec![Vec::new(); NUM_ACCOUNTS];

    for run in 0u64..10u64 {
        println!("Run {}", run);
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
            expected_inserted_id.push(run + 1);
        }
        assert_compaction(&mail_store, NUM_ACCOUNTS);
    }

    match Entry::deserialize(
        &mail_store
            .get_raft_entries(RaftId::none(), 1)
            .unwrap()
            .pop()
            .unwrap()
            .data,
    )
    .unwrap()
    {
        Entry::Item { .. } => panic!("Expected log entry to be a snapshot."),
        Entry::Snapshot { changed_accounts } => {
            assert_eq!(changed_accounts, expected_changed_accounts)
        }
    }

    for (num, expected_inserted_id) in expected_inserted_ids.into_iter().enumerate() {
        let account_id = (num * 3) as AccountId;

        let changes = mail_store
            .get_jmap_changes(account_id, Collection::Mail, JMAPState::Initial, 0)
            .unwrap();

        assert_eq!(changes.created, expected_inserted_id);
        assert_eq!(changes.updated, Vec::<u64>::new());
        assert_eq!(changes.destroyed, Vec::<u64>::new());
    }
}

pub fn assert_compaction<T>(mail_store: &JMAPStore<T>, num_accounts: usize)
where
    T: for<'x> Store<'x> + 'static,
{
    mail_store
        .compact_log(
            mail_store
                .get_prev_raft_id(RaftId::new(TermId::MAX, LogIndex::MAX))
                .unwrap()
                .unwrap()
                .index,
        )
        .unwrap();

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
