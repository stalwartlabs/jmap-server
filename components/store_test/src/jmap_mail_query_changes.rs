use std::collections::{HashMap, HashSet};

use jmap_mail::{
    import::JMAPMailImportItem,
    query::{JMAPMailComparator, JMAPMailFilterCondition},
    JMAPMailId, JMAPMailIdImpl, JMAPMailStoreChanges, JMAPMailStoreImport, MessageField,
};
use jmap_store::{
    changes::JMAPState, local_store::JMAPLocalStore, JMAPComparator, JMAPFilter, JMAPQueryChanges,
};
use store::{
    batch::{DocumentWriter, LogAction},
    Store, Tag,
};

pub fn test_jmap_mail_query_changes<T>(db: T)
where
    T: for<'x> Store<'x>,
{
    let mail_store = JMAPLocalStore::new(db);
    let mut states = vec![JMAPState::Initial];
    let mut id_map = HashMap::new();

    let mut updated_ids = HashSet::new();
    let mut removed_ids = HashSet::new();
    let mut type1_ids = HashSet::new();

    let mut thread_id = 100;

    for (change_num, change) in [
        LogAction::Insert(0),
        LogAction::Insert(1),
        LogAction::Insert(2),
        LogAction::Move(0, 3),
        LogAction::Insert(4),
        LogAction::Insert(5),
        LogAction::Update(1),
        LogAction::Update(2),
        LogAction::Delete(1),
        LogAction::Insert(6),
        LogAction::Insert(7),
        LogAction::Update(2),
        LogAction::Update(4),
        LogAction::Update(5),
        LogAction::Update(6),
        LogAction::Update(7),
        LogAction::Delete(4),
        LogAction::Delete(5),
        LogAction::Delete(6),
        LogAction::Insert(8),
        LogAction::Insert(9),
        LogAction::Insert(10),
        LogAction::Update(3),
        LogAction::Update(2),
        LogAction::Update(8),
        LogAction::Move(9, 11),
        LogAction::Move(10, 12),
        LogAction::Delete(8),
    ]
    .iter()
    .enumerate()
    {
        match &change {
            LogAction::Insert(id) => {
                let jmap_id = mail_store
                    .mail_import_single(
                        0,
                        JMAPMailImportItem {
                            blob: format!(
                                "From: test_{}\nSubject: test_{}\n\ntest",
                                if change_num % 2 == 0 { 1 } else { 2 },
                                *id
                            )
                            .as_bytes()
                            .into(),
                            mailbox_ids: vec![if change_num % 2 == 0 { 1 } else { 2 }],
                            keywords: vec![if change_num % 2 == 0 {
                                "1".into()
                            } else {
                                "2".into()
                            }],
                            received_at: Some(*id as i64),
                        },
                    )
                    .unwrap();

                id_map.insert(*id, jmap_id);
                if change_num % 2 == 0 {
                    type1_ids.insert(jmap_id);
                }
            }
            LogAction::Update(id) => {
                let id = *id_map.get(id).unwrap();

                mail_store
                    .get_store()
                    .update_document(
                        DocumentWriter::update(0, 0, id.get_document_id())
                            .log(LogAction::Update(id)),
                    )
                    .unwrap();
                updated_ids.insert(id);
            }
            LogAction::Delete(id) => {
                let id = *id_map.get(id).unwrap();
                mail_store
                    .get_store()
                    .update_document(
                        DocumentWriter::delete(0, 0, id.get_document_id())
                            .log(LogAction::Delete(id)),
                    )
                    .unwrap();
                removed_ids.insert(id);
            }
            LogAction::Move(from, to) => {
                let id = *id_map.get(from).unwrap();
                let new_id = JMAPMailId::new(thread_id, id.get_document_id());

                let mut batch = DocumentWriter::update(0, 0, id.get_document_id());
                batch.add_integer(MessageField::ThreadId.into(), 0, thread_id, true, false);
                batch.add_tag(MessageField::ThreadId.into(), Tag::Id(thread_id));
                batch.log_move(id, new_id);
                mail_store.get_store().update_document(batch).unwrap();

                id_map.insert(*to, new_id);
                if type1_ids.contains(&id) {
                    type1_ids.insert(new_id);
                }
                removed_ids.insert(id);
                thread_id += 1;
            }
            LogAction::None => (),
        }

        let mut new_state = JMAPState::Initial;
        for state in &states {
            for (test_num, query) in vec![
                JMAPQueryChanges {
                    account_id: 0,
                    filter: JMAPFilter::None,
                    sort: vec![JMAPComparator::ascending(JMAPMailComparator::ReceivedAt)],
                    since_query_state: state.clone(),
                    max_changes: 0,
                    up_to_id: None,
                    calculate_total: false,
                },
                JMAPQueryChanges {
                    account_id: 0,
                    filter: JMAPFilter::Condition(JMAPMailFilterCondition::From("test_1".into())),
                    sort: vec![JMAPComparator::ascending(JMAPMailComparator::ReceivedAt)],
                    since_query_state: state.clone(),
                    max_changes: 0,
                    up_to_id: None,
                    calculate_total: false,
                },
                JMAPQueryChanges {
                    account_id: 0,
                    filter: JMAPFilter::Condition(JMAPMailFilterCondition::InMailbox(1)),
                    sort: vec![JMAPComparator::ascending(JMAPMailComparator::ReceivedAt)],
                    since_query_state: state.clone(),
                    max_changes: 0,
                    up_to_id: None,
                    calculate_total: false,
                },
                JMAPQueryChanges {
                    account_id: 0,
                    filter: JMAPFilter::None,
                    sort: vec![JMAPComparator::ascending(JMAPMailComparator::ReceivedAt)],
                    since_query_state: state.clone(),
                    max_changes: 0,
                    up_to_id: id_map.get(&7).copied(),
                    calculate_total: false,
                },
            ]
            .into_iter()
            .enumerate()
            {
                if test_num == 3 && query.up_to_id.is_none() {
                    continue;
                }
                let changes = mail_store.mail_query_changes(query.clone(), false).unwrap();

                if test_num == 0 || test_num == 1 {
                    // Immutable filters should not return modified ids, only deletions.
                    for id in &changes.removed {
                        assert!(
                            removed_ids.contains(id),
                            "{:?} = {:?} (id: {})",
                            query,
                            changes,
                            id_map.iter().find(|(_, v)| **v == *id).unwrap().0
                        );
                    }
                }
                if test_num == 1 || test_num == 2 {
                    // Only type 1 results should be added to the list.
                    for item in &changes.added {
                        assert!(
                            type1_ids.contains(&item.id),
                            "{:?} = {:?} (id: {})",
                            query,
                            changes,
                            id_map.iter().find(|(_, v)| **v == item.id).unwrap().0
                        );
                    }
                }
                if test_num == 3 {
                    // Only ids up to 7 should be added to the list.
                    for item in &changes.added {
                        let id = id_map.iter().find(|(_, v)| **v == item.id).unwrap().0;
                        assert!(id < &7, "{:?} = {:?} (id: {})", query, changes, id);
                    }
                }

                if let JMAPState::Initial = state {
                    new_state = changes.new_query_state;
                }
            }
        }
        states.push(new_state);
    }
}