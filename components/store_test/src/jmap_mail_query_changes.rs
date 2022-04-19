use std::{
    collections::{HashMap, HashSet},
    iter::FromIterator,
};

use jmap::{
    id::{state::JMAPState, JMAPIdSerialize},
    protocol::json::JSONValue,
    request::query_changes::QueryChangesRequest,
};
use jmap_mail::mail::{import::JMAPMailImport, query_changes::JMAPMailQueryChanges, MessageField};
use store::{
    batch::{Document, WriteBatch},
    field::DefaultOptions,
    AccountId, JMAPId, JMAPStore, Store, Tag,
};
use store::{Collection, JMAPIdPrefix};

use crate::{
    jmap_changes::LogAction,
    jmap_mail_query::{JMAPMailComparator, JMAPMailFilterCondition},
    JMAPComparator, JMAPFilter,
};

#[derive(Debug, Clone)]
pub struct MailQueryChangesRequest {
    pub account_id: AccountId,
    pub filter: JMAPFilter<JMAPMailFilterCondition>,
    pub sort: Vec<JMAPComparator<JMAPMailComparator>>,
    pub since_query_state: JMAPState,
    pub max_changes: usize,
    pub up_to_id: JSONValue,
    pub calculate_total: bool,
    pub collapse_threads: bool,
}

impl From<MailQueryChangesRequest> for QueryChangesRequest {
    fn from(request: MailQueryChangesRequest) -> Self {
        QueryChangesRequest {
            account_id: request.account_id,
            filter: request.filter.into(),
            sort: request
                .sort
                .into_iter()
                .map(|c| c.into())
                .collect::<Vec<_>>()
                .into(),
            calculate_total: request.calculate_total,
            since_query_state: request.since_query_state,
            max_changes: request.max_changes,
            up_to_id: request.up_to_id,
            arguments: HashMap::from_iter([(
                "collapseThreads".to_string(),
                request.collapse_threads.into(),
            )]),
        }
    }
}

pub fn jmap_mail_query_changes<T>(mail_store: &JMAPStore<T>, account_id: AccountId)
where
    T: for<'x> Store<'x> + 'static,
{
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
                    .mail_import_blob(
                        account_id,
                        format!(
                            "From: test_{}\nSubject: test_{}\n\ntest",
                            if change_num % 2 == 0 { 1 } else { 2 },
                            *id
                        )
                        .as_bytes()
                        .to_vec(),
                        vec![if change_num % 2 == 0 { 1 } else { 2 }],
                        vec![Tag::Text(if change_num % 2 == 0 {
                            "1".into()
                        } else {
                            "2".into()
                        })],
                        Some(*id as i64),
                    )
                    .unwrap()
                    .eval_unwrap_jmap_id("/id");

                id_map.insert(*id, jmap_id);
                if change_num % 2 == 0 {
                    type1_ids.insert(jmap_id);
                }
            }
            LogAction::Update(id) => {
                let id = *id_map.get(id).unwrap();
                let mut batch = WriteBatch::new(account_id, false);
                batch.log_update(Collection::Mail, id);
                mail_store.write(batch).unwrap();
                updated_ids.insert(id);
            }
            LogAction::Delete(id) => {
                let id = *id_map.get(id).unwrap();
                let mut batch = WriteBatch::new(account_id, false);
                batch.delete_document(Collection::Mail, id.get_document_id());
                batch.log_delete(Collection::Mail, id);
                mail_store.write(batch).unwrap();
                removed_ids.insert(id);
            }
            LogAction::Move(from, to) => {
                let id = *id_map.get(from).unwrap();
                let new_id = JMAPId::from_parts(thread_id, id.get_document_id());

                let mut batch = WriteBatch::new(account_id, false);
                let mut document = Document::new(Collection::Mail, id.get_document_id());
                document.tag(
                    MessageField::ThreadId,
                    Tag::Id(thread_id),
                    DefaultOptions::new(),
                );
                batch.update_document(document);
                batch.log_move(Collection::Mail, id, new_id);
                mail_store.write(batch).unwrap();

                id_map.insert(*to, new_id);
                if type1_ids.contains(&id) {
                    type1_ids.insert(new_id);
                }
                removed_ids.insert(id);
                thread_id += 1;
            }
            LogAction::UpdateChild(_) => unreachable!(),
        }

        let mut new_state = JMAPState::Initial;
        for state in &states {
            for (test_num, query) in vec![
                MailQueryChangesRequest {
                    account_id,
                    filter: JMAPFilter::None,
                    sort: vec![JMAPComparator::ascending(JMAPMailComparator::ReceivedAt)],
                    since_query_state: state.clone(),
                    max_changes: 0,
                    up_to_id: JSONValue::Null,
                    calculate_total: false,
                    collapse_threads: false,
                },
                MailQueryChangesRequest {
                    account_id,
                    filter: JMAPFilter::Condition(JMAPMailFilterCondition::From("test_1".into())),
                    sort: vec![JMAPComparator::ascending(JMAPMailComparator::ReceivedAt)],
                    since_query_state: state.clone(),
                    max_changes: 0,
                    up_to_id: JSONValue::Null,
                    calculate_total: false,
                    collapse_threads: false,
                },
                MailQueryChangesRequest {
                    account_id,
                    filter: JMAPFilter::Condition(JMAPMailFilterCondition::InMailbox(1)),
                    sort: vec![JMAPComparator::ascending(JMAPMailComparator::ReceivedAt)],
                    since_query_state: state.clone(),
                    max_changes: 0,
                    up_to_id: JSONValue::Null,
                    calculate_total: false,
                    collapse_threads: false,
                },
                MailQueryChangesRequest {
                    account_id,
                    filter: JMAPFilter::None,
                    sort: vec![JMAPComparator::ascending(JMAPMailComparator::ReceivedAt)],
                    since_query_state: state.clone(),
                    max_changes: 0,
                    up_to_id: id_map
                        .get(&7)
                        .map(|id| id.to_jmap_string().into())
                        .unwrap_or(JSONValue::Null),
                    calculate_total: false,
                    collapse_threads: false,
                },
            ]
            .into_iter()
            .enumerate()
            {
                if test_num == 3 && query.up_to_id.is_null() {
                    continue;
                }
                let changes = mail_store.mail_query_changes(query.clone().into()).unwrap();

                if test_num == 0 || test_num == 1 {
                    // Immutable filters should not return modified ids, only deletions.
                    for id in changes.eval_unwrap_array("/removed") {
                        let id = id.to_jmap_id().unwrap();
                        assert!(
                            removed_ids.contains(&id),
                            "{:?} = {:?} (id: {})",
                            query,
                            changes,
                            id_map.iter().find(|(_, v)| **v == id).unwrap().0
                        );
                    }
                }
                if test_num == 1 || test_num == 2 {
                    // Only type 1 results should be added to the list.
                    for item in changes.eval_unwrap_array("/added") {
                        let id = item.eval_unwrap_jmap_id("/id");
                        assert!(
                            type1_ids.contains(&id),
                            "{:?} = {:?} (id: {})",
                            query,
                            changes,
                            id_map.iter().find(|(_, v)| **v == id).unwrap().0
                        );
                    }
                }
                if test_num == 3 {
                    // Only ids up to 7 should be added to the list.
                    for item in changes.eval_unwrap_array("/added") {
                        let item_id = item.eval_unwrap_jmap_id("/id");
                        let id = id_map.iter().find(|(_, v)| **v == item_id).unwrap().0;
                        assert!(id < &7, "{:?} = {:?} (id: {})", query, changes, id);
                    }
                }

                if let JMAPState::Initial = state {
                    new_state = changes.eval_unwrap_jmap_state("/newQueryState");
                }
            }
        }
        states.push(new_state);
    }
}
