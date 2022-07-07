use actix_web::web;
use jmap_client::{
    client::Client,
    core::query::{Comparator, Filter},
    email,
    mailbox::Role,
};
use store::{write::options::Options, Store};

use std::collections::{HashMap, HashSet};

use jmap::types::{jmap::JMAPId, state::JMAPState};
use jmap_mail::mail::MessageField;
use store::{
    core::{collection::Collection, document::Document, tag::Tag},
    write::{batch::WriteBatch, options::IndexOptions},
};

use crate::{tests::store::utils::StoreCompareWith, JMAPServer};

use super::email_changes::LogAction;

pub async fn test<T>(server: web::Data<JMAPServer<T>>, client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Running Email QueryChanges tests...");

    let mailbox1_id = client
        .set_default_account_id(JMAPId::new(1).to_string())
        .mailbox_create("JMAP Changes 1", None::<String>, Role::None)
        .await
        .unwrap()
        .unwrap_id();
    let mailbox2_id = client
        .mailbox_create("JMAP Changes 2", None::<String>, Role::None)
        .await
        .unwrap()
        .unwrap_id();

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
                let jmap_id = JMAPId::parse(
                    client
                        .email_import(
                            format!(
                                "From: test_{}\nSubject: test_{}\n\ntest",
                                if change_num % 2 == 0 { 1 } else { 2 },
                                *id
                            )
                            .into_bytes(),
                            [if change_num % 2 == 0 {
                                &mailbox1_id
                            } else {
                                &mailbox2_id
                            }],
                            [if change_num % 2 == 0 { "1" } else { "2" }].into(),
                            Some(*id as i64),
                        )
                        .await
                        .unwrap()
                        .id()
                        .unwrap(),
                )
                .unwrap();

                id_map.insert(*id, jmap_id);
                if change_num % 2 == 0 {
                    type1_ids.insert(jmap_id);
                }
            }
            LogAction::Update(id) => {
                let id = *id_map.get(id).unwrap();
                let mut batch = WriteBatch::new(1);
                batch.log_update(Collection::Mail, id);
                server.store.write(batch).unwrap();
                updated_ids.insert(id);
            }
            LogAction::Delete(id) => {
                let id = *id_map.get(id).unwrap();
                client.email_destroy(&id.to_string()).await.unwrap();

                let mut batch = WriteBatch::new(1);
                let mut document = Document::new(Collection::Mail, id.get_document_id());
                document.tag(
                    MessageField::ThreadId,
                    Tag::Id(id.get_prefix_id()),
                    IndexOptions::new().clear(),
                );
                batch.delete_document(document);
                server.store.write(batch).unwrap();
                removed_ids.insert(id);
            }
            LogAction::Move(from, to) => {
                let id = *id_map.get(from).unwrap();
                let new_id = JMAPId::from_parts(thread_id, id.get_document_id());

                let mut batch = WriteBatch::new(1);
                let mut document = Document::new(Collection::Mail, id.get_document_id());

                document.tag(
                    MessageField::ThreadId,
                    Tag::Id(id.get_prefix_id()),
                    IndexOptions::new().clear(),
                );
                document.tag(
                    MessageField::ThreadId,
                    Tag::Id(thread_id),
                    IndexOptions::new(),
                );
                document.number(
                    MessageField::ThreadId,
                    thread_id,
                    IndexOptions::new().store(),
                );

                batch.update_document(document);
                batch.log_move(Collection::Mail, id, new_id);
                server.store.write(batch).unwrap();

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
                QueryChanges {
                    filter: None,
                    sort: vec![email::query::Comparator::received_at()],
                    since_query_state: state.clone(),
                    max_changes: 0,
                    up_to_id: None,
                },
                QueryChanges {
                    filter: Some(email::query::Filter::from("test_1").into()),
                    sort: vec![email::query::Comparator::received_at()],
                    since_query_state: state.clone(),
                    max_changes: 0,
                    up_to_id: None,
                },
                QueryChanges {
                    filter: Some(email::query::Filter::in_mailbox(&mailbox1_id).into()),
                    sort: vec![email::query::Comparator::received_at()],
                    since_query_state: state.clone(),
                    max_changes: 0,
                    up_to_id: None,
                },
                QueryChanges {
                    filter: None,
                    sort: vec![email::query::Comparator::received_at()],
                    since_query_state: state.clone(),
                    max_changes: 0,
                    up_to_id: id_map
                        .get(&7)
                        .map(|id| id.to_string().into())
                        .unwrap_or(None),
                },
            ]
            .into_iter()
            .enumerate()
            {
                if test_num == 3 && query.up_to_id.is_none() {
                    continue;
                }
                let mut request = client.build();
                let query_request = request
                    .query_email_changes(query.since_query_state.to_string())
                    .sort(query.sort);

                if let Some(filter) = query.filter {
                    query_request.filter(filter);
                }

                if let Some(up_to_id) = query.up_to_id {
                    query_request.up_to_id(up_to_id);
                }

                let changes = request.send_query_email_changes().await.unwrap();

                if test_num == 0 || test_num == 1 {
                    // Immutable filters should not return modified ids, only deletions.
                    for id in changes.removed() {
                        let id = JMAPId::parse(id).unwrap();
                        assert!(
                            removed_ids.contains(&id),
                            "{:?} (id: {})",
                            changes,
                            id_map.iter().find(|(_, v)| **v == id).unwrap().0
                        );
                    }
                }
                if test_num == 1 || test_num == 2 {
                    // Only type 1 results should be added to the list.
                    for item in changes.added() {
                        let id = JMAPId::parse(item.id()).unwrap();
                        assert!(
                            type1_ids.contains(&id),
                            "{:?} (id: {})",
                            changes,
                            id_map.iter().find(|(_, v)| **v == id).unwrap().0
                        );
                    }
                }
                if test_num == 3 {
                    // Only ids up to 7 should be added to the list.
                    for item in changes.added() {
                        let item_id = JMAPId::parse(item.id()).unwrap();
                        let id = id_map.iter().find(|(_, v)| **v == item_id).unwrap().0;
                        assert!(id < &7, "{:?} (id: {})", changes, id);
                    }
                }

                if let JMAPState::Initial = state {
                    new_state = JMAPState::parse(changes.new_query_state()).unwrap();
                }
            }
        }
        states.push(new_state);
    }

    client.mailbox_destroy(&mailbox1_id, true).await.unwrap();
    client.mailbox_destroy(&mailbox2_id, true).await.unwrap();

    server.store.assert_is_empty();
}

#[derive(Debug, Clone)]
pub struct QueryChanges {
    pub filter: Option<Filter<email::query::Filter>>,
    pub sort: Vec<Comparator<email::query::Comparator>>,
    pub since_query_state: JMAPState,
    pub max_changes: usize,
    pub up_to_id: Option<String>,
}
