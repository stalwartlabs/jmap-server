use std::{
    collections::{HashMap, HashSet},
    iter::FromIterator,
};

use async_recursion::async_recursion;
use jmap_mail::{
    get::{JMAPMailGet, JMAPMailGetArguments},
    import::JMAPMailLocalStoreImport,
    mailbox::{
        JMAPMailMailbox, JMAPMailboxComparator, JMAPMailboxFilterCondition, JMAPMailboxProperties,
        JMAPMailboxQueryArguments, JMAPMailboxSetArguments,
    },
    set::JMAPMailSet,
};
use jmap_store::{
    changes::{JMAPChangesRequest, JMAPState},
    id::JMAPIdSerialize,
    json::JSONValue,
    JMAPComparator, JMAPFilter, JMAPGet, JMAPQueryRequest, JMAPSet,
};

use store::{changelog::RaftId, JMAPId, JMAPIdPrefix, JMAPStore, Store};

const TEST_MAILBOXES: &[u8] = br#"
[
    {
        "id": "inbox",
        "name": "Inbox",
        "role": "INBOX",
        "sortOrder": 5,
        "children": [
            {
                "name": "Level 1",
                "id": "1",
                "sortOrder": 4,
                "children": [
                    {
                        "name": "Sub-Level 1.1",
                        "id": "1.1",

                        "sortOrder": 3,
                        "children": [
                            {
                                "name": "Z-Sub-Level 1.1.1",
                                "id": "1.1.1",
                                "sortOrder": 2,
                                "children": [
                                    {
                                        "name": "X-Sub-Level 1.1.1.1",
                                        "id": "1.1.1.1",
                                        "sortOrder": 1,
                                        "children": [
                                            {
                                                "name": "Y-Sub-Level 1.1.1.1.1",
                                                "id": "1.1.1.1.1",
                                                "sortOrder": 0
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "name": "Sub-Level 1.2",
                        "id": "1.2",
                        "sortOrder": 7,
                        "children": [
                            {
                                "name": "Z-Sub-Level 1.2.1",
                                "id": "1.2.1",
                                "sortOrder": 6
                            }
                        ]
                    }
                ]
            },
            {
                "name": "Level 2",
                "id": "2",
                "sortOrder": 8
            },
            {
                "name": "Level 3",
                "id": "3",
                "sortOrder": 9
            }
        ]
    },
    {
        "id": "sent",
        "name": "Sent",
        "role": "SENT",
        "sortOrder": 15
    },
    {
        "id": "drafts",
        "name": "Drafts",
        "role": "DRAFTS",
        "sortOrder": 14
    },
    {
        "id": "trash",
        "name": "Trash",
        "role": "TRASH",
        "sortOrder": 13
    },
    {
        "id": "spam",
        "name": "Spam",
        "role": "SPAM",
        "sortOrder": 12,
        "children": [{
            "id": "spam1",
            "name": "Work Spam",
            "sortOrder": 11,
            "children": [{
                "id": "spam2",
                "name": "Friendly Spam",
                "sortOrder": 10
            }]
        }]
    }
]
"#;

pub async fn jmap_mailbox<T>(mail_store: JMAPStore<T>)
where
    T: for<'x> Store<'x> + 'static,
{
    let mut id_map = HashMap::new();
    create_nested_mailboxes(
        &mail_store,
        None,
        serde_json::from_slice(TEST_MAILBOXES).unwrap(),
        &mut id_map,
    )
    .await;

    // Sort by name
    assert_eq!(
        mail_store
            .mailbox_query(JMAPQueryRequest {
                account_id: 0,
                filter: JMAPFilter::None,
                sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 100,
                calculate_total: true,
                arguments: JMAPMailboxQueryArguments {
                    sort_as_tree: false,
                    filter_as_tree: false,
                },
            })
            .await
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id).unwrap())
            .collect::<Vec<_>>(),
        [
            "drafts",
            "spam2",
            "inbox",
            "1",
            "2",
            "3",
            "sent",
            "spam",
            "1.1",
            "1.2",
            "trash",
            "spam1",
            "1.1.1.1",
            "1.1.1.1.1",
            "1.1.1",
            "1.2.1"
        ]
    );

    // Sort by name as tree
    assert_eq!(
        mail_store
            .mailbox_query(JMAPQueryRequest {
                account_id: 0,
                filter: JMAPFilter::None,
                sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 100,
                calculate_total: true,
                arguments: JMAPMailboxQueryArguments {
                    sort_as_tree: true,
                    filter_as_tree: false,
                },
            })
            .await
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id).unwrap())
            .collect::<Vec<_>>(),
        [
            "drafts",
            "inbox",
            "1",
            "1.1",
            "1.1.1",
            "1.1.1.1",
            "1.1.1.1.1",
            "1.2",
            "1.2.1",
            "2",
            "3",
            "sent",
            "spam",
            "spam1",
            "spam2",
            "trash"
        ]
    );

    assert_eq!(
        mail_store
            .mailbox_query(JMAPQueryRequest {
                account_id: 0,
                filter: JMAPFilter::condition(JMAPMailboxFilterCondition::Name(
                    "level".to_string()
                )),
                sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 100,
                calculate_total: true,
                arguments: JMAPMailboxQueryArguments {
                    sort_as_tree: true,
                    filter_as_tree: false,
                },
            })
            .await
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id).unwrap())
            .collect::<Vec<_>>(),
        [
            "1",
            "1.1",
            "1.1.1",
            "1.1.1.1",
            "1.1.1.1.1",
            "1.2",
            "1.2.1",
            "2",
            "3"
        ]
    );

    // Filter as tree
    assert_eq!(
        mail_store
            .mailbox_query(JMAPQueryRequest {
                account_id: 0,
                filter: JMAPFilter::condition(JMAPMailboxFilterCondition::Name("spam".to_string())),
                sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 100,
                calculate_total: true,
                arguments: JMAPMailboxQueryArguments {
                    sort_as_tree: true,
                    filter_as_tree: true,
                },
            })
            .await
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id).unwrap())
            .collect::<Vec<_>>(),
        ["spam", "spam1", "spam2"]
    );

    assert!(mail_store
        .mailbox_query(JMAPQueryRequest {
            account_id: 0,
            filter: JMAPFilter::condition(JMAPMailboxFilterCondition::Name("level".to_string())),
            sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
            position: 0,
            anchor: None,
            anchor_offset: 0,
            limit: 100,
            calculate_total: true,
            arguments: JMAPMailboxQueryArguments {
                sort_as_tree: true,
                filter_as_tree: true,
            },
        })
        .await
        .unwrap()
        .ids
        .is_empty());

    // Role filters
    assert_eq!(
        mail_store
            .mailbox_query(JMAPQueryRequest {
                account_id: 0,
                filter: JMAPFilter::condition(JMAPMailboxFilterCondition::Role(
                    "inbox".to_string()
                )),
                sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 100,
                calculate_total: true,
                arguments: JMAPMailboxQueryArguments {
                    sort_as_tree: false,
                    filter_as_tree: false,
                },
            })
            .await
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id).unwrap())
            .collect::<Vec<_>>(),
        ["inbox"]
    );

    assert_eq!(
        mail_store
            .mailbox_query(JMAPQueryRequest {
                account_id: 0,
                filter: JMAPFilter::condition(JMAPMailboxFilterCondition::HasAnyRole),
                sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 100,
                calculate_total: true,
                arguments: JMAPMailboxQueryArguments {
                    sort_as_tree: false,
                    filter_as_tree: false,
                },
            })
            .await
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id).unwrap())
            .collect::<Vec<_>>(),
        ["drafts", "inbox", "sent", "spam", "trash"]
    );

    // Duplicate role
    assert!(mail_store
        .mailbox_set(JMAPSet {
            account_id: 0,
            if_in_state: None,
            create: JSONValue::Null,
            update: HashMap::from_iter([(
                get_mailbox_id(&id_map, "sent"),
                HashMap::from_iter([("role".to_string(), "INBOX".to_string().into())]).into(),
            )])
            .into(),
            destroy: JSONValue::Null,
            arguments: JMAPMailboxSetArguments {
                remove_emails: false,
            },
        })
        .await
        .unwrap()
        .not_updated
        .unwrap_object()
        .unwrap()
        .remove(&get_mailbox_id(&id_map, "sent"))
        .is_some());

    // Duplicate name
    assert!(mail_store
        .mailbox_set(JMAPSet {
            account_id: 0,
            if_in_state: None,
            create: JSONValue::Null,
            update: HashMap::from_iter([(
                get_mailbox_id(&id_map, "2"),
                HashMap::from_iter([("name".to_string(), "Level 3".to_string().into())]).into(),
            )])
            .into(),
            destroy: JSONValue::Null,
            arguments: JMAPMailboxSetArguments {
                remove_emails: false,
            },
        })
        .await
        .unwrap()
        .not_updated
        .unwrap_object()
        .unwrap()
        .remove(&get_mailbox_id(&id_map, "2"))
        .is_some());

    // Circular relationship
    assert!(mail_store
        .mailbox_set(JMAPSet {
            account_id: 0,
            if_in_state: None,
            create: JSONValue::Null,
            update: HashMap::from_iter([(
                get_mailbox_id(&id_map, "1"),
                HashMap::from_iter([(
                    "parentId".to_string(),
                    get_mailbox_id(&id_map, "1.1.1.1.1").into()
                )])
                .into(),
            )])
            .into(),
            destroy: JSONValue::Null,
            arguments: JMAPMailboxSetArguments {
                remove_emails: false,
            },
        })
        .await
        .unwrap()
        .not_updated
        .unwrap_object()
        .unwrap()
        .remove(&get_mailbox_id(&id_map, "1"))
        .is_some());

    assert!(mail_store
        .mailbox_set(JMAPSet {
            account_id: 0,
            if_in_state: None,
            create: JSONValue::Null,
            update: HashMap::from_iter([(
                get_mailbox_id(&id_map, "1"),
                HashMap::from_iter([("parentId".to_string(), get_mailbox_id(&id_map, "1").into())])
                    .into(),
            )])
            .into(),
            destroy: JSONValue::Null,
            arguments: JMAPMailboxSetArguments {
                remove_emails: false,
            },
        })
        .await
        .unwrap()
        .not_updated
        .unwrap_object()
        .unwrap()
        .remove(&get_mailbox_id(&id_map, "1"))
        .is_some());

    // Invalid parent ID
    assert!(mail_store
        .mailbox_set(JMAPSet {
            account_id: 0,
            if_in_state: None,
            create: JSONValue::Null,
            update: HashMap::from_iter([(
                get_mailbox_id(&id_map, "1"),
                HashMap::from_iter([("parentId".to_string(), JMAPId::MAX.to_jmap_string().into())])
                    .into(),
            )])
            .into(),
            destroy: JSONValue::Null,
            arguments: JMAPMailboxSetArguments {
                remove_emails: false,
            },
        })
        .await
        .unwrap()
        .not_updated
        .unwrap_object()
        .unwrap()
        .remove(&get_mailbox_id(&id_map, "1"))
        .is_some());

    // Get state
    let state = mail_store
        .mailbox_changes(JMAPChangesRequest {
            account: 0,
            since_state: JMAPState::Initial,
            max_changes: 0,
        })
        .await
        .unwrap()
        .new_state;

    // Rename and move mailbox
    assert_eq!(
        mail_store
            .mailbox_set(JMAPSet {
                account_id: 0,
                if_in_state: None,
                create: JSONValue::Null,
                update: HashMap::from_iter([(
                    get_mailbox_id(&id_map, "1.1.1.1.1"),
                    HashMap::from_iter([
                        ("name".to_string(), "Renamed and moved".to_string().into()),
                        ("parentId".to_string(), get_mailbox_id(&id_map, "2").into())
                    ])
                    .into(),
                )])
                .into(),
                destroy: JSONValue::Null,
                arguments: JMAPMailboxSetArguments {
                    remove_emails: false,
                },
            })
            .await
            .unwrap()
            .not_updated,
        JSONValue::Null
    );

    // Verify changes
    let state = mail_store
        .mailbox_changes(JMAPChangesRequest {
            account: 0,
            since_state: state,
            max_changes: 0,
        })
        .await
        .unwrap();
    assert_eq!(state.total_changes, 1);
    assert!(state.updated.len() == 1);
    assert!(
        state.arguments.updated_properties.is_empty(),
        "{:?}",
        state.arguments.updated_properties
    );
    let state = state.new_state;

    // Insert email into Inbox
    let message_id = mail_store
        .mail_import_blob(
            0,
            RaftId::default(),
            b"From: test@test.com\nSubject: hey\n\ntest",
            vec![JMAPId::from_jmap_string(&get_mailbox_id(&id_map, "inbox"))
                .unwrap()
                .get_document_id()],
            vec![],
            None,
        )
        .await
        .unwrap()
        .unwrap_object()
        .unwrap()
        .remove("id")
        .unwrap()
        .unwrap_string()
        .unwrap();

    // Only email properties must have changed
    let state = mail_store
        .mailbox_changes(JMAPChangesRequest {
            account: 0,
            since_state: state,
            max_changes: 0,
        })
        .await
        .unwrap();
    assert_eq!(state.total_changes, 1);
    assert_eq!(
        state.updated,
        HashSet::from_iter([JMAPId::from_jmap_string(&get_mailbox_id(&id_map, "inbox")).unwrap()])
    );
    assert_eq!(
        state.arguments.updated_properties,
        vec![
            JMAPMailboxProperties::TotalEmails,
            JMAPMailboxProperties::UnreadEmails,
            JMAPMailboxProperties::TotalThreads,
            JMAPMailboxProperties::UnreadThreads,
        ]
    );
    let state = state.new_state;

    // Move email from Inbox to Trash
    assert_eq!(
        mail_store
            .mail_set(JMAPSet {
                account_id: 0,
                if_in_state: None,
                create: JSONValue::Null,
                update: HashMap::from_iter([(
                    message_id.clone(),
                    HashMap::from_iter([(
                        "mailboxIds".to_string(),
                        HashMap::from_iter([(get_mailbox_id(&id_map, "trash"), true.into())])
                            .into()
                    )])
                    .into(),
                )])
                .into(),
                destroy: JSONValue::Null,
                arguments: (),
            })
            .await
            .unwrap()
            .not_updated,
        JSONValue::Null
    );

    // E-mail properties of both Inbox and Trash must have changed
    let state = mail_store
        .mailbox_changes(JMAPChangesRequest {
            account: 0,
            since_state: state,
            max_changes: 0,
        })
        .await
        .unwrap();
    assert_eq!(state.total_changes, 2);
    assert_eq!(
        state.updated,
        HashSet::from_iter([
            JMAPId::from_jmap_string(&get_mailbox_id(&id_map, "inbox")).unwrap(),
            JMAPId::from_jmap_string(&get_mailbox_id(&id_map, "trash")).unwrap()
        ])
    );
    assert_eq!(
        state.arguments.updated_properties,
        vec![
            JMAPMailboxProperties::TotalEmails,
            JMAPMailboxProperties::UnreadEmails,
            JMAPMailboxProperties::TotalThreads,
            JMAPMailboxProperties::UnreadThreads,
        ]
    );

    // Deleting folders with children is not allowed
    assert_eq!(
        mail_store
            .mailbox_set(JMAPSet {
                account_id: 0,
                if_in_state: None,
                create: JSONValue::Null,
                update: JSONValue::Null,
                destroy: vec![get_mailbox_id(&id_map, "1").into()].into(),
                arguments: JMAPMailboxSetArguments {
                    remove_emails: false,
                },
            })
            .await
            .unwrap()
            .destroyed,
        JSONValue::Null,
    );

    // Deleting folders with contents is not allowed (unless remove_emails is true)
    assert_eq!(
        mail_store
            .mailbox_set(JMAPSet {
                account_id: 0,
                if_in_state: None,
                create: JSONValue::Null,
                update: JSONValue::Null,
                destroy: vec![get_mailbox_id(&id_map, "trash").into()].into(),
                arguments: JMAPMailboxSetArguments {
                    remove_emails: false,
                },
            })
            .await
            .unwrap()
            .destroyed,
        JSONValue::Null,
    );

    // Delete Trash folder and its contents
    assert_eq!(
        mail_store
            .mailbox_set(JMAPSet {
                account_id: 0,
                if_in_state: None,
                create: JSONValue::Null,
                update: JSONValue::Null,
                destroy: vec![get_mailbox_id(&id_map, "trash").into()].into(),
                arguments: JMAPMailboxSetArguments {
                    remove_emails: true,
                },
            })
            .await
            .unwrap()
            .not_destroyed,
        JSONValue::Null,
    );

    // Verify that Trash folder and its contents are gone
    assert_eq!(
        mail_store
            .mailbox_get(JMAPGet {
                account_id: 0,
                ids: vec![JMAPId::from_jmap_string(&get_mailbox_id(&id_map, "trash")).unwrap()]
                    .into(),
                properties: None,
                arguments: (),
            })
            .await
            .unwrap()
            .not_found,
        vec![JMAPId::from_jmap_string(&get_mailbox_id(&id_map, "trash")).unwrap()].into()
    );
    assert_eq!(
        mail_store
            .mail_get(JMAPGet {
                account_id: 0,
                ids: vec![JMAPId::from_jmap_string(&message_id).unwrap()].into(),
                properties: None,
                arguments: JMAPMailGetArguments::default(),
            })
            .await
            .unwrap()
            .not_found,
        vec![JMAPId::from_jmap_string(&message_id).unwrap()].into()
    );

    // Check search results after changing folder properties
    assert_eq!(
        mail_store
            .mailbox_set(JMAPSet {
                account_id: 0,
                if_in_state: None,
                create: JSONValue::Null,
                update: HashMap::from_iter([(
                    get_mailbox_id(&id_map, "drafts"),
                    HashMap::from_iter([
                        ("name".to_string(), "Borradores".to_string().into()),
                        ("role".to_string(), JSONValue::Null),
                        ("sortOrder".to_string(), 100u64.into()),
                        ("parentId".to_string(), get_mailbox_id(&id_map, "2").into())
                    ])
                    .into(),
                )])
                .into(),
                destroy: JSONValue::Null,
                arguments: JMAPMailboxSetArguments {
                    remove_emails: false,
                },
            })
            .await
            .unwrap()
            .not_updated,
        JSONValue::Null
    );

    assert_eq!(
        mail_store
            .mailbox_query(JMAPQueryRequest {
                account_id: 0,
                filter: JMAPFilter::and(vec![
                    JMAPFilter::condition(JMAPMailboxFilterCondition::Name(
                        "Borradores".to_string()
                    )),
                    JMAPFilter::condition(JMAPMailboxFilterCondition::ParentId(
                        JMAPId::from_jmap_string(&get_mailbox_id(&id_map, "2")).unwrap() + 1
                    )),
                    JMAPFilter::not(vec![JMAPFilter::condition(
                        JMAPMailboxFilterCondition::HasAnyRole
                    )]),
                ]),
                sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 100,
                calculate_total: true,
                arguments: JMAPMailboxQueryArguments {
                    sort_as_tree: false,
                    filter_as_tree: false,
                },
            })
            .await
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id).unwrap())
            .collect::<Vec<_>>(),
        ["drafts",]
    );

    assert!(mail_store
        .mailbox_query(JMAPQueryRequest {
            account_id: 0,
            filter: JMAPFilter::condition(JMAPMailboxFilterCondition::Name("Drafts".to_string())),
            sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
            position: 0,
            anchor: None,
            anchor_offset: 0,
            limit: 100,
            calculate_total: true,
            arguments: JMAPMailboxQueryArguments {
                sort_as_tree: false,
                filter_as_tree: false,
            },
        })
        .await
        .unwrap()
        .ids
        .is_empty());

    assert!(mail_store
        .mailbox_query(JMAPQueryRequest {
            account_id: 0,
            filter: JMAPFilter::condition(JMAPMailboxFilterCondition::Role("drafts".to_string())),
            sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
            position: 0,
            anchor: None,
            anchor_offset: 0,
            limit: 100,
            calculate_total: true,
            arguments: JMAPMailboxQueryArguments {
                sort_as_tree: false,
                filter_as_tree: false,
            },
        })
        .await
        .unwrap()
        .ids
        .is_empty());

    assert_eq!(
        mail_store
            .mailbox_query(JMAPQueryRequest {
                account_id: 0,
                filter: JMAPFilter::condition(JMAPMailboxFilterCondition::ParentId(0)),
                sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 100,
                calculate_total: true,
                arguments: JMAPMailboxQueryArguments {
                    sort_as_tree: false,
                    filter_as_tree: false,
                },
            })
            .await
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id).unwrap())
            .collect::<Vec<_>>(),
        ["inbox", "sent", "spam",]
    );

    assert_eq!(
        mail_store
            .mailbox_query(JMAPQueryRequest {
                account_id: 0,
                filter: JMAPFilter::condition(JMAPMailboxFilterCondition::HasAnyRole),
                sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 100,
                calculate_total: true,
                arguments: JMAPMailboxQueryArguments {
                    sort_as_tree: false,
                    filter_as_tree: false,
                },
            })
            .await
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id).unwrap())
            .collect::<Vec<_>>(),
        ["inbox", "sent", "spam",]
    );
}

fn get_mailbox_id(id_map: &HashMap<JMAPId, String>, local_id: &str) -> String {
    id_map
        .keys()
        .find(|id| id_map.get(id).unwrap() == local_id)
        .unwrap()
        .clone()
        .to_jmap_string()
}

#[async_recursion]
async fn create_nested_mailboxes<T>(
    mail_store: &JMAPStore<T>,
    parent_id: Option<JMAPId>,
    mailboxes: Vec<JSONValue>,
    id_map: &mut HashMap<JMAPId, String>,
) where
    T: for<'x> Store<'x> + 'static,
{
    for (mailbox_num, mut mailbox) in mailboxes.into_iter().enumerate() {
        let mut children = None;
        let mut local_id = None;

        if let JSONValue::Object(mailbox) = &mut mailbox {
            children = mailbox.remove("children");
            local_id = mailbox.remove("id").unwrap().unwrap_string();

            if let Some(parent_id) = parent_id {
                mailbox.insert("parentId".to_string(), parent_id.to_jmap_string().into());
            }
        }
        let mailbox_num = mailbox_num.to_string();
        let result = mail_store
            .mailbox_set(JMAPSet {
                account_id: 0,
                if_in_state: None,
                create: HashMap::from_iter([(mailbox_num.clone(), mailbox)]).into(),
                update: JSONValue::Null,
                destroy: JSONValue::Null,
                arguments: JMAPMailboxSetArguments {
                    remove_emails: false,
                },
            })
            .await
            .unwrap();

        assert_eq!(result.not_created, JSONValue::Null);

        let mailbox_id = JMAPId::from_jmap_string(
            &result
                .created
                .unwrap_object()
                .unwrap()
                .remove(&mailbox_num)
                .unwrap()
                .unwrap_object()
                .unwrap()
                .remove("id")
                .unwrap()
                .unwrap_string()
                .unwrap(),
        )
        .unwrap();

        if let Some(children) = children {
            create_nested_mailboxes(
                mail_store,
                mailbox_id.into(),
                children.unwrap_array().unwrap(),
                id_map,
            )
            .await;
        }

        assert!(id_map.insert(mailbox_id, local_id.unwrap()).is_none());
    }
}
