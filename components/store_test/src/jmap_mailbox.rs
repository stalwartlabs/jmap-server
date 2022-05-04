use std::{collections::HashMap, iter::FromIterator};

use jmap::{
    id::{state::JMAPState, JMAPIdSerialize},
    jmap_store::{changes::JMAPChanges, get::JMAPGet, query::JMAPQuery, set::JMAPSet},
    protocol::json::JSONValue,
    request::{changes::ChangesRequest, get::GetRequest, query::QueryRequest, set::SetRequest},
};
use jmap_mail::{
    mail::{get::GetMail, import::JMAPMailImport, set::SetMail},
    mailbox::{
        changes::ChangesMailbox, get::GetMailbox, query::QueryMailbox, set::SetMailbox,
        MailboxProperty,
    },
};

use store::{core::JMAPIdPrefix, AccountId, JMAPId, JMAPStore, Store};

use crate::{jmap_mail_get::build_mail_get_arguments, JMAPComparator, JMAPFilter};

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

#[derive(Debug, Clone)]
pub enum JMAPMailboxFilterCondition {
    ParentId(Option<JMAPId>),
    Name(String),
    Role(String),
    HasAnyRole(bool),
    IsSubscribed(bool),
}

#[derive(Debug, Clone)]
pub enum JMAPMailboxComparator {
    Name,
    Role,
    ParentId,
}

impl From<JMAPComparator<JMAPMailboxComparator>> for jmap::request::query::Comparator {
    fn from(comp: JMAPComparator<JMAPMailboxComparator>) -> Self {
        jmap::request::query::Comparator {
            property: match comp.property {
                JMAPMailboxComparator::Name => "name".to_string(),
                JMAPMailboxComparator::Role => "role".to_string(),
                JMAPMailboxComparator::ParentId => "parentId".to_string(),
            },
            is_ascending: comp.is_ascending,
            collation: None,
            arguments: HashMap::new(),
        }
    }
}

impl From<JMAPMailboxFilterCondition> for JSONValue {
    fn from(condition: JMAPMailboxFilterCondition) -> Self {
        let mut json = HashMap::new();
        match condition {
            JMAPMailboxFilterCondition::ParentId(id) => {
                json.insert(
                    "parentId".to_string(),
                    id.map(|id| id.to_jmap_string()).into(),
                );
            }
            JMAPMailboxFilterCondition::Name(name) => {
                json.insert("name".to_string(), name.into());
            }
            JMAPMailboxFilterCondition::Role(role) => {
                json.insert("role".to_string(), role.into());
            }
            JMAPMailboxFilterCondition::HasAnyRole(b) => {
                json.insert("hasAnyRole".to_string(), b.into());
            }
            JMAPMailboxFilterCondition::IsSubscribed(b) => {
                json.insert("isSubscribed".to_string(), b.into());
            }
        }

        json.into()
    }
}

#[derive(Debug, Clone)]
pub struct MailboxQueryRequest {
    pub account_id: AccountId,
    pub filter: JMAPFilter<JMAPMailboxFilterCondition>,
    pub sort: Vec<JMAPComparator<JMAPMailboxComparator>>,
    pub position: i64,
    pub anchor: Option<JMAPId>,
    pub anchor_offset: i64,
    pub limit: usize,
    pub calculate_total: bool,
    pub sort_as_tree: bool,
    pub filter_as_tree: bool,
}

impl From<MailboxQueryRequest> for QueryRequest {
    fn from(request: MailboxQueryRequest) -> Self {
        QueryRequest {
            account_id: request.account_id,
            filter: request.filter.into(),
            sort: request
                .sort
                .into_iter()
                .map(|c| c.into())
                .collect::<Vec<_>>()
                .into(),
            position: request.position,
            anchor: request.anchor,
            anchor_offset: request.anchor_offset,
            limit: request.limit,
            calculate_total: request.calculate_total,
            arguments: HashMap::from_iter([
                ("sortAsTree".to_string(), request.sort_as_tree.into()),
                ("filterAsTree".to_string(), request.filter_as_tree.into()),
            ]),
        }
    }
}

pub fn jmap_mailbox<T>(mail_store: &JMAPStore<T>, account_id: AccountId)
where
    T: for<'x> Store<'x> + 'static,
{
    let mut id_map = HashMap::new();
    create_nested_mailboxes(
        mail_store,
        None,
        serde_json::from_slice(TEST_MAILBOXES).unwrap(),
        &mut id_map,
        account_id,
    );

    // Sort by name
    assert_eq!(
        mail_store
            .query::<QueryMailbox<T>>(
                MailboxQueryRequest {
                    account_id,
                    filter: JMAPFilter::None,
                    sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                    position: 0,
                    anchor: None,
                    anchor_offset: 0,
                    limit: 100,
                    calculate_total: true,
                    sort_as_tree: false,
                    filter_as_tree: false,
                }
                .into()
            )
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id.to_jmap_id().unwrap()).unwrap())
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
            .query::<QueryMailbox<T>>(
                MailboxQueryRequest {
                    account_id,
                    filter: JMAPFilter::None,
                    sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                    position: 0,
                    anchor: None,
                    anchor_offset: 0,
                    limit: 100,
                    calculate_total: true,

                    sort_as_tree: true,
                    filter_as_tree: false,
                }
                .into()
            )
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id.to_jmap_id().unwrap()).unwrap())
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
            .query::<QueryMailbox<T>>(
                MailboxQueryRequest {
                    account_id,
                    filter: JMAPFilter::condition(JMAPMailboxFilterCondition::Name(
                        "level".to_string()
                    )),
                    sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                    position: 0,
                    anchor: None,
                    anchor_offset: 0,
                    limit: 100,
                    calculate_total: true,

                    sort_as_tree: true,
                    filter_as_tree: false,
                }
                .into()
            )
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id.to_jmap_id().unwrap()).unwrap())
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
            .query::<QueryMailbox<T>>(
                MailboxQueryRequest {
                    account_id,
                    filter: JMAPFilter::condition(JMAPMailboxFilterCondition::Name(
                        "spam".to_string()
                    )),
                    sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                    position: 0,
                    anchor: None,
                    anchor_offset: 0,
                    limit: 100,
                    calculate_total: true,
                    sort_as_tree: true,
                    filter_as_tree: true,
                }
                .into()
            )
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id.to_jmap_id().unwrap()).unwrap())
            .collect::<Vec<_>>(),
        ["spam", "spam1", "spam2"]
    );

    assert!(mail_store
        .query::<QueryMailbox<T>>(
            MailboxQueryRequest {
                account_id,
                filter: JMAPFilter::condition(JMAPMailboxFilterCondition::Name(
                    "level".to_string()
                )),
                sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 100,
                calculate_total: true,

                sort_as_tree: true,
                filter_as_tree: true,
            }
            .into()
        )
        .unwrap()
        .ids
        .is_empty());

    // Role filters
    assert_eq!(
        mail_store
            .query::<QueryMailbox<T>>(
                MailboxQueryRequest {
                    account_id,
                    filter: JMAPFilter::condition(JMAPMailboxFilterCondition::Role(
                        "inbox".to_string()
                    )),
                    sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                    position: 0,
                    anchor: None,
                    anchor_offset: 0,
                    limit: 100,
                    calculate_total: true,

                    sort_as_tree: false,
                    filter_as_tree: false,
                }
                .into()
            )
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id.to_jmap_id().unwrap()).unwrap())
            .collect::<Vec<_>>(),
        ["inbox"]
    );

    assert_eq!(
        mail_store
            .query::<QueryMailbox<T>>(
                MailboxQueryRequest {
                    account_id,
                    filter: JMAPFilter::condition(JMAPMailboxFilterCondition::HasAnyRole(true)),
                    sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                    position: 0,
                    anchor: None,
                    anchor_offset: 0,
                    limit: 100,
                    calculate_total: true,

                    sort_as_tree: false,
                    filter_as_tree: false,
                }
                .into()
            )
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id.to_jmap_id().unwrap()).unwrap())
            .collect::<Vec<_>>(),
        ["drafts", "inbox", "sent", "spam", "trash"]
    );

    // Duplicate role
    assert!(mail_store
        .set::<SetMailbox>(SetRequest {
            account_id,
            if_in_state: None,
            create: vec![],
            update: HashMap::from_iter([(
                get_mailbox_id(&id_map, "sent"),
                HashMap::from_iter([("role".to_string(), "INBOX".to_string().into())]).into(),
            )]),
            destroy: vec![],
            arguments: HashMap::new(),
            tombstone_deletions: false,
        })
        .unwrap()
        .not_updated
        .contains_key(&get_mailbox_id(&id_map, "sent")));

    // Duplicate name
    assert!(mail_store
        .set::<SetMailbox>(SetRequest {
            account_id,
            if_in_state: None,
            create: vec![],
            update: HashMap::from_iter([(
                get_mailbox_id(&id_map, "2"),
                HashMap::from_iter([("name".to_string(), "Level 3".to_string().into())]).into(),
            )]),
            destroy: vec![],
            arguments: HashMap::new(),
            tombstone_deletions: false,
        })
        .unwrap()
        .not_updated
        .contains_key(&get_mailbox_id(&id_map, "2")));

    // Circular relationship
    assert!(mail_store
        .set::<SetMailbox>(SetRequest {
            account_id,
            if_in_state: None,
            create: vec![],
            update: HashMap::from_iter([(
                get_mailbox_id(&id_map, "1"),
                HashMap::from_iter([(
                    "parentId".to_string(),
                    get_mailbox_id(&id_map, "1.1.1.1.1").into()
                )])
                .into(),
            )]),
            destroy: vec![],
            arguments: HashMap::new(),
            tombstone_deletions: false,
        })
        .unwrap()
        .not_updated
        .contains_key(&get_mailbox_id(&id_map, "1")));

    assert!(mail_store
        .set::<SetMailbox>(SetRequest {
            account_id,
            if_in_state: None,
            create: vec![],
            update: HashMap::from_iter([(
                get_mailbox_id(&id_map, "1"),
                HashMap::from_iter([("parentId".to_string(), get_mailbox_id(&id_map, "1").into())])
                    .into(),
            )]),
            destroy: vec![],
            arguments: HashMap::new(),
            tombstone_deletions: false,
        })
        .unwrap()
        .not_updated
        .contains_key(&get_mailbox_id(&id_map, "1")));

    // Invalid parent ID
    assert!(mail_store
        .set::<SetMailbox>(SetRequest {
            account_id,
            if_in_state: None,
            create: vec![],
            update: HashMap::from_iter([(
                get_mailbox_id(&id_map, "1"),
                HashMap::from_iter([("parentId".to_string(), JMAPId::MAX.to_jmap_string().into())])
                    .into(),
            )]),
            destroy: vec![],
            arguments: HashMap::new(),
            tombstone_deletions: false,
        })
        .unwrap()
        .not_updated
        .contains_key(&get_mailbox_id(&id_map, "1")));

    // Get state
    let state = mail_store
        .changes::<ChangesMailbox>(ChangesRequest {
            account_id,
            since_state: JMAPState::Initial,
            max_changes: 0,
            arguments: HashMap::new(),
        })
        .unwrap()
        .new_state;

    // Rename and move mailbox
    assert_eq!(
        mail_store
            .set::<SetMailbox>(SetRequest {
                account_id,
                if_in_state: None,
                create: vec![],
                update: HashMap::from_iter([(
                    get_mailbox_id(&id_map, "1.1.1.1.1"),
                    HashMap::from_iter([
                        ("name".to_string(), "Renamed and moved".to_string().into()),
                        ("parentId".to_string(), get_mailbox_id(&id_map, "2").into())
                    ])
                    .into(),
                )]),
                destroy: vec![],
                arguments: HashMap::new(),
                tombstone_deletions: false,
            })
            .unwrap()
            .not_updated,
        HashMap::new()
    );

    // Verify changes
    let state: JSONValue = mail_store
        .changes::<ChangesMailbox>(ChangesRequest {
            account_id,
            since_state: state,
            max_changes: 0,
            arguments: HashMap::new(),
        })
        .unwrap()
        .into();
    assert_eq!(state.eval_unwrap_unsigned_int("/totalChanges"), 1);
    assert!(state.eval_unwrap_array("/updated").len() == 1);
    assert!(
        state.eval("/updatedProperties").is_err(),
        "{:?}",
        state.eval("/updatedProperties").unwrap()
    );
    let state = state.eval_unwrap_jmap_state("/newState");

    // Insert email into Inbox
    let message_id = mail_store
        .mail_import(
            account_id,
            0.into(),
            b"From: test@test.com\nSubject: hey\n\ntest",
            vec![JMAPId::from_jmap_string(&get_mailbox_id(&id_map, "inbox"))
                .unwrap()
                .get_document_id()],
            vec![],
            None,
        )
        .unwrap()
        .id
        .to_jmap_string();

    // Only email properties must have changed
    let state: JSONValue = mail_store
        .changes::<ChangesMailbox>(ChangesRequest {
            account_id,
            since_state: state,
            max_changes: 0,
            arguments: HashMap::new(),
        })
        .unwrap()
        .into();
    assert_eq!(state.eval_unwrap_unsigned_int("/totalChanges"), 1);
    assert_eq!(
        state.eval("/updated").unwrap(),
        vec![get_mailbox_id(&id_map, "inbox").into()].into()
    );
    assert_eq!(
        state.eval("/updatedProperties").unwrap(),
        JSONValue::Array(vec![
            MailboxProperty::TotalEmails.into(),
            MailboxProperty::UnreadEmails.into(),
            MailboxProperty::TotalThreads.into(),
            MailboxProperty::UnreadThreads.into(),
        ])
    );
    let state = state.eval_unwrap_jmap_state("/newState");

    // Move email from Inbox to Trash
    assert_eq!(
        mail_store
            .set::<SetMail>(SetRequest {
                account_id,
                if_in_state: None,
                create: vec![],
                update: HashMap::from_iter([(
                    message_id.clone(),
                    HashMap::from_iter([(
                        "mailboxIds".to_string(),
                        HashMap::from_iter([(get_mailbox_id(&id_map, "trash"), true.into())])
                            .into()
                    )])
                    .into(),
                )]),
                destroy: vec![],
                arguments: HashMap::new(),
                tombstone_deletions: false,
            })
            .unwrap()
            .not_updated,
        HashMap::new()
    );

    // E-mail properties of both Inbox and Trash must have changed
    let state: JSONValue = mail_store
        .changes::<ChangesMailbox>(ChangesRequest {
            account_id,
            since_state: state,
            max_changes: 0,
            arguments: HashMap::new(),
        })
        .unwrap()
        .into();
    assert_eq!(state.eval_unwrap_unsigned_int("/totalChanges"), 2);
    let mut folder_ids = vec![
        JMAPId::from_jmap_string(&get_mailbox_id(&id_map, "trash")).unwrap(),
        JMAPId::from_jmap_string(&get_mailbox_id(&id_map, "inbox")).unwrap(),
    ];
    let mut updated_ids = state
        .eval_unwrap_array("/updated")
        .into_iter()
        .map(|i| i.to_jmap_id().unwrap())
        .collect::<Vec<_>>();
    updated_ids.sort_unstable();
    folder_ids.sort_unstable();
    assert_eq!(updated_ids, folder_ids);
    assert_eq!(
        state.eval("/updatedProperties").unwrap(),
        JSONValue::Array(vec![
            MailboxProperty::TotalEmails.into(),
            MailboxProperty::UnreadEmails.into(),
            MailboxProperty::TotalThreads.into(),
            MailboxProperty::UnreadThreads.into(),
        ])
    );

    // Deleting folders with children is not allowed
    assert_eq!(
        mail_store
            .set::<SetMailbox>(SetRequest {
                account_id,
                if_in_state: None,
                create: vec![],
                update: HashMap::new(),
                destroy: vec![get_mailbox_id(&id_map, "1").into()],
                arguments: HashMap::new(),
                tombstone_deletions: false,
            })
            .unwrap()
            .destroyed,
        Vec::new(),
    );

    // Deleting folders with contents is not allowed (unless remove_emails is true)
    assert_eq!(
        mail_store
            .set::<SetMailbox>(SetRequest {
                account_id,
                if_in_state: None,
                create: vec![],
                update: HashMap::new(),
                destroy: vec![get_mailbox_id(&id_map, "trash").into()],
                arguments: HashMap::new(),
                tombstone_deletions: false,
            })
            .unwrap()
            .destroyed,
        Vec::new(),
    );

    // Delete Trash folder and its contents
    assert_eq!(
        JSONValue::from(
            mail_store
                .set::<SetMailbox>(SetRequest {
                    account_id,
                    if_in_state: None,
                    create: vec![],
                    update: HashMap::new(),
                    destroy: vec![get_mailbox_id(&id_map, "trash").into()],
                    arguments: HashMap::from_iter([(
                        "onDestroyRemoveEmails".to_string(),
                        true.into(),
                    )]),
                    tombstone_deletions: false,
                })
                .unwrap()
        )
        .eval("/destroyed/0")
        .unwrap(),
        get_mailbox_id(&id_map, "trash").into(),
    );

    // Verify that Trash folder and its contents are gone
    assert_eq!(
        mail_store
            .get::<GetMailbox<T>>(GetRequest {
                account_id,
                ids: vec![JMAPId::from_jmap_string(&get_mailbox_id(&id_map, "trash")).unwrap()]
                    .into(),
                properties: JSONValue::Null,
                arguments: HashMap::new(),
            })
            .unwrap()
            .not_found,
        vec![get_mailbox_id(&id_map, "trash").into()]
    );
    assert_eq!(
        mail_store
            .get::<GetMail<T>>(GetRequest {
                account_id,
                ids: vec![JMAPId::from_jmap_string(&message_id).unwrap()].into(),
                properties: JSONValue::Null,
                arguments: build_mail_get_arguments(vec![], true, true, true, 0),
            })
            .unwrap()
            .not_found,
        vec![message_id.into()]
    );

    // Check search results after changing folder properties
    assert_eq!(
        mail_store
            .set::<SetMailbox>(SetRequest {
                account_id,
                if_in_state: None,
                create: vec![],
                update: HashMap::from_iter([(
                    get_mailbox_id(&id_map, "drafts"),
                    HashMap::from_iter([
                        ("name".to_string(), "Borradores".to_string().into()),
                        ("role".to_string(), JSONValue::Null),
                        ("sortOrder".to_string(), 100u64.into()),
                        ("parentId".to_string(), get_mailbox_id(&id_map, "2").into())
                    ])
                    .into(),
                )]),
                destroy: vec![],
                arguments: HashMap::new(),
                tombstone_deletions: false,
            })
            .unwrap()
            .not_updated,
        HashMap::new()
    );

    assert_eq!(
        mail_store
            .query::<QueryMailbox<T>>(
                MailboxQueryRequest {
                    account_id,
                    filter: JMAPFilter::and(vec![
                        JMAPFilter::condition(JMAPMailboxFilterCondition::Name(
                            "Borradores".to_string()
                        )),
                        JMAPFilter::condition(JMAPMailboxFilterCondition::ParentId(Some(
                            JMAPId::from_jmap_string(&get_mailbox_id(&id_map, "2")).unwrap()
                        ))),
                        JMAPFilter::not(vec![JMAPFilter::condition(
                            JMAPMailboxFilterCondition::HasAnyRole(true)
                        )]),
                    ]),
                    sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                    position: 0,
                    anchor: None,
                    anchor_offset: 0,
                    limit: 100,
                    calculate_total: true,
                    sort_as_tree: false,
                    filter_as_tree: false,
                }
                .into()
            )
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id.to_jmap_id().unwrap()).unwrap())
            .collect::<Vec<_>>(),
        ["drafts",]
    );

    assert!(mail_store
        .query::<QueryMailbox<T>>(
            MailboxQueryRequest {
                account_id,
                filter: JMAPFilter::condition(JMAPMailboxFilterCondition::Name(
                    "Drafts".to_string()
                )),
                sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 100,
                calculate_total: true,
                sort_as_tree: false,
                filter_as_tree: false,
            }
            .into()
        )
        .unwrap()
        .ids
        .is_empty());

    assert!(mail_store
        .query::<QueryMailbox<T>>(
            MailboxQueryRequest {
                account_id,
                filter: JMAPFilter::condition(JMAPMailboxFilterCondition::Role(
                    "drafts".to_string()
                )),
                sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 100,
                calculate_total: true,
                sort_as_tree: false,
                filter_as_tree: false,
            }
            .into()
        )
        .unwrap()
        .ids
        .is_empty());

    assert_eq!(
        mail_store
            .query::<QueryMailbox<T>>(
                MailboxQueryRequest {
                    account_id,
                    filter: JMAPFilter::condition(JMAPMailboxFilterCondition::ParentId(None)),
                    sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                    position: 0,
                    anchor: None,
                    anchor_offset: 0,
                    limit: 100,
                    calculate_total: true,
                    sort_as_tree: false,
                    filter_as_tree: false,
                }
                .into()
            )
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id.to_jmap_id().unwrap()).unwrap())
            .collect::<Vec<_>>(),
        ["inbox", "sent", "spam",]
    );

    assert_eq!(
        mail_store
            .query::<QueryMailbox<T>>(
                MailboxQueryRequest {
                    account_id,
                    filter: JMAPFilter::condition(JMAPMailboxFilterCondition::HasAnyRole(true)),
                    sort: vec![JMAPComparator::ascending(JMAPMailboxComparator::Name)],
                    position: 0,
                    anchor: None,
                    anchor_offset: 0,
                    limit: 100,
                    calculate_total: true,
                    sort_as_tree: false,
                    filter_as_tree: false,
                }
                .into()
            )
            .unwrap()
            .ids
            .into_iter()
            .map(|id| id_map.get(&id.to_jmap_id().unwrap()).unwrap())
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

fn create_nested_mailboxes<T>(
    mail_store: &JMAPStore<T>,
    parent_id: Option<JMAPId>,
    mailboxes: Vec<JSONValue>,
    id_map: &mut HashMap<JMAPId, String>,
    account_id: AccountId,
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
        let mailbox_num = format!("b{}", mailbox_num);
        let result: JSONValue = mail_store
            .set::<SetMailbox>(SetRequest {
                account_id,
                if_in_state: None,
                create: Vec::from_iter([(mailbox_num.clone(), mailbox)]),
                update: HashMap::new(),
                destroy: vec![],
                arguments: HashMap::new(),
                tombstone_deletions: false,
            })
            .unwrap()
            .into();

        assert_eq!(result.eval("/notCreated").unwrap(), HashMap::new().into());

        let mailbox_id = result.eval_unwrap_jmap_id(&format!("/created/{}/id", mailbox_num));

        if let Some(children) = children {
            create_nested_mailboxes(
                mail_store,
                mailbox_id.into(),
                children.unwrap_array().unwrap(),
                id_map,
                account_id,
            );
        }

        assert!(id_map.insert(mailbox_id, local_id.unwrap()).is_none());
    }
}

pub fn insert_mailbox<T>(
    mail_store: &JMAPStore<T>,
    account_id: AccountId,
    name: &str,
    role: Option<&str>,
) -> JMAPId
where
    T: for<'x> Store<'x> + 'static,
{
    let result: JSONValue = mail_store
        .set::<SetMailbox>(SetRequest {
            account_id,
            if_in_state: None,
            create: Vec::from_iter([(
                "my_id".to_string(),
                if let Some(role) = role {
                    HashMap::from_iter([
                        ("name".to_string(), name.to_string().into()),
                        ("role".to_string(), role.to_string().into()),
                    ])
                } else {
                    HashMap::from_iter([("name".to_string(), name.to_string().into())])
                }
                .into(),
            )]),
            update: HashMap::new(),
            destroy: vec![],
            arguments: HashMap::new(),
            tombstone_deletions: false,
        })
        .unwrap()
        .into();

    assert_eq!(result.eval("/notCreated").unwrap(), HashMap::new().into());

    result.eval_unwrap_jmap_id("/created/my_id/id")
}

pub fn update_mailbox<T>(
    mail_store: &JMAPStore<T>,
    account_id: AccountId,
    jmap_id: JMAPId,
    ref_id: u32,
    seq_id: u32,
) where
    T: for<'x> Store<'x> + 'static,
{
    assert_eq!(
        mail_store
            .set::<SetMailbox>(SetRequest {
                account_id,
                if_in_state: None,
                create: vec![],
                update: HashMap::from_iter([(
                    jmap_id.to_jmap_string(),
                    HashMap::from_iter([
                        (
                            "name".to_string(),
                            format!("Mailbox {}_{}", ref_id, seq_id).into()
                        ),
                        ("sortOrder".to_string(), ((ref_id * 100) + seq_id).into())
                    ])
                    .into(),
                )]),
                destroy: vec![],
                arguments: HashMap::new(),
                tombstone_deletions: false,
            })
            .unwrap()
            .not_updated,
        HashMap::new()
    );
}

pub fn delete_mailbox<T>(
    mail_store: &JMAPStore<T>,
    account_id: AccountId,
    jmap_id: JMAPId,
    tombstone_deletions: bool,
) where
    T: for<'x> Store<'x> + 'static,
{
    assert_eq!(
        mail_store
            .set::<SetMailbox>(SetRequest {
                account_id,
                if_in_state: None,
                create: vec![],
                update: HashMap::new(),
                destroy: vec![jmap_id.to_jmap_string().into()],
                arguments: HashMap::from_iter([
                    ("onDestroyRemoveEmails".to_string(), true.into(),)
                ]),
                tombstone_deletions,
            })
            .unwrap()
            .not_destroyed,
        HashMap::new()
    );
}
