use std::{collections::HashMap, fs, iter::FromIterator, path::PathBuf};

use jmap_mail::{
    get::JMAPMailGetArguments, parse::get_message_blob, JMAPMailBodyProperties, JMAPMailGet,
    JMAPMailProperties, JMAPMailSet,
};
use jmap_store::{
    blob::JMAPLocalBlobStore,
    id::{BlobId, JMAPIdSerialize},
    json::{JSONNumber, JSONValue},
    local_store::JMAPLocalStore,
    JMAPGet, JMAPId, JMAPSet, JMAP_MAILBOX,
};
use store::{batch::WriteBatch, changelog::RaftId, Store};

use crate::jmap_mail_get::SortedJSONValue;

impl<'x> From<SortedJSONValue> for JSONValue {
    fn from(value: SortedJSONValue) -> Self {
        match value {
            SortedJSONValue::Null => JSONValue::Null,
            SortedJSONValue::Bool(b) => JSONValue::Bool(b),
            SortedJSONValue::String(s) => JSONValue::String(s),
            SortedJSONValue::Number(n) => JSONValue::Number(JSONNumber::PosInt(n)),
            SortedJSONValue::Array(a) => {
                JSONValue::Array(a.into_iter().map(JSONValue::from).collect())
            }
            SortedJSONValue::Object(o) => JSONValue::Object(
                o.into_iter()
                    .map(|(k, v)| (k, JSONValue::from(v)))
                    .collect(),
            ),
        }
    }
}

fn store_blobs<'x, T>(mail_store: &JMAPLocalStore<T>, value: &mut JSONValue)
where
    T: Store<'x>,
{
    match value {
        JSONValue::Object(o) => {
            for (k, v) in o.iter_mut() {
                if k == "blobId" {
                    if let JSONValue::String(value) = v {
                        *value = mail_store
                            .upload_blob(0, value.as_bytes())
                            .unwrap()
                            .to_jmap_string();
                    } else {
                        panic!("blobId is not a string");
                    }
                } else {
                    store_blobs(mail_store, v);
                }
            }
        }
        JSONValue::Array(a) => {
            for v in a.iter_mut() {
                store_blobs(mail_store, v);
            }
        }
        _ => {}
    }
}

fn replace_boundaries(mut string: String) -> String {
    let mut last_pos = 0;
    let mut boundaries = Vec::new();

    while let Some(pos) = string[last_pos..].find("boundary=") {
        let mut boundary = string[last_pos + pos..].split('"').nth(1).unwrap();
        if boundary.ends_with('\\') {
            boundary = &boundary[..boundary.len() - 1];
        }
        boundaries.push(boundary.to_string());
        last_pos += pos + 9;
    }

    if !boundaries.is_empty() {
        for (pos, boundary) in boundaries.into_iter().enumerate() {
            string = string.replace(&boundary, &format!("boundary_{}", pos));
        }
    }

    string
}

fn assert_diff(str1: &str, str2: &str, filename: &str) {
    for ((pos1, ch1), (pos2, ch2)) in str1.char_indices().zip(str2.char_indices()) {
        if ch1 != ch2 {
            panic!(
                "{:?} != {:?} ({})",
                &str1[if pos1 >= 10 { pos1 - 10 } else { pos1 }..pos1 + 10],
                &str2[if pos2 >= 10 { pos2 - 10 } else { pos2 }..pos2 + 10],
                filename
            );
        }
    }

    assert_eq!(str1.len(), str2.len(), "{}", filename);
}

pub fn test_jmap_mail_set<T>(mail_store: JMAPLocalStore<T>)
where
    T: for<'x> Store<'x>,
{
    // TODO use mailbox create API
    let doc_id = mail_store
        .store
        .assign_document_id(0, JMAP_MAILBOX)
        .unwrap();
    mail_store
        .store
        .update_document(
            0,
            RaftId::default(),
            WriteBatch::insert(JMAP_MAILBOX, doc_id, doc_id),
        )
        .unwrap();
    let doc_id = mail_store
        .store
        .assign_document_id(0, JMAP_MAILBOX)
        .unwrap();
    mail_store
        .store
        .update_document(
            0,
            RaftId::default(),
            WriteBatch::insert(JMAP_MAILBOX, doc_id, doc_id),
        )
        .unwrap();

    test_jmap_mail_update(&mail_store, test_jmap_mail_create(&mail_store));
}

fn test_jmap_mail_create<'x, T>(mail_store: &'x JMAPLocalStore<T>) -> Vec<String>
where
    T: Store<'x>,
{
    let mut test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_dir.push("resources");
    test_dir.push("jmap_mail_set");
    let mut message_ids = Vec::new();

    for file_name in fs::read_dir(&test_dir).unwrap() {
        let mut file_name = file_name.as_ref().unwrap().path();
        if file_name.extension().map_or(true, |e| e != "json") {
            continue;
        }

        let result = mail_store
            .mail_set(JMAPSet {
                account_id: 0,
                if_in_state: None,
                create: HashMap::from_iter(
                    vec![(
                        "1".to_string(),
                        JSONValue::from(
                            serde_json::from_slice::<SortedJSONValue>(
                                &fs::read(&file_name).unwrap(),
                            )
                            .unwrap(),
                        )
                        .unwrap_object()
                        .unwrap()
                        .into_iter()
                        .map(|(k, mut v)| {
                            store_blobs(mail_store, &mut v);
                            (k, v)
                        })
                        .collect::<HashMap<String, JSONValue>>()
                        .into(),
                    )]
                    .into_iter(),
                )
                .into(),
                update: JSONValue::Null,
                destroy: JSONValue::Null,
                arguments: (),
            })
            .unwrap();

        assert_eq!(result.not_created, JSONValue::Null);

        let mut values = result
            .created
            .unwrap_object()
            .unwrap()
            .remove("1")
            .unwrap()
            .unwrap_object()
            .unwrap();

        let raw_message = mail_store
            .download_blob(
                0,
                &BlobId::from_jmap_string(values.get("blobId").unwrap().to_string().unwrap())
                    .unwrap(),
                get_message_blob,
            )
            .unwrap()
            .unwrap();

        let jmap_id_str = values.remove("id").unwrap().unwrap_string().unwrap();
        let jmap_id = JMAPId::from_jmap_string(&jmap_id_str).unwrap();
        message_ids.push(jmap_id_str);

        let parsed_message = SortedJSONValue::from(
            mail_store
                .mail_get(JMAPGet {
                    account_id: 0,
                    ids: vec![jmap_id].into(),
                    properties: vec![
                        JMAPMailProperties::Id,
                        JMAPMailProperties::BlobId,
                        JMAPMailProperties::ThreadId,
                        JMAPMailProperties::MailboxIds,
                        JMAPMailProperties::Keywords,
                        JMAPMailProperties::ReceivedAt,
                        JMAPMailProperties::MessageId,
                        JMAPMailProperties::InReplyTo,
                        JMAPMailProperties::References,
                        JMAPMailProperties::Sender,
                        JMAPMailProperties::From,
                        JMAPMailProperties::To,
                        JMAPMailProperties::Cc,
                        JMAPMailProperties::Bcc,
                        JMAPMailProperties::ReplyTo,
                        JMAPMailProperties::Subject,
                        JMAPMailProperties::SentAt,
                        JMAPMailProperties::HasAttachment,
                        JMAPMailProperties::Preview,
                        JMAPMailProperties::BodyValues,
                        JMAPMailProperties::TextBody,
                        JMAPMailProperties::HtmlBody,
                        JMAPMailProperties::Attachments,
                        JMAPMailProperties::BodyStructure,
                    ]
                    .into(),
                    arguments: JMAPMailGetArguments {
                        body_properties: vec![
                            JMAPMailBodyProperties::PartId,
                            JMAPMailBodyProperties::BlobId,
                            JMAPMailBodyProperties::Size,
                            JMAPMailBodyProperties::Name,
                            JMAPMailBodyProperties::Type,
                            JMAPMailBodyProperties::Charset,
                            JMAPMailBodyProperties::Headers,
                            JMAPMailBodyProperties::Disposition,
                            JMAPMailBodyProperties::Cid,
                            JMAPMailBodyProperties::Language,
                            JMAPMailBodyProperties::Location,
                        ],
                        fetch_text_body_values: true,
                        fetch_html_body_values: true,
                        fetch_all_body_values: true,
                        max_body_value_bytes: 100,
                    },
                })
                .unwrap()
                .list,
        );

        file_name.set_extension("jmap");

        assert_diff(
            &replace_boundaries(serde_json::to_string_pretty(&parsed_message).unwrap()),
            &String::from_utf8(fs::read(&file_name).unwrap()).unwrap(),
            file_name.to_str().unwrap(),
        );

        /*fs::write(
            file_name.clone(),
            replace_boundaries(serde_json::to_string_pretty(&parsed_message).unwrap()),
        )
        .unwrap();*/

        file_name.set_extension("eml");

        assert_diff(
            &replace_boundaries(String::from_utf8(raw_message).unwrap()),
            &String::from_utf8(fs::read(&file_name).unwrap()).unwrap(),
            file_name.to_str().unwrap(),
        );

        /*fs::write(
            file_name,
            replace_boundaries(String::from_utf8(raw_message).unwrap()),
        )
        .unwrap();*/
    }
    assert!(!message_ids.is_empty());
    message_ids
}

fn json_to_jmap_update(entries: Vec<(String, &[u8])>) -> JSONValue {
    entries
        .into_iter()
        .map(|(jmap_id, bytes)| {
            (
                jmap_id,
                JSONValue::from(serde_json::from_slice::<SortedJSONValue>(bytes).unwrap()),
            )
        })
        .collect::<HashMap<String, JSONValue>>()
        .into()
}

fn get_mailboxes_and_keywords<'x, T>(
    mail_store: &'x JMAPLocalStore<T>,
    message_id: &str,
) -> (Vec<String>, Vec<String>)
where
    T: Store<'x>,
{
    let mut result = mail_store
        .mail_get(JMAPGet {
            account_id: 0,
            ids: vec![JMAPId::from_jmap_string(message_id).unwrap()].into(),
            properties: vec![JMAPMailProperties::MailboxIds, JMAPMailProperties::Keywords].into(),
            arguments: JMAPMailGetArguments {
                body_properties: vec![],
                fetch_text_body_values: false,
                fetch_html_body_values: false,
                fetch_all_body_values: false,
                max_body_value_bytes: 100,
            },
        })
        .unwrap()
        .list
        .unwrap_array()
        .unwrap()
        .pop()
        .unwrap()
        .unwrap_object()
        .unwrap();

    let mut mailboxes = Vec::new();
    let mut keywords = Vec::new();

    if let Some(m) = result.remove("mailboxIds").unwrap().unwrap_object() {
        for (k, v) in m {
            mailboxes.push(k.to_string());
            assert!(v.to_bool().unwrap());
        }
    }

    if let Some(m) = result.remove("keywords").unwrap().unwrap_object() {
        for (k, v) in m {
            keywords.push(k.to_string());
            assert!(v.to_bool().unwrap());
        }
    }

    mailboxes.sort_unstable();
    keywords.sort_unstable();

    (mailboxes, keywords)
}

fn test_jmap_mail_update<'x, T>(mail_store: &'x JMAPLocalStore<T>, mut message_ids: Vec<String>)
where
    T: Store<'x>,
{
    let message_id_1 = message_ids.pop().unwrap();
    let message_id_2 = message_ids.pop().unwrap();
    let message_id_3 = message_ids.pop().unwrap();

    assert_eq!(
        mail_store
            .mail_set(JMAPSet {
                account_id: 0,
                if_in_state: None,
                update: json_to_jmap_update(vec![(
                    message_id_1.clone(),
                    br#"{
                "keywords": {"test1": true, "test2": true},
                "mailboxIds": {"i0": true, "i1": true}
            }"#,
                )]),
                create: JSONValue::Null,
                destroy: JSONValue::Null,
                arguments: (),
            })
            .unwrap()
            .not_updated,
        JSONValue::Null
    );

    assert_eq!(
        get_mailboxes_and_keywords(mail_store, &message_id_1),
        (
            vec!["i00".to_string(), "i01".to_string()],
            vec!["test1".to_string(), "test2".to_string()]
        )
    );

    assert_eq!(
        mail_store
            .mail_set(JMAPSet {
                account_id: 0,
                if_in_state: None,
                update: json_to_jmap_update(vec![(
                    message_id_1.clone(),
                    br#"{
                "keywords/test1": true,
                "keywords/test3": true,
                "keywords/test2": false,
                "mailboxIds/i0": null
            }"#,
                )]),
                create: JSONValue::Null,
                destroy: JSONValue::Null,
                arguments: (),
            })
            .unwrap()
            .not_updated,
        JSONValue::Null
    );

    assert_eq!(
        get_mailboxes_and_keywords(mail_store, &message_id_1),
        (
            vec!["i01".to_string()],
            vec!["test1".to_string(), "test3".to_string()]
        )
    );

    assert_eq!(
        mail_store
            .mail_set(JMAPSet {
                account_id: 0,
                if_in_state: None,
                update: json_to_jmap_update(vec![(
                    message_id_1.clone(),
                    br#"{
                "mailboxIds/i1": null
                }"#,
                )]),
                create: JSONValue::Null,
                destroy: JSONValue::Null,
                arguments: (),
            })
            .unwrap()
            .not_updated
            .unwrap_object()
            .unwrap()
            .into_values()
            .next()
            .unwrap()
            .unwrap_object()
            .unwrap()
            .remove("description")
            .unwrap()
            .unwrap_string()
            .unwrap(),
        "Message must belong to at least one mailbox."
    );

    assert_eq!(
        mail_store
            .mail_set(JMAPSet {
                account_id: 0,
                if_in_state: None,
                update: json_to_jmap_update(vec![(
                    message_id_1.clone(),
                    br#"{
                "mailboxIds/i1": null
                }"#,
                )]),
                create: JSONValue::Null,
                destroy: vec![message_id_1.into()].into(),
                arguments: (),
            })
            .unwrap()
            .not_updated
            .unwrap_object()
            .unwrap()
            .into_values()
            .next()
            .unwrap()
            .unwrap_object()
            .unwrap()
            .remove("error_type")
            .unwrap()
            .unwrap_string()
            .unwrap(),
        "willDestroy"
    );

    assert_eq!(
        mail_store
            .mail_set(JMAPSet {
                account_id: 0,
                if_in_state: None,
                update: JSONValue::Null,
                create: JSONValue::Null,
                destroy: vec![message_id_2.clone().into(), message_id_3.clone().into()].into(),
                arguments: (),
            })
            .unwrap()
            .not_destroyed,
        JSONValue::Null
    );

    assert_eq!(
        vec![
            JMAPId::from_jmap_string(&message_id_2).unwrap(),
            JMAPId::from_jmap_string(&message_id_3).unwrap()
        ],
        mail_store
            .mail_get(JMAPGet {
                account_id: 0,
                ids: vec![
                    JMAPId::from_jmap_string(&message_id_2).unwrap(),
                    JMAPId::from_jmap_string(&message_id_3).unwrap()
                ]
                .into(),
                properties: vec![JMAPMailProperties::MailboxIds, JMAPMailProperties::Keywords]
                    .into(),
                arguments: JMAPMailGetArguments {
                    body_properties: vec![],
                    fetch_text_body_values: false,
                    fetch_html_body_values: false,
                    fetch_all_body_values: false,
                    max_body_value_bytes: 100,
                }
            },)
            .unwrap()
            .not_found
            .unwrap()
    )
}
