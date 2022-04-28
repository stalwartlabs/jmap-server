use std::{collections::HashMap, fs, iter::FromIterator, path::PathBuf};

use jmap::{
    id::JMAPIdSerialize,
    jmap_store::{blob::JMAPBlobStore, get::JMAPGet, set::JMAPSet},
    protocol::json::{JSONNumber, JSONValue},
    request::{get::GetRequest, set::SetRequest},
};
use jmap_mail::mail::{
    get::GetMail, import::JMAPMailImport, parse::get_message_blob, set::SetMail, MailBodyProperty,
    MailProperty,
};
use store::{AccountId, JMAPId, JMAPIdPrefix, JMAPStore, Store, Tag};

use crate::{
    jmap_mail_get::{build_mail_get_arguments, SortedJSONValue},
    jmap_mailbox::insert_mailbox,
};

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

fn store_blobs<T>(mail_store: &JMAPStore<T>, value: &mut JSONValue, account_id: AccountId)
where
    T: for<'x> Store<'x> + 'static,
{
    match value {
        JSONValue::Object(o) => {
            for (k, v) in o.iter_mut() {
                if k == "blobId" {
                    if let JSONValue::String(value) = v {
                        *value = mail_store
                            .upload_blob(account_id, value.as_bytes())
                            .unwrap()
                            .to_jmap_string();
                    } else {
                        panic!("blobId is not a string");
                    }
                } else {
                    store_blobs(mail_store, v, account_id);
                }
            }
        }
        JSONValue::Array(a) => {
            for v in a.iter_mut() {
                store_blobs(mail_store, v, account_id);
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

pub fn jmap_mail_set<T>(mail_store: &JMAPStore<T>, account_id: AccountId)
where
    T: for<'x> Store<'x> + 'static,
{
    let _mailbox_id_1 = insert_mailbox(mail_store, account_id, "Inbox", "INBOX");
    let _mailbox_id_2 = insert_mailbox(mail_store, account_id, "Sent", "SENT");

    jmap_mail_update(
        mail_store,
        jmap_mail_create(mail_store, account_id),
        account_id,
    );
}

fn jmap_mail_create<T>(mail_store: &JMAPStore<T>, account_id: AccountId) -> Vec<String>
where
    T: for<'x> Store<'x> + 'static,
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

        let result: JSONValue = mail_store
            .set::<SetMail>(SetRequest {
                account_id,
                if_in_state: None,
                create: Vec::from_iter(
                    vec![("m1".to_string(), {
                        let mut result: HashMap<String, JSONValue> = HashMap::new();
                        for (k, mut v) in JSONValue::from(
                            serde_json::from_slice::<SortedJSONValue>(
                                &fs::read(&file_name).unwrap(),
                            )
                            .unwrap(),
                        )
                        .unwrap_object()
                        .unwrap()
                        {
                            store_blobs(mail_store, &mut v, account_id);
                            result.insert(k, v);
                        }
                        result.into()
                    })]
                    .into_iter(),
                ),
                update: HashMap::new(),
                destroy: vec![],
                arguments: HashMap::new(),
            })
            .unwrap()
            .into();

        assert_eq!(result.eval("/notCreated").unwrap(), HashMap::new().into());

        let values = result.eval("/created/m1").unwrap();

        let raw_message = mail_store
            .download_blob(
                account_id,
                &values.eval_unwrap_blob("/blobId"),
                get_message_blob,
            )
            .unwrap()
            .unwrap();

        let jmap_id_str = values.eval_unwrap_string("/id");
        let jmap_id = JMAPId::from_jmap_string(&jmap_id_str).unwrap();
        message_ids.push(jmap_id_str);

        let parsed_message = SortedJSONValue::from(JSONValue::from(
            mail_store
                .get::<GetMail<T>>(GetRequest {
                    account_id,
                    ids: vec![jmap_id].into(),
                    properties: vec![
                        MailProperty::Id,
                        MailProperty::BlobId,
                        MailProperty::ThreadId,
                        MailProperty::MailboxIds,
                        MailProperty::Keywords,
                        MailProperty::ReceivedAt,
                        MailProperty::MessageId,
                        MailProperty::InReplyTo,
                        MailProperty::References,
                        MailProperty::Sender,
                        MailProperty::From,
                        MailProperty::To,
                        MailProperty::Cc,
                        MailProperty::Bcc,
                        MailProperty::ReplyTo,
                        MailProperty::Subject,
                        MailProperty::SentAt,
                        MailProperty::HasAttachment,
                        MailProperty::Preview,
                        MailProperty::BodyValues,
                        MailProperty::TextBody,
                        MailProperty::HtmlBody,
                        MailProperty::Attachments,
                        MailProperty::BodyStructure,
                    ]
                    .into_iter()
                    .map(|p| p.to_string().into())
                    .collect::<Vec<_>>()
                    .into(),
                    arguments: build_mail_get_arguments(
                        vec![
                            MailBodyProperty::PartId,
                            MailBodyProperty::BlobId,
                            MailBodyProperty::Size,
                            MailBodyProperty::Name,
                            MailBodyProperty::Type,
                            MailBodyProperty::Charset,
                            MailBodyProperty::Headers,
                            MailBodyProperty::Disposition,
                            MailBodyProperty::Cid,
                            MailBodyProperty::Language,
                            MailBodyProperty::Location,
                        ],
                        true,
                        true,
                        true,
                        100,
                    ),
                })
                .unwrap()
                .list,
        ));

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

fn json_to_jmap_update(entries: Vec<(String, &[u8])>) -> HashMap<String, JSONValue> {
    entries
        .into_iter()
        .map(|(jmap_id, bytes)| {
            (
                jmap_id,
                JSONValue::from(serde_json::from_slice::<SortedJSONValue>(bytes).unwrap()),
            )
        })
        .collect::<HashMap<String, JSONValue>>()
}

fn get_mailboxes_and_keywords<T>(
    mail_store: &JMAPStore<T>,
    message_id: &str,
    account_id: AccountId,
) -> (Vec<String>, Vec<String>)
where
    T: for<'x> Store<'x> + 'static,
{
    let mut result = JSONValue::from(
        mail_store
            .get::<GetMail<T>>(GetRequest {
                account_id,
                ids: vec![JMAPId::from_jmap_string(message_id).unwrap()].into(),
                properties: vec![MailProperty::MailboxIds, MailProperty::Keywords]
                    .into_iter()
                    .map(|p| p.to_string().into())
                    .collect::<Vec<_>>()
                    .into(),
                arguments: build_mail_get_arguments(vec![], false, false, false, 100),
            })
            .unwrap(),
    )
    .eval_unwrap_object("/list/0");

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

fn jmap_mail_update<T>(
    mail_store: &JMAPStore<T>,
    mut message_ids: Vec<String>,
    account_id: AccountId,
) where
    T: for<'x> Store<'x> + 'static,
{
    let message_id_1 = message_ids.pop().unwrap();
    let message_id_2 = message_ids.pop().unwrap();
    let message_id_3 = message_ids.pop().unwrap();

    assert_eq!(
        mail_store
            .set::<SetMail>(SetRequest {
                account_id,
                if_in_state: None,
                update: json_to_jmap_update(vec![(
                    message_id_1.clone(),
                    br#"{
                "keywords": {"test1": true, "test2": true},
                "mailboxIds": {"i0": true, "i1": true}
            }"#,
                )]),
                create: vec![],
                destroy: vec![],
                arguments: HashMap::new(),
            })
            .unwrap()
            .not_updated,
        HashMap::new()
    );

    assert_eq!(
        get_mailboxes_and_keywords(mail_store, &message_id_1, account_id),
        (
            vec!["i00".to_string(), "i01".to_string()],
            vec!["test1".to_string(), "test2".to_string()]
        )
    );

    assert_eq!(
        mail_store
            .set::<SetMail>(SetRequest {
                account_id,
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
                create: vec![],
                destroy: vec![],
                arguments: HashMap::new(),
            })
            .unwrap()
            .not_updated,
        HashMap::new()
    );

    assert_eq!(
        get_mailboxes_and_keywords(mail_store, &message_id_1, account_id),
        (
            vec!["i01".to_string()],
            vec!["test1".to_string(), "test3".to_string()]
        )
    );

    assert_eq!(
        JSONValue::from(
            mail_store
                .set::<SetMail>(SetRequest {
                    account_id,
                    if_in_state: None,
                    update: json_to_jmap_update(vec![(
                        message_id_1.clone(),
                        br#"{
                "mailboxIds/i1": null
                }"#,
                    )]),
                    create: vec![],
                    destroy: vec![],
                    arguments: HashMap::new(),
                })
                .unwrap()
        )
        .eval_unwrap_string(&format!("/notUpdated/{}/description", message_id_1)),
        "Message has to belong to at least one mailbox."
    );

    assert_eq!(
        JSONValue::from(
            mail_store
                .set::<SetMail>(SetRequest {
                    account_id,
                    if_in_state: None,
                    update: json_to_jmap_update(vec![(
                        message_id_1.clone(),
                        br#"{
                "mailboxIds/i1": null
                }"#,
                    )]),
                    create: vec![],
                    destroy: vec![message_id_1.clone().into()],
                    arguments: HashMap::new(),
                })
                .unwrap()
        )
        .eval_unwrap_string(&format!("/notUpdated/{}/error_type", message_id_1)),
        "willDestroy"
    );

    assert_eq!(
        mail_store
            .set::<SetMail>(SetRequest {
                account_id,
                if_in_state: None,
                update: HashMap::new(),
                create: vec![],
                destroy: vec![message_id_2.clone().into(), message_id_3.clone().into()],
                arguments: HashMap::new(),
            })
            .unwrap()
            .not_destroyed,
        HashMap::new()
    );

    assert_eq!(
        JSONValue::from(vec![
            message_id_2.clone().into(),
            message_id_3.clone().into()
        ]),
        JSONValue::from(
            mail_store
                .get::<GetMail<T>>(GetRequest {
                    account_id,
                    ids: vec![
                        JMAPId::from_jmap_string(&message_id_2).unwrap(),
                        JMAPId::from_jmap_string(&message_id_3).unwrap()
                    ]
                    .into(),
                    properties: vec![MailProperty::MailboxIds, MailProperty::Keywords]
                        .into_iter()
                        .map(|p| p.to_string().into())
                        .collect::<Vec<_>>()
                        .into(),
                    arguments: build_mail_get_arguments(vec![], false, false, false, 100,)
                },)
                .unwrap()
                .not_found
        ),
    )
}

pub fn insert_email<T>(
    mail_store: &JMAPStore<T>,
    account_id: AccountId,
    raw_message: Vec<u8>,
    mailboxes: Vec<JMAPId>,
    keywords: Vec<&str>,
    received_at: Option<i64>,
) -> JMAPId
where
    T: for<'x> Store<'x> + 'static,
{
    mail_store
        .mail_import(
            account_id,
            mail_store.blob_store(&raw_message).unwrap(),
            &raw_message,
            mailboxes.into_iter().map(|m| m.get_document_id()).collect(),
            keywords
                .into_iter()
                .map(|k| Tag::Text(k.to_string()))
                .collect(),
            received_at,
        )
        .unwrap()
        .id
}

pub fn update_email<T>(
    mail_store: &JMAPStore<T>,
    account_id: AccountId,
    jmap_id: JMAPId,
    mailboxes: Option<Vec<JMAPId>>,
    keywords: Option<Vec<String>>,
) where
    T: for<'x> Store<'x> + 'static,
{
    let mut update_values = HashMap::new();
    if let Some(mailboxes) = mailboxes {
        update_values.insert(
            "mailboxIds".to_string(),
            HashMap::from_iter(
                mailboxes
                    .into_iter()
                    .map(|m| (m.to_jmap_string(), JSONValue::Bool(true))),
            )
            .into(),
        );
    }
    if let Some(keywords) = keywords {
        update_values.insert(
            "keywords".to_string(),
            HashMap::from_iter(keywords.into_iter().map(|k| (k, JSONValue::Bool(true)))).into(),
        );
    }

    assert_eq!(
        mail_store
            .set::<SetMail>(SetRequest {
                account_id,
                if_in_state: None,
                update: HashMap::from_iter([(jmap_id.to_jmap_string(), update_values.into())]),
                create: vec![],
                destroy: vec![],
                arguments: HashMap::new(),
            })
            .unwrap()
            .not_updated,
        HashMap::new()
    );
}

pub fn delete_email<T>(mail_store: &JMAPStore<T>, account_id: AccountId, jmap_id: JMAPId)
where
    T: for<'x> Store<'x> + 'static,
{
    assert_eq!(
        mail_store
            .set::<SetMail>(SetRequest {
                account_id,
                if_in_state: None,
                update: HashMap::new(),
                create: vec![],
                destroy: vec![jmap_id.to_jmap_string().into()],
                arguments: HashMap::new(),
            })
            .unwrap()
            .not_destroyed,
        HashMap::new()
    );
}
