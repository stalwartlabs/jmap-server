use jmap_mail::{
    import::JMAPMailImportItem, JMAPMailBodyProperties, JMAPMailLocalStore, JMAPMailProperties,
    JMAPMailStoreGetArguments,
};
use jmap_store::{json::JSONValue, JMAPGet};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, path::PathBuf};
use store::Tag;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(untagged)]
pub enum UntaggedJSONValue {
    Null,
    Bool(bool),
    String(String),
    Number(i64),
    Array(Vec<UntaggedJSONValue>),
    Object(HashMap<String, UntaggedJSONValue>),
}

impl<'x> From<JSONValue> for UntaggedJSONValue {
    fn from(value: JSONValue) -> Self {
        match value {
            JSONValue::Null => UntaggedJSONValue::Null,
            JSONValue::Bool(value) => UntaggedJSONValue::Bool(value),
            JSONValue::String(string) => UntaggedJSONValue::String(string),
            JSONValue::Number(value) => UntaggedJSONValue::Number(value),
            JSONValue::Array(list) => {
                UntaggedJSONValue::Array(list.into_iter().map(|value| value.into()).collect())
            }
            JSONValue::Object(map) => UntaggedJSONValue::Object(
                map.into_iter()
                    .map(|(key, value)| (key, value.into()))
                    .collect(),
            ),
        }
    }
}

pub fn test_jmap_mail_get<T>(mail_store: T)
where
    T: for<'x> JMAPMailLocalStore<'x>,
{
    let mut test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_dir.push("resources");
    test_dir.push("messages");

    for file_name in fs::read_dir(&test_dir).unwrap() {
        let mut file_name = file_name.as_ref().unwrap().path();
        if file_name.extension().map_or(true, |e| e != "eml") {
            continue;
        }
        let jmap_id = mail_store
            .mail_import_single(
                0,
                JMAPMailImportItem {
                    blob: fs::read(&file_name).unwrap().into(),
                    mailbox_ids: vec![0, 1, 2],
                    keywords: vec![Tag::Text("tag".into())],
                    received_at: None,
                },
            )
            .unwrap();
        let result = mail_store
            .mail_get(
                JMAPGet {
                    account_id: 0,
                    ids: vec![jmap_id].into(),
                    properties: vec![
                        JMAPMailProperties::Id,
                        JMAPMailProperties::BlobId,
                        JMAPMailProperties::ThreadId,
                        JMAPMailProperties::MailboxIds,
                        JMAPMailProperties::Keywords,
                        JMAPMailProperties::Size,
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
                },
                JMAPMailStoreGetArguments {
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
            )
            .unwrap();
        //let output = serde_yaml::to_string(&UntaggedJSONValue::from(result.list)).unwrap();
        let output = serde_json::to_string_pretty(&UntaggedJSONValue::from(result.list)).unwrap();
        file_name.set_extension("json");
        fs::write(file_name, &output).unwrap();
    }
}
