use jmap::{jmap_store::get::JMAPGet, protocol::json::JSONValue, request::get::GetRequest};
use jmap_mail::{
    mail::{
        get::GetMail, HeaderName, MailBodyProperty, MailHeaderForm, MailHeaderProperty,
        MailProperty,
    },
    mail_parser::RfcHeader,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    iter::FromIterator,
    path::PathBuf,
};
use store::{AccountId, JMAPStore, Store};

use crate::tests::store::jmap_mail_set::{delete_email, insert_email};

use super::utils::StoreCompareWith;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(untagged)]
pub enum SortedJSONValue {
    Null,
    Bool(bool),
    String(String),
    Number(u64),
    Array(Vec<SortedJSONValue>),
    Object(BTreeMap<String, SortedJSONValue>),
}

impl<'x> From<JSONValue> for SortedJSONValue {
    fn from(value: JSONValue) -> Self {
        match value {
            JSONValue::Null => SortedJSONValue::Null,
            JSONValue::Bool(value) => SortedJSONValue::Bool(value),
            JSONValue::String(string) => SortedJSONValue::String(string),
            JSONValue::Number(value) => SortedJSONValue::Number(value.to_unsigned_int()),
            JSONValue::Array(mut list) => {
                match list.first() {
                    Some(JSONValue::Object(map))
                        if map.get("name").is_some() && map.get("value").is_some() =>
                    {
                        list.sort_unstable_by_key(|value| match value {
                            JSONValue::Object(map) => match (map.get("name"), map.get("value")) {
                                (Some(JSONValue::String(name)), Some(JSONValue::String(value))) => {
                                    (name.clone(), value.clone())
                                }
                                (Some(JSONValue::String(name)), Some(JSONValue::Null)) => {
                                    (name.clone(), "".to_string())
                                }
                                _ => {
                                    unreachable!()
                                }
                            },
                            _ => unreachable!(),
                        });
                    }
                    _ => (),
                }
                SortedJSONValue::Array(list.into_iter().map(|value| value.into()).collect())
            }
            JSONValue::Object(map) => SortedJSONValue::Object(
                map.into_iter()
                    .map(|(key, value)| {
                        if key == "blobId" || key == "id" || key == "threadId" {
                            (key, SortedJSONValue::String("ignored_value".into()))
                        } else {
                            (key, value.into())
                        }
                    })
                    .collect(),
            ),
        }
    }
}

pub fn build_mail_get_arguments(
    properties: Vec<MailBodyProperty>,
    fetch_text: bool,
    fetch_html: bool,
    fetch_all: bool,
    max_bytes: u64,
) -> HashMap<String, JSONValue> {
    HashMap::from_iter([
        (
            "bodyProperties".to_string(),
            properties
                .into_iter()
                .map(|p| p.to_string().into())
                .collect::<Vec<JSONValue>>()
                .into(),
        ),
        ("fetchTextBodyValues".to_string(), fetch_text.into()),
        ("fetchHtmlBodyValues".to_string(), fetch_html.into()),
        ("fetchAllBodyValues".to_string(), fetch_all.into()),
        ("maxBodyValueBytes".to_string(), max_bytes.into()),
    ])
}

/*

pub fn jmap_mail_get<T>(mail_store: &JMAPStore<T>, account_id: AccountId)
where
    T: for<'x> Store<'x> + 'static,
{
    let result = if file_name.file_name().unwrap() != "headers.eml" {
        JSONValue::from(
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
                        MailProperty::Size,
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
                .unwrap(),
        )
        .eval("/list")
        .unwrap()
    } else {
        let mut result = HashMap::new();
        for property in properties {
            result.extend(
                JSONValue::from(
                    mail_store
                        .get::<GetMail<T>>(GetRequest {
                            account_id,
                            ids: vec![jmap_id].into(),
                            properties: vec![property.to_string().into()].into(),
                            arguments: build_mail_get_arguments(
                                vec![
                                    MailBodyProperty::Size,
                                    MailBodyProperty::Name,
                                    MailBodyProperty::Type,
                                    MailBodyProperty::Charset,
                                    MailBodyProperty::Disposition,
                                    MailBodyProperty::Cid,
                                    MailBodyProperty::Language,
                                    MailBodyProperty::Location,
                                    MailBodyProperty::Header(MailHeaderProperty::new_other(
                                        "X-Custom-Header".into(),
                                        MailHeaderForm::Raw,
                                        false,
                                    )),
                                    MailBodyProperty::Header(MailHeaderProperty::new_other(
                                        "X-Custom-Header-2".into(),
                                        MailHeaderForm::Raw,
                                        false,
                                    )),
                                ],
                                true,
                                true,
                                true,
                                100,
                            ),
                        })
                        .unwrap(),
                )
                .eval_unwrap_object("/list/0"),
            );
        }
        JSONValue::Array(vec![JSONValue::Object(result)])
    };
}
*/
