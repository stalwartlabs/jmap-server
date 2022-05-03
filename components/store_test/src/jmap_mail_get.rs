use jmap::{jmap_store::get::JMAPGet, protocol::json::JSONValue, request::get::GetRequest};
use jmap_mail::mail::{
    get::GetMail, HeaderName, MailBodyProperty, MailHeaderForm, MailHeaderProperty, MailProperty,
};
use mail_parser::RfcHeader;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    iter::FromIterator,
    path::PathBuf,
};
use store::{AccountId, JMAPStore, Store};

use crate::{
    jmap_mail_set::{delete_email, insert_email},
    StoreCompareWith,
};

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

pub fn jmap_mail_get<T>(mail_store: &JMAPStore<T>, account_id: AccountId)
where
    T: for<'x> Store<'x> + 'static,
{
    let mut test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_dir.push("resources");
    test_dir.push("jmap_mail_get");

    for file_name in fs::read_dir(&test_dir).unwrap() {
        let mut file_name = file_name.as_ref().unwrap().path();
        if file_name.extension().map_or(true, |e| e != "eml") {
            continue;
        }
        let blob = fs::read(&file_name).unwrap();
        let blob_len = blob.len();
        let jmap_id = insert_email(
            mail_store,
            account_id,
            blob,
            vec![],
            vec!["tag"],
            Some((blob_len * 1000000) as u64),
        );

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
            let mut properties = vec![
                MailProperty::Id,
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
                MailProperty::Preview,
                MailProperty::TextBody,
                MailProperty::HtmlBody,
                MailProperty::Attachments,
            ];

            for header in [
                HeaderName::Rfc(RfcHeader::From),
                HeaderName::Rfc(RfcHeader::To),
                HeaderName::Rfc(RfcHeader::Cc),
                HeaderName::Rfc(RfcHeader::Bcc),
                HeaderName::Other("X-Address-Single".into()),
                HeaderName::Other("X-Address".into()),
                HeaderName::Other("X-AddressList-Single".into()),
                HeaderName::Other("X-AddressList".into()),
                HeaderName::Other("X-AddressesGroup-Single".into()),
                HeaderName::Other("X-AddressesGroup".into()),
            ] {
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::Raw,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::Raw,
                    header: header.clone(),
                    all: false,
                }));
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::Addresses,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::Addresses,
                    header: header.clone(),
                    all: false,
                }));
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::GroupedAddresses,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::GroupedAddresses,
                    header: header.clone(),
                    all: false,
                }));
            }

            for header in [
                HeaderName::Rfc(RfcHeader::ListPost),
                HeaderName::Rfc(RfcHeader::ListSubscribe),
                HeaderName::Rfc(RfcHeader::ListUnsubscribe),
                HeaderName::Rfc(RfcHeader::ListOwner),
                HeaderName::Other("X-List-Single".into()),
                HeaderName::Other("X-List".into()),
            ] {
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::Raw,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::Raw,
                    header: header.clone(),
                    all: false,
                }));
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::URLs,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::URLs,
                    header: header.clone(),
                    all: false,
                }));
            }

            for header in [
                HeaderName::Rfc(RfcHeader::Date),
                HeaderName::Rfc(RfcHeader::ResentDate),
                HeaderName::Other("X-Date-Single".into()),
                HeaderName::Other("X-Date".into()),
            ] {
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::Raw,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::Raw,
                    header: header.clone(),
                    all: false,
                }));
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::Date,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::Date,
                    header: header.clone(),
                    all: false,
                }));
            }

            for header in [
                HeaderName::Rfc(RfcHeader::MessageId),
                HeaderName::Rfc(RfcHeader::References),
                HeaderName::Other("X-Id-Single".into()),
                HeaderName::Other("X-Id".into()),
            ] {
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::Raw,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::Raw,
                    header: header.clone(),
                    all: false,
                }));
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::MessageIds,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::MessageIds,
                    header: header.clone(),
                    all: false,
                }));
            }

            for header in [
                HeaderName::Rfc(RfcHeader::Subject),
                HeaderName::Rfc(RfcHeader::Keywords),
                HeaderName::Other("X-Text-Single".into()),
                HeaderName::Other("X-Text".into()),
            ] {
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::Raw,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::Raw,
                    header: header.clone(),
                    all: false,
                }));
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::Text,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(MailProperty::Header(MailHeaderProperty {
                    form: MailHeaderForm::Text,
                    header: header.clone(),
                    all: false,
                }));
            }

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

        file_name.set_extension("json");

        let result = SortedJSONValue::from(result);
        //fs::write(&file_name, &serde_json::to_string_pretty(&result).unwrap()).unwrap();

        assert_eq!(
            &result,
            &serde_json::from_slice::<SortedJSONValue>(&fs::read(&file_name).unwrap()).unwrap(),
            "{} ({})",
            serde_json::to_string_pretty(&result).unwrap(),
            file_name.to_str().unwrap()
        );

        delete_email(mail_store, account_id, jmap_id);
    }

    mail_store.assert_is_empty();
}
