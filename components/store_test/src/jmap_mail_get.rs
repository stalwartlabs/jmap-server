use jmap_mail::{
    get::{JMAPMailGet, JMAPMailGetArguments},
    import::JMAPMailLocalStoreImport,
    HeaderName, JMAPMailBodyProperties, JMAPMailHeaderForm, JMAPMailHeaderProperty,
    JMAPMailProperties,
};
use jmap_store::{json::JSONValue, JMAPGet};
use mail_parser::RfcHeader;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::PathBuf,
};
use store::{raft::RaftId, JMAPStore, Store, Tag};

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

pub fn jmap_mail_get<T>(mail_store: JMAPStore<T>)
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
        let jmap_id = mail_store
            .mail_import_blob(
                0,
                RaftId::default(),
                &blob,
                vec![],
                vec![Tag::Text("tag".into())],
                Some((blob.len() * 1000000) as i64),
            )
            .unwrap()
            .unwrap_object()
            .unwrap()
            .get("id")
            .unwrap()
            .to_jmap_id()
            .unwrap();

        let result = if file_name.file_name().unwrap() != "headers.eml" {
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
                .list
        } else {
            let mut properties = vec![
                JMAPMailProperties::Id,
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
                JMAPMailProperties::Preview,
                JMAPMailProperties::TextBody,
                JMAPMailProperties::HtmlBody,
                JMAPMailProperties::Attachments,
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
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Raw,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Raw,
                    header: header.clone(),
                    all: false,
                }));
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Addresses,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Addresses,
                    header: header.clone(),
                    all: false,
                }));
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::GroupedAddresses,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::GroupedAddresses,
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
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Raw,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Raw,
                    header: header.clone(),
                    all: false,
                }));
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::URLs,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::URLs,
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
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Raw,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Raw,
                    header: header.clone(),
                    all: false,
                }));
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Date,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Date,
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
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Raw,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Raw,
                    header: header.clone(),
                    all: false,
                }));
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::MessageIds,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::MessageIds,
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
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Raw,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Raw,
                    header: header.clone(),
                    all: false,
                }));
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Text,
                    header: header.clone(),
                    all: true,
                }));
                properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Text,
                    header: header.clone(),
                    all: false,
                }));
            }

            let mut result = HashMap::new();
            for property in properties {
                result.extend(
                    mail_store
                        .mail_get(JMAPGet {
                            account_id: 0,
                            ids: vec![jmap_id].into(),
                            properties: vec![property].into(),
                            arguments: JMAPMailGetArguments {
                                body_properties: vec![
                                    JMAPMailBodyProperties::Size,
                                    JMAPMailBodyProperties::Name,
                                    JMAPMailBodyProperties::Type,
                                    JMAPMailBodyProperties::Charset,
                                    JMAPMailBodyProperties::Disposition,
                                    JMAPMailBodyProperties::Cid,
                                    JMAPMailBodyProperties::Language,
                                    JMAPMailBodyProperties::Location,
                                    JMAPMailBodyProperties::Header(
                                        JMAPMailHeaderProperty::new_other(
                                            "X-Custom-Header".into(),
                                            JMAPMailHeaderForm::Raw,
                                            false,
                                        ),
                                    ),
                                    JMAPMailBodyProperties::Header(
                                        JMAPMailHeaderProperty::new_other(
                                            "X-Custom-Header-2".into(),
                                            JMAPMailHeaderForm::Raw,
                                            false,
                                        ),
                                    ),
                                ],
                                fetch_text_body_values: true,
                                fetch_html_body_values: true,
                                fetch_all_body_values: true,
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
                        .unwrap(),
                );
            }
            JSONValue::Array(vec![JSONValue::Object(result)])
        };

        file_name.set_extension("json");

        //fs::write(file_name, &serde_json::to_string_pretty(&SortedJSONValue::from(result)).unwrap()).unwrap();
        let result = SortedJSONValue::from(result);

        assert_eq!(
            &result,
            &serde_json::from_slice::<SortedJSONValue>(&fs::read(&file_name).unwrap()).unwrap(),
            "{}",
            serde_json::to_string_pretty(&result).unwrap()
        );
    }
}
