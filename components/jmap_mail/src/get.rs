use std::collections::HashMap;

use chrono::{LocalResult, SecondsFormat, TimeZone, Utc};
use jmap_store::{
    id::{BlobId, JMAPIdSerialize},
    json::JSONValue,
    JMAPError, JMAPGet, JMAPGetResponse, JMAPId, JMAP_MAIL,
};
use mail_parser::{
    parsers::{
        fields::{
            address::parse_address, date::parse_date, id::parse_id,
            unstructured::parse_unstructured,
        },
        message::MessageStream,
    },
    HeaderName, HeaderOffsetName, HeaderValue, MessageStructure,
};
use store::{BlobEntry, DocumentId, Store, Tag};

use crate::{
    changes::JMAPMailLocalStoreChanges,
    import::bincode_deserialize,
    parse::{
        header_to_jmap_address, header_to_jmap_date, header_to_jmap_id, header_to_jmap_text,
        header_to_jmap_url,
    },
    query::MailboxId,
    JMAPMailBodyProperties, JMAPMailHeaderForm, JMAPMailHeaderProperty, JMAPMailHeaders,
    JMAPMailIdImpl, JMAPMailProperties, JMAPMailStoreGetArguments, MessageBody, MessageField,
    MessageRawHeaders, MESSAGE_BODY, MESSAGE_BODY_STRUCTURE, MESSAGE_HEADERS, MESSAGE_HEADERS_RAW,
    MESSAGE_PARTS, MESSAGE_RAW,
};

pub trait JMAPMailLocalStoreGet<'x>: JMAPMailLocalStoreChanges<'x> + Store<'x> {
    fn mail_get(
        &self,
        request: JMAPGet<JMAPMailProperties<'x>>,
        mut arguments: JMAPMailStoreGetArguments,
    ) -> jmap_store::Result<jmap_store::JMAPGetResponse<'x, JMAPMailProperties<'x>>> {
        let mut blob_indexes = [false; MESSAGE_PARTS];

        let properties = request.properties.unwrap_or_else(|| {
            vec![
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
            ]
        });

        if arguments.body_properties.is_empty() {
            arguments.body_properties = vec![
                JMAPMailBodyProperties::PartId,
                JMAPMailBodyProperties::BlobId,
                JMAPMailBodyProperties::Size,
                JMAPMailBodyProperties::Name,
                JMAPMailBodyProperties::Type,
                JMAPMailBodyProperties::Charset,
                JMAPMailBodyProperties::Disposition,
                JMAPMailBodyProperties::Cid,
                JMAPMailBodyProperties::Language,
                JMAPMailBodyProperties::Location,
            ];
        }

        for property in &properties {
            match property {
                JMAPMailProperties::BodyStructure => {
                    blob_indexes[MESSAGE_BODY_STRUCTURE] = true;
                    blob_indexes[MESSAGE_BODY] = true;
                }

                JMAPMailProperties::HasAttachment
                | JMAPMailProperties::Attachments
                | JMAPMailProperties::Preview
                | JMAPMailProperties::BodyValues
                | JMAPMailProperties::TextBody
                | JMAPMailProperties::HtmlBody
                | JMAPMailProperties::Size
                | JMAPMailProperties::ReceivedAt => {
                    blob_indexes[MESSAGE_BODY] = true;
                }

                JMAPMailProperties::RfcHeader(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Raw,
                    ..
                })
                | JMAPMailProperties::OtherHeader(_) => {
                    blob_indexes[MESSAGE_HEADERS_RAW] = true;
                    blob_indexes[MESSAGE_RAW] = true;
                }

                JMAPMailProperties::MessageId
                | JMAPMailProperties::InReplyTo
                | JMAPMailProperties::References
                | JMAPMailProperties::Sender
                | JMAPMailProperties::From
                | JMAPMailProperties::To
                | JMAPMailProperties::Cc
                | JMAPMailProperties::Bcc
                | JMAPMailProperties::ReplyTo
                | JMAPMailProperties::Subject
                | JMAPMailProperties::SentAt
                | JMAPMailProperties::RfcHeader(_) => {
                    blob_indexes[MESSAGE_HEADERS] = true;
                }

                // Ignore sub-properties
                _ => (),
            }
        }

        let request_ids = if let Some(request_ids) = request.ids {
            if request_ids.len() > self.get_config().jmap_mail_options.get_max_results {
                return Err(JMAPError::RequestTooLarge);
            } else {
                request_ids
            }
        } else {
            let document_ids = self
                .get_document_ids(request.account_id, JMAP_MAIL)?
                .into_iter()
                .take(self.get_config().jmap_mail_options.get_max_results)
                .collect::<Vec<DocumentId>>();
            if !document_ids.is_empty() {
                self.get_multi_document_value(
                    request.account_id,
                    JMAP_MAIL,
                    document_ids.iter().copied(),
                    MessageField::ThreadId.into(),
                )?
                .into_iter()
                .zip(document_ids)
                .filter_map(|(thread_id, document_id)| {
                    JMAPId::from_email(thread_id?, document_id).into()
                })
                .collect::<Vec<u64>>()
            } else {
                Vec::new()
            }
        };

        let mut not_found = Vec::new();
        let mut results = Vec::with_capacity(request_ids.len());

        for jmap_id in request_ids {
            let document_id = jmap_id.get_document_id();
            let mut message_body = None;
            let mut message_body_structure = None;
            let mut message_headers = None;
            let mut message_headers_raw = None;
            let mut message_raw = None;

            let entries = self.get_document_blob_entries(
                request.account_id,
                JMAP_MAIL,
                document_id,
                (MESSAGE_RAW..MESSAGE_PARTS)
                    .into_iter()
                    .filter_map(|index| {
                        if blob_indexes[index] {
                            Some(if index != MESSAGE_RAW {
                                BlobEntry::new(index)
                            } else {
                                BlobEntry::new_range(index, 0..512)
                            })
                        } else {
                            None
                        }
                    }),
            )?;

            if entries.is_empty() {
                not_found.push(jmap_id);
                continue;
            }

            for entry in entries {
                match entry.index {
                    MESSAGE_BODY => {
                        message_body = Some(bincode_deserialize::<MessageBody>(&entry.value)?);
                    }
                    MESSAGE_BODY_STRUCTURE => {
                        message_body_structure =
                            Some(bincode_deserialize::<MessageStructure>(&entry.value)?);
                    }
                    MESSAGE_HEADERS => {
                        message_headers =
                            Some(bincode_deserialize::<JMAPMailHeaders>(&entry.value)?);
                    }
                    MESSAGE_HEADERS_RAW => {
                        let raw_headers = bincode_deserialize::<MessageRawHeaders>(&entry.value)?;
                        if raw_headers.size > 512 {
                            message_raw = Some(
                                self.get_document_blob_entry(
                                    request.account_id,
                                    JMAP_MAIL,
                                    document_id,
                                    BlobEntry::new_range(MESSAGE_RAW, 0..raw_headers.size),
                                )?
                                .value,
                            );
                        }
                        message_headers_raw = Some(raw_headers);
                    }
                    MESSAGE_RAW => {
                        message_raw = Some(entry.value);
                    }
                    _ => (),
                }
            }

            let mut result = HashMap::new();

            for property in &properties {
                if result.contains_key(property) {
                    continue;
                }

                result.insert(
                    property.into_owned(),
                    match property {
                        JMAPMailProperties::RfcHeader(JMAPMailHeaderProperty {
                            form: JMAPMailHeaderForm::Raw,
                            header,
                            all,
                        }) => add_raw_header(
                            message_headers_raw.as_mut().unwrap(),
                            message_raw.as_ref().unwrap(),
                            HeaderOffsetName::Rfc(*header),
                            JMAPMailHeaderForm::Raw,
                            *all,
                        ),
                        JMAPMailProperties::OtherHeader(JMAPMailHeaderProperty {
                            form,
                            header,
                            all,
                        }) => add_raw_header(
                            message_headers_raw.as_mut().unwrap(),
                            message_raw.as_ref().unwrap(),
                            HeaderOffsetName::Other(header.as_ref().into()),
                            form.clone(),
                            *all,
                        ),
                        JMAPMailProperties::MessageId => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            HeaderName::MessageId,
                            JMAPMailHeaderForm::MessageIds,
                            false,
                        )?,
                        JMAPMailProperties::InReplyTo => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            HeaderName::InReplyTo,
                            JMAPMailHeaderForm::MessageIds,
                            false,
                        )?,
                        JMAPMailProperties::References => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            HeaderName::References,
                            JMAPMailHeaderForm::MessageIds,
                            false,
                        )?,
                        JMAPMailProperties::Sender => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            HeaderName::Sender,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?,
                        JMAPMailProperties::From => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            HeaderName::From,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?,
                        JMAPMailProperties::To => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            HeaderName::To,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?,
                        JMAPMailProperties::Cc => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            HeaderName::Cc,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?,
                        JMAPMailProperties::Bcc => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            HeaderName::Bcc,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?,
                        JMAPMailProperties::ReplyTo => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            HeaderName::ReplyTo,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?,
                        JMAPMailProperties::Subject => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            HeaderName::Subject,
                            JMAPMailHeaderForm::Text,
                            false,
                        )?,
                        JMAPMailProperties::SentAt => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            HeaderName::Date,
                            JMAPMailHeaderForm::Date,
                            false,
                        )?,
                        JMAPMailProperties::RfcHeader(JMAPMailHeaderProperty {
                            form,
                            header,
                            all,
                        }) => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            *header,
                            form.clone(),
                            *all,
                        )?,

                        JMAPMailProperties::Id => {
                            JSONValue::String(jmap_id.to_jmap_string().into())
                        }
                        JMAPMailProperties::BlobId => JSONValue::String(
                            BlobId {
                                account: request.account_id,
                                collection: JMAP_MAIL,
                                document: document_id,
                                blob_index: MESSAGE_RAW,
                            }
                            .to_jmap_string()
                            .into(),
                        ),
                        JMAPMailProperties::ThreadId => JSONValue::String(
                            (jmap_id.get_thread_id() as JMAPId).to_jmap_string().into(),
                        ),
                        JMAPMailProperties::MailboxIds => {
                            if let Some(mailboxes) = self.get_document_value::<Vec<u8>>(
                                request.account_id,
                                JMAP_MAIL,
                                document_id,
                                MessageField::Mailbox.into(),
                            )? {
                                JSONValue::Array(
                                    bincode_deserialize::<Vec<MailboxId>>(&mailboxes)?
                                        .into_iter()
                                        .map(|mailbox_id| {
                                            JSONValue::String(
                                                (mailbox_id as JMAPId).to_jmap_string().into(),
                                            )
                                        })
                                        .collect(),
                                )
                            } else {
                                JSONValue::Null
                            }
                        }
                        JMAPMailProperties::Keywords => {
                            if let Some(tags) = self.get_document_value::<Vec<u8>>(
                                request.account_id,
                                JMAP_MAIL,
                                document_id,
                                MessageField::Keyword.into(),
                            )? {
                                JSONValue::Array(
                                    bincode_deserialize::<Vec<Tag>>(&tags)?
                                        .into_iter()
                                        .map(|tag| {
                                            JSONValue::String(match tag {
                                                Tag::Static(_) => "todo!()".to_string().into(), //TODO map static keywords
                                                Tag::Id(_) => "todo!()".to_string().into(),
                                                Tag::Text(text) => text,
                                            })
                                        })
                                        .collect(),
                                )
                            } else {
                                JSONValue::Null
                            }
                        }
                        JMAPMailProperties::Size => {
                            JSONValue::Number(message_body.as_ref().unwrap().size as i64)
                        }
                        JMAPMailProperties::ReceivedAt => {
                            if let LocalResult::Single(received_at) =
                                Utc.timestamp_opt(message_body.as_ref().unwrap().received_at, 0)
                            {
                                JSONValue::String(
                                    received_at
                                        .to_rfc3339_opts(SecondsFormat::Secs, true)
                                        .into(),
                                )
                            } else {
                                JSONValue::Null
                            }
                        }
                        JMAPMailProperties::HasAttachment => {
                            JSONValue::Bool(message_body.as_ref().unwrap().has_attachments)
                        }
                        JMAPMailProperties::Attachments
                        | JMAPMailProperties::BodyValues
                        | JMAPMailProperties::TextBody
                        | JMAPMailProperties::HtmlBody => JSONValue::Null,

                        JMAPMailProperties::Preview => JSONValue::Null,

                        JMAPMailProperties::BodyStructure => JSONValue::Null,

                        // Ignore internal properties
                        JMAPMailProperties::Name
                        | JMAPMailProperties::Email
                        | JMAPMailProperties::Addresses
                        | JMAPMailProperties::PartId
                        | JMAPMailProperties::Type
                        | JMAPMailProperties::Charset
                        | JMAPMailProperties::Headers
                        | JMAPMailProperties::Disposition
                        | JMAPMailProperties::Cid
                        | JMAPMailProperties::Language
                        | JMAPMailProperties::Location
                        | JMAPMailProperties::Subparts => continue,
                    },
                );
            }

            results.push(JSONValue::Properties(result));
        }

        Ok(JMAPGetResponse {
            state: self.get_state(request.account_id, JMAP_MAIL)?,
            list: if !results.is_empty() {
                JSONValue::Array(results)
            } else {
                JSONValue::Null
            },
            not_found: if not_found.is_empty() {
                None
            } else {
                not_found.into()
            },
        })
    }
}

fn add_rfc_header<'x>(
    message_headers: &mut JMAPMailHeaders<'x>,
    header: HeaderName,
    form: JMAPMailHeaderForm,
    all: bool,
) -> jmap_store::Result<JSONValue<'x, JMAPMailProperties<'x>>> {
    Ok(match (header, form.clone()) {
        (HeaderName::Date | HeaderName::ResentDate, JMAPMailHeaderForm::Date)
        | (
            HeaderName::Subject | HeaderName::Comments | HeaderName::Keywords | HeaderName::ListId,
            JMAPMailHeaderForm::Text,
        ) => transform_json_string(message_headers.remove(&header).unwrap_or_default(), all),
        (
            HeaderName::MessageId
            | HeaderName::References
            | HeaderName::ResentMessageId
            | HeaderName::InReplyTo,
            JMAPMailHeaderForm::MessageIds,
        )
        | (
            HeaderName::ListArchive
            | HeaderName::ListHelp
            | HeaderName::ListOwner
            | HeaderName::ListPost
            | HeaderName::ListSubscribe
            | HeaderName::ListUnsubscribe,
            JMAPMailHeaderForm::URLs,
        ) => transform_json_stringlist(message_headers.remove(&header).unwrap_or_default(), all),
        (
            HeaderName::From
            | HeaderName::To
            | HeaderName::Cc
            | HeaderName::Bcc
            | HeaderName::ReplyTo
            | HeaderName::Sender
            | HeaderName::ResentTo
            | HeaderName::ResentFrom
            | HeaderName::ResentBcc
            | HeaderName::ResentCc
            | HeaderName::ResentSender,
            JMAPMailHeaderForm::Addresses | JMAPMailHeaderForm::GroupedAddresses,
        ) => transform_json_emailaddress(
            message_headers.remove(&header).unwrap_or_default(),
            matches!(form, JMAPMailHeaderForm::GroupedAddresses),
            all,
        ),
        _ => return Err(JMAPError::InvalidArguments),
    })
}

fn add_raw_header<'x, 'y>(
    message_headers_raw: &mut MessageRawHeaders<'y>,
    message_raw: &[u8],
    header_name: HeaderOffsetName<'y>,
    form: JMAPMailHeaderForm,
    all: bool,
) -> JSONValue<'x, JMAPMailProperties<'x>> {
    if let Some(offsets) = message_headers_raw.headers.remove(&header_name) {
        let mut header_values: Vec<HeaderValue> = offsets
            .iter()
            .skip(if !all && offsets.len() > 1 {
                offsets.len() - 1
            } else {
                0
            })
            .map(|offset| {
                (message_raw
                    .get(offset.start..offset.end)
                    .map_or(HeaderValue::Empty, |bytes| match form {
                        JMAPMailHeaderForm::Raw => {
                            HeaderValue::Text(std::str::from_utf8(bytes).map_or_else(
                                |_| String::from_utf8_lossy(bytes).trim().to_string().into(),
                                |str| str.trim().to_string().into(),
                            ))
                        }
                        JMAPMailHeaderForm::Text => {
                            parse_unstructured(&mut MessageStream::new(bytes))
                        }
                        JMAPMailHeaderForm::Addresses => {
                            parse_address(&mut MessageStream::new(bytes))
                        }
                        JMAPMailHeaderForm::GroupedAddresses => {
                            parse_address(&mut MessageStream::new(bytes))
                        }
                        JMAPMailHeaderForm::MessageIds => parse_id(&mut MessageStream::new(bytes)),
                        JMAPMailHeaderForm::Date => parse_date(&mut MessageStream::new(bytes)),
                        JMAPMailHeaderForm::URLs => parse_address(&mut MessageStream::new(bytes)),
                    }))
                .into_owned()
            })
            .collect();
        let header_values = if all {
            HeaderValue::Collection(header_values)
        } else {
            header_values.pop().unwrap_or_default()
        };
        match form {
            JMAPMailHeaderForm::Raw | JMAPMailHeaderForm::Text => {
                header_to_jmap_text(header_values)
            }
            JMAPMailHeaderForm::Addresses => transform_json_emailaddress(
                header_to_jmap_address(header_values, false),
                false,
                all,
            ),
            JMAPMailHeaderForm::GroupedAddresses => {
                transform_json_emailaddress(header_to_jmap_address(header_values, false), true, all)
            }
            JMAPMailHeaderForm::MessageIds => header_to_jmap_id(header_values),
            JMAPMailHeaderForm::Date => header_to_jmap_date(header_values),
            JMAPMailHeaderForm::URLs => header_to_jmap_url(header_values),
        }
    } else {
        JSONValue::Null
    }
}

pub fn transform_json_emailaddress<'x>(
    value: JSONValue<'x, JMAPMailProperties<'x>>,
    as_grouped: bool,
    as_collection: bool,
) -> JSONValue<'x, JMAPMailProperties<'x>> {
    if let JSONValue::Array(mut list) = value {
        let (is_collection, is_grouped) = match list.get(0) {
            Some(JSONValue::Array(list)) => (
                true,
                matches!(list.get(0), Some(JSONValue::Properties(obj)) if obj.contains_key(&JMAPMailProperties::Addresses)),
            ),
            Some(JSONValue::Properties(obj)) => {
                (false, obj.contains_key(&JMAPMailProperties::Addresses))
            }
            _ => (false, false),
        };

        if ((as_grouped && is_grouped) || (!as_grouped && !is_grouped))
            && ((is_collection && as_collection) || (!is_collection && !as_collection))
        {
            JSONValue::Array(list)
        } else if (as_grouped && is_grouped) || (!as_grouped && !is_grouped) {
            if as_collection && !is_collection {
                JSONValue::Array(vec![JSONValue::Array(list)])
            } else {
                // !as_collection && is_collection
                list.pop().unwrap_or_default()
            }
        } else {
            let mut list = if as_collection && !is_collection {
                vec![JSONValue::Array(list)]
            } else if !as_collection && is_collection {
                if let JSONValue::Array(list) = list.pop().unwrap_or_default() {
                    list
                } else {
                    vec![]
                }
            } else {
                list
            };

            if as_grouped && !is_grouped {
                let list_to_group = |list: Vec<JSONValue<'x, JMAPMailProperties<'x>>>| -> JSONValue<'x, JMAPMailProperties<'x>> {
                let mut group = HashMap::new();
                group.insert(JMAPMailProperties::Name, JSONValue::Null);
                group.insert(JMAPMailProperties::Addresses, JSONValue::Array(list));
                JSONValue::Properties(group)
            };
                JSONValue::Array(if !as_collection {
                    vec![list_to_group(list)]
                } else {
                    list.iter_mut().for_each(|field| {
                        if let JSONValue::Array(list) = field {
                            *field = JSONValue::Array(vec![list_to_group(std::mem::take(list))]);
                        }
                    });
                    list
                })
            } else {
                // !as_grouped && is_grouped
                let flatten_group = |list: Vec<JSONValue<'x, JMAPMailProperties<'x>>>| -> Vec<JSONValue<'x, JMAPMailProperties<'x>>> {
                let mut addresses = Vec::with_capacity(list.len() * 2);
                list.into_iter().for_each(|group| {
                    if let JSONValue::Properties(mut group) = group {
                        if let Some(JSONValue::Array(mut group_addresses)) = group.remove(&JMAPMailProperties::Addresses) {
                            addresses.append(&mut group_addresses);
                        }
                    }
                });
                addresses
            };
                JSONValue::Array(if !as_collection {
                    flatten_group(list)
                } else {
                    list.into_iter()
                        .map(|field| {
                            if let JSONValue::Array(list) = field {
                                JSONValue::Array(flatten_group(list))
                            } else {
                                field
                            }
                        })
                        .collect()
                })
            }
        }
    } else {
        JSONValue::Null
    }
}

pub fn transform_json_stringlist<'x>(
    value: JSONValue<'x, JMAPMailProperties<'x>>,
    as_collection: bool,
) -> JSONValue<'x, JMAPMailProperties<'x>> {
    if let JSONValue::Array(mut list) = value {
        let is_collection = matches!(list.get(0), Some(JSONValue::Array(_)));
        if !as_collection {
            if !is_collection {
                JSONValue::Array(list)
            } else {
                list.pop().unwrap_or_default()
            }
        } else if is_collection {
            JSONValue::Array(list)
        } else {
            JSONValue::Array(vec![JSONValue::Array(list)])
        }
    } else {
        JSONValue::Null
    }
}

pub fn transform_json_string<'x>(
    value: JSONValue<'x, JMAPMailProperties<'x>>,
    as_collection: bool,
) -> JSONValue<'x, JMAPMailProperties<'x>> {
    match value {
        JSONValue::Array(mut list) => {
            if !as_collection {
                list.pop().unwrap_or_default()
            } else {
                JSONValue::Array(list)
            }
        }
        value @ JSONValue::String(_) => {
            if !as_collection {
                value
            } else {
                JSONValue::Array(vec![value])
            }
        }
        _ => JSONValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use jmap_store::json::JSONValue;

    use crate::JMAPMailProperties;

    #[test]
    fn test_json_transform() {
        for (value, expected_result, expected_result_all) in [
            (
                JSONValue::String("hello".into()),
                JSONValue::String("hello".into()),
                JSONValue::Array::<JMAPMailProperties>(vec![JSONValue::String("hello".into())]),
            ),
            (
                JSONValue::Array(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ]),
                JSONValue::String("world".into()),
                JSONValue::Array(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ]),
            ),
        ] {
            assert_eq!(
                super::transform_json_string(value.clone(), false),
                expected_result
            );
            assert_eq!(
                super::transform_json_string(value, true),
                expected_result_all
            );
        }

        for (value, expected_result, expected_result_all) in [
            (
                JSONValue::Array::<JMAPMailProperties>(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ]),
                JSONValue::Array::<JMAPMailProperties>(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ]),
                JSONValue::Array::<JMAPMailProperties>(vec![JSONValue::Array(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ])]),
            ),
            (
                JSONValue::Array(vec![
                    JSONValue::Array::<JMAPMailProperties>(vec![
                        JSONValue::String("hello".into()),
                        JSONValue::String("world".into()),
                    ]),
                    JSONValue::Array::<JMAPMailProperties>(vec![
                        JSONValue::String("hola".into()),
                        JSONValue::String("mundo".into()),
                    ]),
                ]),
                JSONValue::Array::<JMAPMailProperties>(vec![
                    JSONValue::String("hola".into()),
                    JSONValue::String("mundo".into()),
                ]),
                JSONValue::Array(vec![
                    JSONValue::Array::<JMAPMailProperties>(vec![
                        JSONValue::String("hello".into()),
                        JSONValue::String("world".into()),
                    ]),
                    JSONValue::Array::<JMAPMailProperties>(vec![
                        JSONValue::String("hola".into()),
                        JSONValue::String("mundo".into()),
                    ]),
                ]),
            ),
        ] {
            assert_eq!(
                super::transform_json_stringlist(value.clone(), false),
                expected_result
            );
            assert_eq!(
                super::transform_json_stringlist(value, true),
                expected_result_all
            );
        }

        fn make_email<'x>(name: &str, addr: &str) -> JSONValue<'x, JMAPMailProperties<'x>> {
            let mut email = HashMap::new();
            email.insert(
                JMAPMailProperties::Name,
                JSONValue::String(name.to_string().into()),
            );
            email.insert(
                JMAPMailProperties::Email,
                JSONValue::String(addr.to_string().into()),
            );
            JSONValue::Properties(email)
        }

        fn make_group<'x>(
            name: Option<&str>,
            addresses: JSONValue<'x, JMAPMailProperties<'x>>,
        ) -> JSONValue<'x, JMAPMailProperties<'x>> {
            let mut email = HashMap::new();
            email.insert(
                JMAPMailProperties::Name,
                name.map_or(JSONValue::Null, |name| {
                    JSONValue::String(name.to_string().into())
                }),
            );
            email.insert(JMAPMailProperties::Addresses, addresses);
            JSONValue::Properties(email)
        }

        fn make_list<'x>(
            value1: JSONValue<'x, JMAPMailProperties<'x>>,
            value2: JSONValue<'x, JMAPMailProperties<'x>>,
        ) -> JSONValue<'x, JMAPMailProperties<'x>> {
            JSONValue::Array(vec![value1, value2])
        }

        fn make_list_many<'x>(
            value1: JSONValue<'x, JMAPMailProperties<'x>>,
            value2: JSONValue<'x, JMAPMailProperties<'x>>,
            value3: JSONValue<'x, JMAPMailProperties<'x>>,
            value4: JSONValue<'x, JMAPMailProperties<'x>>,
        ) -> JSONValue<'x, JMAPMailProperties<'x>> {
            JSONValue::Array(vec![value1, value2, value3, value4])
        }

        fn make_list_single<'x>(
            value: JSONValue<'x, JMAPMailProperties<'x>>,
        ) -> JSONValue<'x, JMAPMailProperties<'x>> {
            JSONValue::Array(vec![value])
        }

        for (
            value,
            expected_result_single_addr,
            expected_result_all_addr,
            expected_result_single_group,
            expected_result_all_group,
        ) in [
            (
                make_list(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                ),
                make_list(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                ),
                make_list_single(make_list(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                )),
                make_list_single(make_group(
                    None,
                    make_list(
                        make_email("John Doe", "jdoe@domain.com"),
                        make_email("Jane Smith", "jsmith@test.com"),
                    ),
                )),
                make_list_single(make_list_single(make_group(
                    None,
                    make_list(
                        make_email("John Doe", "jdoe@domain.com"),
                        make_email("Jane Smith", "jsmith@test.com"),
                    ),
                ))),
            ),
            (
                make_list(
                    make_list(
                        make_email("John Doe", "jdoe@domain.com"),
                        make_email("Jane Smith", "jsmith@test.com"),
                    ),
                    make_list(
                        make_email("Juan Gomez", "jgomez@dominio.com"),
                        make_email("Juanita Perez", "jperez@prueba.com"),
                    ),
                ),
                make_list(
                    make_email("Juan Gomez", "jgomez@dominio.com"),
                    make_email("Juanita Perez", "jperez@prueba.com"),
                ),
                make_list(
                    make_list(
                        make_email("John Doe", "jdoe@domain.com"),
                        make_email("Jane Smith", "jsmith@test.com"),
                    ),
                    make_list(
                        make_email("Juan Gomez", "jgomez@dominio.com"),
                        make_email("Juanita Perez", "jperez@prueba.com"),
                    ),
                ),
                make_list_single(make_group(
                    None,
                    make_list(
                        make_email("Juan Gomez", "jgomez@dominio.com"),
                        make_email("Juanita Perez", "jperez@prueba.com"),
                    ),
                )),
                make_list(
                    make_list_single(make_group(
                        None,
                        make_list(
                            make_email("John Doe", "jdoe@domain.com"),
                            make_email("Jane Smith", "jsmith@test.com"),
                        ),
                    )),
                    make_list_single(make_group(
                        None,
                        make_list(
                            make_email("Juan Gomez", "jgomez@dominio.com"),
                            make_email("Juanita Perez", "jperez@prueba.com"),
                        ),
                    )),
                ),
            ),
            (
                make_list(
                    make_group(
                        "Group 1".into(),
                        make_list(
                            make_email("John Doe", "jdoe@domain.com"),
                            make_email("Jane Smith", "jsmith@test.com"),
                        ),
                    ),
                    make_group(
                        "Group 2".into(),
                        make_list(
                            make_email("Juan Gomez", "jgomez@dominio.com"),
                            make_email("Juanita Perez", "jperez@prueba.com"),
                        ),
                    ),
                ),
                make_list_many(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                    make_email("Juan Gomez", "jgomez@dominio.com"),
                    make_email("Juanita Perez", "jperez@prueba.com"),
                ),
                make_list_single(make_list_many(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                    make_email("Juan Gomez", "jgomez@dominio.com"),
                    make_email("Juanita Perez", "jperez@prueba.com"),
                )),
                make_list(
                    make_group(
                        "Group 1".into(),
                        make_list(
                            make_email("John Doe", "jdoe@domain.com"),
                            make_email("Jane Smith", "jsmith@test.com"),
                        ),
                    ),
                    make_group(
                        "Group 2".into(),
                        make_list(
                            make_email("Juan Gomez", "jgomez@dominio.com"),
                            make_email("Juanita Perez", "jperez@prueba.com"),
                        ),
                    ),
                ),
                make_list_single(make_list(
                    make_group(
                        "Group 1".into(),
                        make_list(
                            make_email("John Doe", "jdoe@domain.com"),
                            make_email("Jane Smith", "jsmith@test.com"),
                        ),
                    ),
                    make_group(
                        "Group 2".into(),
                        make_list(
                            make_email("Juan Gomez", "jgomez@dominio.com"),
                            make_email("Juanita Perez", "jperez@prueba.com"),
                        ),
                    ),
                )),
            ),
            (
                make_list(
                    make_list(
                        make_group(
                            "Group 1".into(),
                            make_list(
                                make_email("Tim Hortons", "tim@hortos.com"),
                                make_email("Ronald McDowell", "ronnie@mac.com"),
                            ),
                        ),
                        make_group(
                            "Group 2".into(),
                            make_list(
                                make_email("Wendy D", "wendy@d.com"),
                                make_email("Kentucky Frango", "kentucky@frango.com"),
                            ),
                        ),
                    ),
                    make_list(
                        make_group(
                            "Group 3".into(),
                            make_list(
                                make_email("John Doe", "jdoe@domain.com"),
                                make_email("Jane Smith", "jsmith@test.com"),
                            ),
                        ),
                        make_group(
                            "Group 4".into(),
                            make_list(
                                make_email("Juan Gomez", "jgomez@dominio.com"),
                                make_email("Juanita Perez", "jperez@prueba.com"),
                            ),
                        ),
                    ),
                ),
                make_list_many(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                    make_email("Juan Gomez", "jgomez@dominio.com"),
                    make_email("Juanita Perez", "jperez@prueba.com"),
                ),
                make_list(
                    make_list_many(
                        make_email("Tim Hortons", "tim@hortos.com"),
                        make_email("Ronald McDowell", "ronnie@mac.com"),
                        make_email("Wendy D", "wendy@d.com"),
                        make_email("Kentucky Frango", "kentucky@frango.com"),
                    ),
                    make_list_many(
                        make_email("John Doe", "jdoe@domain.com"),
                        make_email("Jane Smith", "jsmith@test.com"),
                        make_email("Juan Gomez", "jgomez@dominio.com"),
                        make_email("Juanita Perez", "jperez@prueba.com"),
                    ),
                ),
                make_list(
                    make_group(
                        "Group 3".into(),
                        make_list(
                            make_email("John Doe", "jdoe@domain.com"),
                            make_email("Jane Smith", "jsmith@test.com"),
                        ),
                    ),
                    make_group(
                        "Group 4".into(),
                        make_list(
                            make_email("Juan Gomez", "jgomez@dominio.com"),
                            make_email("Juanita Perez", "jperez@prueba.com"),
                        ),
                    ),
                ),
                make_list(
                    make_list(
                        make_group(
                            "Group 1".into(),
                            make_list(
                                make_email("Tim Hortons", "tim@hortos.com"),
                                make_email("Ronald McDowell", "ronnie@mac.com"),
                            ),
                        ),
                        make_group(
                            "Group 2".into(),
                            make_list(
                                make_email("Wendy D", "wendy@d.com"),
                                make_email("Kentucky Frango", "kentucky@frango.com"),
                            ),
                        ),
                    ),
                    make_list(
                        make_group(
                            "Group 3".into(),
                            make_list(
                                make_email("John Doe", "jdoe@domain.com"),
                                make_email("Jane Smith", "jsmith@test.com"),
                            ),
                        ),
                        make_group(
                            "Group 4".into(),
                            make_list(
                                make_email("Juan Gomez", "jgomez@dominio.com"),
                                make_email("Juanita Perez", "jperez@prueba.com"),
                            ),
                        ),
                    ),
                ),
            ),
        ] {
            assert_eq!(
                super::transform_json_emailaddress(value.clone(), false, false),
                expected_result_single_addr,
                "single+address"
            );
            assert_eq!(
                super::transform_json_emailaddress(value.clone(), false, true),
                expected_result_all_addr,
                "all+address"
            );
            assert_eq!(
                super::transform_json_emailaddress(value.clone(), true, false),
                expected_result_single_group,
                "single+group"
            );
            assert_eq!(
                super::transform_json_emailaddress(value.clone(), true, true),
                expected_result_all_group,
                "all+group"
            );
        }
    }
}
