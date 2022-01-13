use std::{
    borrow::Cow,
    collections::{btree_map, hash_map::Entry, BTreeMap, HashMap},
    iter::FromIterator,
};

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
        preview::{preview_html, preview_text, truncate_html, truncate_text},
    },
    HeaderName, HeaderOffset, HeaderValue, MessageStructure, RawHeaders, RfcHeader,
};
use store::{AccountId, BlobEntry, DocumentId, Store, Tag};

use crate::{
    changes::JMAPMailLocalStoreChanges,
    import::bincode_deserialize,
    parse::{
        header_to_jmap_address, header_to_jmap_date, header_to_jmap_id, header_to_jmap_text,
        header_to_jmap_url,
    },
    query::MailboxId,
    JMAPMailBodyProperties, JMAPMailHeaderForm, JMAPMailHeaderProperty, JMAPMailHeaders,
    JMAPMailIdImpl, JMAPMailMimeHeaders, JMAPMailProperties, JMAPMailStoreGetArguments,
    MessageBody, MessageField, MessageRawHeaders, MimePart, MimePartType, MESSAGE_BODY,
    MESSAGE_BODY_STRUCTURE, MESSAGE_HEADERS, MESSAGE_HEADERS_RAW, MESSAGE_PARTS, MESSAGE_RAW,
};

pub const DEFAULT_RAW_FETCH_SIZE: usize = 512;

pub trait JMAPMailLocalStoreGet<'x>: JMAPMailLocalStoreChanges<'x> + Store<'x> {
    fn mail_get(
        &self,
        request: JMAPGet<JMAPMailProperties<'x>>,
        mut arguments: JMAPMailStoreGetArguments,
    ) -> jmap_store::Result<jmap_store::JMAPGetResponse<'x>> {
        let mut blob_indexes = [false; MESSAGE_PARTS];

        let properties = if let Some(properties) = request.properties {
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

                    JMAPMailProperties::Header(JMAPMailHeaderProperty {
                        form: JMAPMailHeaderForm::Raw,
                        ..
                    })
                    | JMAPMailProperties::Header(JMAPMailHeaderProperty {
                        header: HeaderName::Other(_),
                        ..
                    }) => {
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
                    | JMAPMailProperties::Header(_) => {
                        blob_indexes[MESSAGE_HEADERS] = true;
                    }

                    // Ignore sub-properties
                    _ => (),
                }
            }
            properties
        } else {
            blob_indexes[MESSAGE_HEADERS] = true;
            blob_indexes[MESSAGE_BODY] = true;

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
        };

        let raw_fetch_size = if arguments.body_properties.is_empty() {
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
            DEFAULT_RAW_FETCH_SIZE
        } else if arguments.body_properties.iter().any(|prop| {
            matches!(
                prop,
                JMAPMailBodyProperties::Headers | JMAPMailBodyProperties::Header(_)
            )
        }) {
            blob_indexes[MESSAGE_HEADERS_RAW] = true;
            blob_indexes[MESSAGE_RAW] = true;
            0
        } else {
            DEFAULT_RAW_FETCH_SIZE
        };

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
                            Some(if index != MESSAGE_RAW || raw_fetch_size == 0 {
                                BlobEntry::new(index)
                            } else {
                                BlobEntry::new_range(index, 0..raw_fetch_size)
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
                        if raw_fetch_size > 0 && raw_headers.size > raw_fetch_size {
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

            let mut result: HashMap<Cow<'x, str>, JSONValue<'x, Cow<'x, str>>> = HashMap::new();

            for property in &properties {
                let property_str: Cow<str> = property.to_string().into();
                if result.contains_key(&property_str) {
                    continue;
                }

                result.insert(
                    property_str,
                    match property {
                        JMAPMailProperties::Header(JMAPMailHeaderProperty {
                            form: JMAPMailHeaderForm::Raw,
                            header,
                            all,
                        }) => add_raw_header(
                            message_headers_raw.as_mut().unwrap(),
                            message_raw.as_ref().unwrap(),
                            header.clone(),
                            JMAPMailHeaderForm::Raw,
                            *all,
                        )
                        .into_string(),
                        JMAPMailProperties::Header(JMAPMailHeaderProperty {
                            form,
                            header: header @ HeaderName::Other(_),
                            all,
                        }) => add_raw_header(
                            message_headers_raw.as_mut().unwrap(),
                            message_raw.as_ref().unwrap(),
                            header.clone(),
                            form.clone(),
                            *all,
                        )
                        .into_string(),
                        JMAPMailProperties::MessageId => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            RfcHeader::MessageId,
                            JMAPMailHeaderForm::MessageIds,
                            false,
                        )?
                        .into_string(),
                        JMAPMailProperties::InReplyTo => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            RfcHeader::InReplyTo,
                            JMAPMailHeaderForm::MessageIds,
                            false,
                        )?
                        .into_string(),
                        JMAPMailProperties::References => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            RfcHeader::References,
                            JMAPMailHeaderForm::MessageIds,
                            false,
                        )?
                        .into_string(),
                        JMAPMailProperties::Sender => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            RfcHeader::Sender,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?
                        .into_string(),
                        JMAPMailProperties::From => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            RfcHeader::From,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?
                        .into_string(),
                        JMAPMailProperties::To => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            RfcHeader::To,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?
                        .into_string(),
                        JMAPMailProperties::Cc => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            RfcHeader::Cc,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?
                        .into_string(),
                        JMAPMailProperties::Bcc => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            RfcHeader::Bcc,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?
                        .into_string(),
                        JMAPMailProperties::ReplyTo => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            RfcHeader::ReplyTo,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?
                        .into_string(),
                        JMAPMailProperties::Subject => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            RfcHeader::Subject,
                            JMAPMailHeaderForm::Text,
                            false,
                        )?
                        .into_string(),
                        JMAPMailProperties::SentAt => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            RfcHeader::Date,
                            JMAPMailHeaderForm::Date,
                            false,
                        )?
                        .into_string(),
                        JMAPMailProperties::Header(JMAPMailHeaderProperty {
                            form,
                            header: HeaderName::Rfc(header),
                            all,
                        }) => add_rfc_header(
                            message_headers.as_mut().unwrap(),
                            *header,
                            form.clone(),
                            *all,
                        )?
                        .into_string(),

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
                                JSONValue::Object(
                                    bincode_deserialize::<Vec<MailboxId>>(&mailboxes)?
                                        .into_iter()
                                        .map(|mailbox_id| {
                                            (
                                                (mailbox_id as JMAPId).to_jmap_string().into(),
                                                JSONValue::Bool(true),
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
                                JSONValue::Object(
                                    bincode_deserialize::<Vec<Tag>>(&tags)?
                                        .into_iter()
                                        .map(|tag| {
                                            (
                                                match tag {
                                                    Tag::Static(_) => "todo!()".to_string().into(), //TODO map static keywords
                                                    Tag::Id(_) => "todo!()".to_string().into(),
                                                    Tag::Text(text) => text,
                                                },
                                                JSONValue::Bool(true),
                                            )
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
                        JMAPMailProperties::TextBody => add_body_parts(
                            request.account_id,
                            document_id,
                            &message_body.as_ref().unwrap().text_body,
                            &message_body.as_ref().unwrap().mime_parts,
                            &arguments.body_properties,
                            message_raw.as_ref(),
                            message_headers_raw.as_ref(),
                        ),

                        JMAPMailProperties::HtmlBody => add_body_parts(
                            request.account_id,
                            document_id,
                            &message_body.as_ref().unwrap().html_body,
                            &message_body.as_ref().unwrap().mime_parts,
                            &arguments.body_properties,
                            message_raw.as_ref(),
                            message_headers_raw.as_ref(),
                        ),

                        JMAPMailProperties::Attachments => add_body_parts(
                            request.account_id,
                            document_id,
                            &message_body.as_ref().unwrap().attachments,
                            &message_body.as_ref().unwrap().mime_parts,
                            &arguments.body_properties,
                            message_raw.as_ref(),
                            message_headers_raw.as_ref(),
                        ),

                        JMAPMailProperties::Preview => {
                            let message_body = message_body.as_ref().unwrap();
                            if !message_body.text_body.is_empty() {
                                JSONValue::String(preview_text(
                                    String::from_utf8(
                                        self.get_document_blob_entry(
                                            request.account_id,
                                            JMAP_MAIL,
                                            document_id,
                                            BlobEntry::new_range(
                                                MESSAGE_PARTS + message_body.text_body[0],
                                                0..260,
                                            ),
                                        )?
                                        .value,
                                    )
                                    .map_or_else(
                                        |err| {
                                            String::from_utf8_lossy(err.as_bytes())
                                                .into_owned()
                                                .into()
                                        },
                                        |s| s.into(),
                                    ),
                                    256,
                                ))
                            } else if !message_body.html_body.is_empty() {
                                JSONValue::String(preview_html(
                                    String::from_utf8(
                                        self.get_document_blob_entry(
                                            request.account_id,
                                            JMAP_MAIL,
                                            document_id,
                                            BlobEntry::new(
                                                MESSAGE_PARTS + message_body.html_body[0],
                                            ),
                                        )?
                                        .value,
                                    )
                                    .map_or_else(
                                        |err| {
                                            String::from_utf8_lossy(err.as_bytes())
                                                .into_owned()
                                                .into()
                                        },
                                        |s| s.into(),
                                    ),
                                    256,
                                ))
                            } else {
                                JSONValue::Null
                            }
                        }
                        JMAPMailProperties::BodyValues => {
                            let message_body = message_body.as_ref().unwrap();
                            let mut fetch_parts = BTreeMap::new();
                            if arguments.fetch_all_body_values || arguments.fetch_text_body_values {
                                message_body.text_body.iter().for_each(|part| {
                                    if let Some(mime_part) = message_body.mime_parts.get(*part) {
                                        if let MimePartType::Html | MimePartType::Text =
                                            mime_part.mime_type
                                        {
                                            if let btree_map::Entry::Vacant(entry) =
                                                fetch_parts.entry(*part + MESSAGE_PARTS)
                                            {
                                                entry.insert(mime_part);
                                            }
                                        }
                                    }
                                });
                            }
                            if arguments.fetch_all_body_values || arguments.fetch_html_body_values {
                                message_body.html_body.iter().for_each(|part| {
                                    if let Some(mime_part) = message_body.mime_parts.get(*part) {
                                        if let MimePartType::Html | MimePartType::Text =
                                            mime_part.mime_type
                                        {
                                            if let btree_map::Entry::Vacant(entry) =
                                                fetch_parts.entry(*part + MESSAGE_PARTS)
                                            {
                                                entry.insert(mime_part);
                                            }
                                        }
                                    }
                                });
                            }
                            if !fetch_parts.is_empty() {
                                JSONValue::Object(HashMap::from_iter(
                                    self.get_document_blob_entries(
                                        request.account_id,
                                        JMAP_MAIL,
                                        document_id,
                                        fetch_parts.keys().map(|k| {
                                            if arguments.max_body_value_bytes == 0 {
                                                BlobEntry::new(*k)
                                            } else {
                                                BlobEntry::new_range(
                                                    *k,
                                                    0..(arguments.max_body_value_bytes + 10),
                                                )
                                            }
                                        }),
                                    )?
                                    .into_iter()
                                    .map(|blob_entry| {
                                        let mut body_value = HashMap::with_capacity(3);
                                        let mime_part = fetch_parts.get(&blob_entry.index).unwrap();
                                        body_value.insert(
                                            "isEncodingProblem".into(),
                                            JSONValue::Bool(mime_part.is_encoding_problem),
                                        );
                                        body_value.insert(
                                            "isTruncated".into(),
                                            JSONValue::Bool(
                                                arguments.max_body_value_bytes > 0
                                                    && blob_entry.value.len()
                                                        > arguments.max_body_value_bytes,
                                            ),
                                        );
                                        let body_text: Cow<str> =
                                            String::from_utf8(blob_entry.value).map_or_else(
                                                |err| {
                                                    String::from_utf8_lossy(err.as_bytes())
                                                        .into_owned()
                                                        .into()
                                                },
                                                |s| s.into(),
                                            );
                                        body_value.insert(
                                            "value".into(),
                                            if arguments.max_body_value_bytes == 0
                                                || body_text.len() <= arguments.max_body_value_bytes
                                            {
                                                JSONValue::String(body_text)
                                            } else {
                                                JSONValue::String(
                                                    if let MimePartType::Html = mime_part.mime_type
                                                    {
                                                        truncate_html(
                                                            body_text,
                                                            arguments.max_body_value_bytes,
                                                        )
                                                    } else {
                                                        truncate_text(
                                                            body_text,
                                                            arguments.max_body_value_bytes,
                                                        )
                                                    },
                                                )
                                            },
                                        );

                                        (
                                            (blob_entry.index - MESSAGE_PARTS).to_string().into(),
                                            JSONValue::Object(body_value),
                                        )
                                    }),
                                ))
                            } else {
                                JSONValue::Null
                            }
                        }

                        JMAPMailProperties::BodyStructure => {
                            if let Some(body_structure) = add_body_structure(
                                request.account_id,
                                document_id,
                                message_body_structure.as_ref().unwrap(),
                                &message_body.as_ref().unwrap().mime_parts,
                                &arguments.body_properties,
                                message_raw.as_ref(),
                                message_headers_raw.as_ref(),
                            ) {
                                body_structure
                            } else {
                                JSONValue::Null
                            }
                        }

                        // Ignore internal properties
                        _ => continue,
                    },
                );
            }

            results.push(JSONValue::Object(result));
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

fn add_body_structure<'x, 'y>(
    account: AccountId,
    document: DocumentId,
    message_body_structure: &MessageStructure,
    mime_parts: &[MimePart<'x>],
    properties: &[JMAPMailBodyProperties<'y>],
    message_raw: Option<&Vec<u8>>,
    message_raw_headers: Option<&MessageRawHeaders>,
) -> Option<JSONValue<'x, Cow<'x, str>>> {
    let mut parts_stack = Vec::with_capacity(5);
    let mut stack = Vec::new();

    let part_list = match message_body_structure {
        MessageStructure::Part(part_id) => {
            return Some(JSONValue::Object(add_body_part(
                account,
                document,
                (*part_id).into(),
                &mime_parts.get(part_id + 1)?.headers,
                properties,
                message_raw,
                if let Some(message_raw_headers) = message_raw_headers {
                    Some(&message_raw_headers.headers)
                } else {
                    None
                },
            )))
        }
        MessageStructure::List(part_list) => {
            parts_stack.push(add_body_part(
                account,
                document,
                None,
                &mime_parts.get(0)?.headers,
                properties,
                message_raw,
                if let Some(message_raw_headers) = message_raw_headers {
                    Some(&message_raw_headers.headers)
                } else {
                    None
                },
            ));
            part_list
        }
        MessageStructure::MultiPart((part_id, part_list)) => {
            parts_stack.push(add_body_part(
                account,
                document,
                None,
                &mime_parts.get(0)?.headers,
                properties,
                message_raw,
                if let Some(message_raw_headers) = message_raw_headers {
                    Some(&message_raw_headers.headers)
                } else {
                    None
                },
            ));
            parts_stack.push(add_body_part(
                account,
                document,
                None,
                &mime_parts.get(part_id + 1)?.headers,
                properties,
                message_raw,
                if let Some(message_raw_headers) = message_raw_headers {
                    Some(message_raw_headers.parts_headers.get(*part_id)?)
                } else {
                    None
                },
            ));
            stack.push(([].iter(), vec![]));
            part_list
        }
    };

    let mut subparts = Vec::with_capacity(part_list.len());
    let mut part_list_iter = part_list.iter();

    loop {
        while let Some(part) = part_list_iter.next() {
            match part {
                MessageStructure::Part(part_id) => subparts.push(JSONValue::Object(add_body_part(
                    account,
                    document,
                    (*part_id).into(),
                    &mime_parts.get(part_id + 1)?.headers,
                    properties,
                    message_raw,
                    if let Some(message_raw_headers) = message_raw_headers {
                        Some(message_raw_headers.parts_headers.get(*part_id)?)
                    } else {
                        None
                    },
                ))),
                MessageStructure::MultiPart((part_id, next_part_list)) => {
                    parts_stack.push(add_body_part(
                        account,
                        document,
                        None,
                        &mime_parts.get(part_id + 1)?.headers,
                        properties,
                        message_raw,
                        if let Some(message_raw_headers) = message_raw_headers {
                            Some(message_raw_headers.parts_headers.get(*part_id)?)
                        } else {
                            None
                        },
                    ));
                    stack.push((part_list_iter, subparts));
                    part_list_iter = next_part_list.iter();
                    subparts = Vec::with_capacity(part_list.len());
                }
                MessageStructure::List(_) => (),
            }
        }

        if let Some((prev_part_list_iter, prev_subparts)) = stack.pop() {
            let mut prev_part = parts_stack.pop().unwrap();
            prev_part.insert("subparts".into(), JSONValue::Array(subparts));
            part_list_iter = prev_part_list_iter;
            subparts = prev_subparts;
        } else {
            break;
        }
    }

    let mut root_part = parts_stack.pop().unwrap();
    root_part.insert("subparts".into(), JSONValue::Array(subparts));
    Some(JSONValue::Object(root_part))
}

fn add_body_parts<'x, 'y>(
    account: AccountId,
    document: DocumentId,
    parts: &[usize],
    mime_parts: &[MimePart<'x>],
    properties: &[JMAPMailBodyProperties<'y>],
    message_raw: Option<&Vec<u8>>,
    message_raw_headers: Option<&MessageRawHeaders>,
) -> JSONValue<'x, Cow<'x, str>> {
    JSONValue::Array(
        parts
            .iter()
            .filter_map(|part_index| {
                Some(JSONValue::Object(add_body_part(
                    account,
                    document,
                    (*part_index).into(),
                    &mime_parts.get(part_index + 1)?.headers,
                    properties,
                    message_raw,
                    if let Some(message_raw_headers) = message_raw_headers {
                        Some(message_raw_headers.parts_headers.get(*part_index)?)
                    } else {
                        None
                    },
                )))
            })
            .collect(),
    )
}

fn add_body_part<'x, 'y>(
    account: AccountId,
    document: DocumentId,
    part_id: Option<usize>,
    headers: &JMAPMailMimeHeaders<'x>,
    properties: &[JMAPMailBodyProperties<'y>],
    message_raw: Option<&Vec<u8>>,
    headers_raw: Option<&RawHeaders<'y>>,
) -> HashMap<Cow<'x, str>, JSONValue<'x, Cow<'x, str>>> {
    let mut body_part = HashMap::with_capacity(properties.len());

    let mut headers_result: HashMap<Cow<str>, Vec<JSONValue<Cow<str>>>> = HashMap::new();

    let get_raw_header = |value: &Vec<HeaderOffset>| -> Vec<_> {
        value
            .iter()
            .filter_map(|offset| {
                Some(JSONValue::String(
                    std::str::from_utf8(message_raw.unwrap().get(offset.start..offset.end)?)
                        .map_or_else(
                            |_| {
                                String::from_utf8_lossy(
                                    message_raw.unwrap().get(offset.start..offset.end).unwrap(),
                                )
                                .trim()
                                .to_string()
                                .into()
                            },
                            |str| str.trim().to_string().into(),
                        ),
                ))
            })
            .collect::<Vec<JSONValue<Cow<str>>>>()
    };

    for property in properties {
        match property {
            JMAPMailBodyProperties::Size
            | JMAPMailBodyProperties::Name
            | JMAPMailBodyProperties::Type
            | JMAPMailBodyProperties::Charset
            | JMAPMailBodyProperties::Disposition
            | JMAPMailBodyProperties::Cid
            | JMAPMailBodyProperties::Language
            | JMAPMailBodyProperties::Location => {
                if let Some(value) = headers.get(&property.into()) {
                    body_part.insert(property.to_string().into(), value.into());
                }
            }

            JMAPMailBodyProperties::BlobId => {
                if let Some(part_id) = part_id {
                    body_part.insert(
                        "blobId".into(),
                        JSONValue::String(
                            BlobId::new(account, JMAP_MAIL, document, MESSAGE_PARTS + part_id)
                                .to_jmap_string()
                                .into(),
                        ),
                    );
                }
            }
            JMAPMailBodyProperties::Header(header) => {
                if let Some(header_raw) = headers_raw.unwrap().get(&header.header) {
                    if let Entry::Vacant(entry) = headers_result.entry(header.to_string().into()) {
                        entry.insert(get_raw_header(header_raw));
                    }
                }
            }
            JMAPMailBodyProperties::Headers => {
                for (header, value) in headers_raw.unwrap() {
                    if let Entry::Vacant(entry) = headers_result.entry(
                        JMAPMailProperties::Header(JMAPMailHeaderProperty {
                            form: JMAPMailHeaderForm::Raw,
                            header: header.into_owned(),
                            all: false,
                        })
                        .to_string()
                        .into(),
                    ) {
                        entry.insert(get_raw_header(value));
                    }
                }
            }
            JMAPMailBodyProperties::PartId => {
                if let Some(part_id) = part_id {
                    body_part.insert("partId".into(), JSONValue::Number(part_id as i64));
                }
            }
            JMAPMailBodyProperties::Subparts => (),
        }
    }

    if !headers_result.is_empty() {
        body_part.insert(
            "headers".into(),
            JSONValue::Array(
                headers_result
                    .into_iter()
                    .map(|(header, values)| {
                        values.into_iter().map(move |value| {
                            let mut result: HashMap<Cow<'x, str>, JSONValue<Cow<'x, str>>> =
                                HashMap::with_capacity(2);
                            result.insert(
                                "name".into(),
                                JSONValue::String(header.as_ref().to_string().into()),
                            );
                            result.insert("value".into(), value);
                            JSONValue::Object(result)
                        })
                    })
                    .flatten()
                    .collect(),
            ),
        );
    }

    body_part
}

fn add_rfc_header<'x>(
    message_headers: &mut JMAPMailHeaders<'x>,
    header: RfcHeader,
    form: JMAPMailHeaderForm,
    all: bool,
) -> jmap_store::Result<JSONValue<'x, JMAPMailProperties<'x>>> {
    Ok(match (header, form.clone()) {
        (RfcHeader::Date | RfcHeader::ResentDate, JMAPMailHeaderForm::Date)
        | (
            RfcHeader::Subject | RfcHeader::Comments | RfcHeader::Keywords | RfcHeader::ListId,
            JMAPMailHeaderForm::Text,
        ) => transform_json_string(message_headers.remove(&header).unwrap_or_default(), all),
        (
            RfcHeader::MessageId
            | RfcHeader::References
            | RfcHeader::ResentMessageId
            | RfcHeader::InReplyTo,
            JMAPMailHeaderForm::MessageIds,
        )
        | (
            RfcHeader::ListArchive
            | RfcHeader::ListHelp
            | RfcHeader::ListOwner
            | RfcHeader::ListPost
            | RfcHeader::ListSubscribe
            | RfcHeader::ListUnsubscribe,
            JMAPMailHeaderForm::URLs,
        ) => transform_json_stringlist(message_headers.remove(&header).unwrap_or_default(), all),
        (
            RfcHeader::From
            | RfcHeader::To
            | RfcHeader::Cc
            | RfcHeader::Bcc
            | RfcHeader::ReplyTo
            | RfcHeader::Sender
            | RfcHeader::ResentTo
            | RfcHeader::ResentFrom
            | RfcHeader::ResentBcc
            | RfcHeader::ResentCc
            | RfcHeader::ResentSender,
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
    header_name: HeaderName<'y>,
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
                matches!(list.get(0), Some(JSONValue::Object(obj)) if obj.contains_key(&JMAPMailProperties::Addresses)),
            ),
            Some(JSONValue::Object(obj)) => {
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
                JSONValue::Object(group)
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
                    if let JSONValue::Object(mut group) = group {
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
            JSONValue::Object(email)
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
            JSONValue::Object(email)
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
