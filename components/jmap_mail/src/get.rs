use std::{
    collections::{btree_map, hash_map::Entry, BTreeMap, HashMap},
    iter::FromIterator,
};

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
use store::{leb128::Leb128, DocumentSet};
use store::{AccountId, BlobEntry, DocumentId, Store, StoreError, Tag};

use crate::{
    changes::JMAPMailLocalStoreChanges,
    import::bincode_deserialize,
    parse::{
        header_to_jmap_address, header_to_jmap_date, header_to_jmap_id, header_to_jmap_text,
        header_to_jmap_url,
    },
    query::MailboxId,
    JMAPMailBodyProperties, JMAPMailHeaderForm, JMAPMailHeaderProperty, JMAPMailHeaders,
    JMAPMailIdImpl, JMAPMailProperties, JMAPMailStoreGetArguments, MessageData, MessageField,
    MessageOutline, MimePart, MimePartType, MESSAGE_DATA, MESSAGE_PARTS, MESSAGE_RAW,
};

pub const DEFAULT_RAW_FETCH_SIZE: usize = 512;

pub trait JMAPMailLocalStoreGet<'x>: JMAPMailLocalStoreChanges<'x> + Store<'x> {
    fn mail_get(
        &self,
        request: JMAPGet<JMAPMailProperties<'x>>,
        mut arguments: JMAPMailStoreGetArguments,
    ) -> jmap_store::Result<jmap_store::JMAPGetResponse> {
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

        enum FetchRaw {
            Header,
            All,
            None,
        }

        let fetch_raw = if arguments.body_properties.iter().any(|prop| {
            matches!(
                prop,
                JMAPMailBodyProperties::Headers | JMAPMailBodyProperties::Header(_)
            )
        }) {
            FetchRaw::All
        } else if properties.iter().any(|prop| {
            matches!(
                prop,
                JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Raw,
                    ..
                }) | JMAPMailProperties::Header(JMAPMailHeaderProperty {
                    header: HeaderName::Other(_),
                    ..
                }) | JMAPMailProperties::BodyStructure
            )
        }) {
            FetchRaw::Header
        } else {
            FetchRaw::None
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

        let document_ids = self.get_document_ids(request.account_id, JMAP_MAIL)?;
        let mut not_found = Vec::new();
        let mut results = Vec::with_capacity(request_ids.len());

        for jmap_id in request_ids {
            let document_id = jmap_id.get_document_id();
            if !document_ids.contains(document_id) {
                not_found.push(jmap_id);
                continue;
            }

            let message_data_bytes = self
                .get_blob(
                    request.account_id,
                    JMAP_MAIL,
                    document_id,
                    BlobEntry::new(MESSAGE_DATA),
                )?
                .ok_or(StoreError::DataCorruption)?
                .value;

            let (message_data_len, read_bytes) = usize::from_leb128_bytes(&message_data_bytes[..])
                .ok_or(StoreError::DataCorruption)?;

            let mut message_data = bincode_deserialize::<MessageData>(
                &message_data_bytes[read_bytes..read_bytes + message_data_len],
            )?;
            let (message_raw, mut message_outline) = match &fetch_raw {
                FetchRaw::All => (
                    Some(
                        self.get_blob(
                            request.account_id,
                            JMAP_MAIL,
                            document_id,
                            BlobEntry::new(MESSAGE_RAW),
                        )?
                        .ok_or(StoreError::DataCorruption)?
                        .value,
                    ),
                    Some(bincode_deserialize::<MessageOutline>(
                        &message_data_bytes[read_bytes + message_data_len..],
                    )?),
                ),
                FetchRaw::Header => {
                    let message_outline: MessageOutline = bincode_deserialize::<MessageOutline>(
                        &message_data_bytes[read_bytes + message_data_len..],
                    )?;
                    (
                        Some(
                            self.get_blob(
                                request.account_id,
                                JMAP_MAIL,
                                document_id,
                                BlobEntry::new_range(MESSAGE_RAW, 0..message_outline.body_offset),
                            )?
                            .ok_or(StoreError::DataCorruption)?
                            .value,
                        ),
                        Some(message_outline),
                    )
                }
                FetchRaw::None => (None, None),
            };

            let mut result: HashMap<String, JSONValue> = HashMap::new();

            for property in &properties {
                if let Entry::Vacant(entry) = result.entry(property.to_string()) {
                    let value = match property {
                        JMAPMailProperties::Header(JMAPMailHeaderProperty {
                            form: form @ JMAPMailHeaderForm::Raw,
                            header,
                            all,
                        })
                        | JMAPMailProperties::Header(JMAPMailHeaderProperty {
                            form,
                            header: header @ HeaderName::Other(_),
                            all,
                        }) => {
                            if let Some(offsets) = message_outline
                                .as_mut()
                                .unwrap()
                                .headers
                                .get_mut(0)
                                .and_then(|l| l.remove(header))
                            {
                                add_raw_header(
                                    &offsets,
                                    message_raw.as_ref().unwrap(),
                                    form.clone(),
                                    *all,
                                )
                            } else {
                                JSONValue::Null
                            }
                        }
                        JMAPMailProperties::MessageId => add_rfc_header(
                            &mut message_data.properties,
                            RfcHeader::MessageId,
                            JMAPMailHeaderForm::MessageIds,
                            false,
                        )?,
                        JMAPMailProperties::InReplyTo => add_rfc_header(
                            &mut message_data.properties,
                            RfcHeader::InReplyTo,
                            JMAPMailHeaderForm::MessageIds,
                            false,
                        )?,
                        JMAPMailProperties::References => add_rfc_header(
                            &mut message_data.properties,
                            RfcHeader::References,
                            JMAPMailHeaderForm::MessageIds,
                            false,
                        )?,
                        JMAPMailProperties::Sender => add_rfc_header(
                            &mut message_data.properties,
                            RfcHeader::Sender,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?,
                        JMAPMailProperties::From => add_rfc_header(
                            &mut message_data.properties,
                            RfcHeader::From,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?,
                        JMAPMailProperties::To => add_rfc_header(
                            &mut message_data.properties,
                            RfcHeader::To,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?,
                        JMAPMailProperties::Cc => add_rfc_header(
                            &mut message_data.properties,
                            RfcHeader::Cc,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?,
                        JMAPMailProperties::Bcc => add_rfc_header(
                            &mut message_data.properties,
                            RfcHeader::Bcc,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?,
                        JMAPMailProperties::ReplyTo => add_rfc_header(
                            &mut message_data.properties,
                            RfcHeader::ReplyTo,
                            JMAPMailHeaderForm::Addresses,
                            false,
                        )?,
                        JMAPMailProperties::Subject => add_rfc_header(
                            &mut message_data.properties,
                            RfcHeader::Subject,
                            JMAPMailHeaderForm::Text,
                            false,
                        )?,
                        JMAPMailProperties::SentAt => add_rfc_header(
                            &mut message_data.properties,
                            RfcHeader::Date,
                            JMAPMailHeaderForm::Date,
                            false,
                        )?,
                        JMAPMailProperties::Header(JMAPMailHeaderProperty {
                            form,
                            header: HeaderName::Rfc(header),
                            all,
                        }) => add_rfc_header(
                            &mut message_data.properties,
                            *header,
                            form.clone(),
                            *all,
                        )?,
                        JMAPMailProperties::Id => JSONValue::String(jmap_id.to_jmap_string()),
                        JMAPMailProperties::BlobId => JSONValue::String(
                            BlobId {
                                account: request.account_id,
                                collection: JMAP_MAIL,
                                document: document_id,
                                blob_index: MESSAGE_RAW,
                            }
                            .to_jmap_string(),
                        ),
                        JMAPMailProperties::ThreadId => {
                            JSONValue::String((jmap_id.get_thread_id() as JMAPId).to_jmap_string())
                        }
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
                                                (mailbox_id as JMAPId).to_jmap_string(),
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
                                                    Tag::Static(_) => "todo!()".to_string(), //TODO map static keywords
                                                    Tag::Id(_) => "todo!()".to_string(),
                                                    Tag::Text(text) => text.to_string(),
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
                        JMAPMailProperties::Size
                        | JMAPMailProperties::ReceivedAt
                        | JMAPMailProperties::HasAttachment => {
                            message_data.properties.remove(property).unwrap_or_default()
                        }
                        JMAPMailProperties::TextBody => add_body_parts(
                            request.account_id,
                            document_id,
                            &message_data.text_body,
                            &message_data.mime_parts,
                            &arguments.body_properties,
                            message_raw.as_ref(),
                            message_outline.as_ref(),
                        ),

                        JMAPMailProperties::HtmlBody => add_body_parts(
                            request.account_id,
                            document_id,
                            &message_data.html_body,
                            &message_data.mime_parts,
                            &arguments.body_properties,
                            message_raw.as_ref(),
                            message_outline.as_ref(),
                        ),

                        JMAPMailProperties::Attachments => add_body_parts(
                            request.account_id,
                            document_id,
                            &message_data.attachments,
                            &message_data.mime_parts,
                            &arguments.body_properties,
                            message_raw.as_ref(),
                            message_outline.as_ref(),
                        ),

                        JMAPMailProperties::Preview => {
                            if !message_data.text_body.is_empty() {
                                JSONValue::String(
                                    preview_text(
                                        String::from_utf8(
                                            self.get_blob(
                                                request.account_id,
                                                JMAP_MAIL,
                                                document_id,
                                                BlobEntry::new_range(
                                                    MESSAGE_PARTS
                                                        + message_data
                                                            .text_body
                                                            .get(0)
                                                            .and_then(|p| {
                                                                message_data.mime_parts.get(p + 1)
                                                            })
                                                            .ok_or(StoreError::DataCorruption)?
                                                            .blob_index,
                                                    0..260,
                                                ),
                                            )?
                                            .ok_or(StoreError::DataCorruption)?
                                            .value,
                                        )
                                        .map_or_else(
                                            |err| {
                                                String::from_utf8_lossy(err.as_bytes()).into_owned()
                                            },
                                            |s| s,
                                        )
                                        .into(),
                                        256,
                                    )
                                    .to_string(),
                                )
                            } else if !message_data.html_body.is_empty() {
                                JSONValue::String(
                                    preview_html(
                                        String::from_utf8(
                                            self.get_blob(
                                                request.account_id,
                                                JMAP_MAIL,
                                                document_id,
                                                BlobEntry::new(
                                                    MESSAGE_PARTS
                                                        + message_data
                                                            .html_body
                                                            .get(0)
                                                            .and_then(|p| {
                                                                message_data.mime_parts.get(p + 1)
                                                            })
                                                            .ok_or(StoreError::DataCorruption)?
                                                            .blob_index,
                                                ),
                                            )?
                                            .ok_or(StoreError::DataCorruption)?
                                            .value,
                                        )
                                        .map_or_else(
                                            |err| {
                                                String::from_utf8_lossy(err.as_bytes()).into_owned()
                                            },
                                            |s| s,
                                        )
                                        .into(),
                                        256,
                                    )
                                    .to_string(),
                                )
                            } else {
                                JSONValue::Null
                            }
                        }
                        JMAPMailProperties::BodyValues => {
                            let mut fetch_parts = BTreeMap::new();
                            if arguments.fetch_all_body_values || arguments.fetch_text_body_values {
                                message_data.text_body.iter().for_each(|part| {
                                    if let Some(mime_part) = message_data.mime_parts.get(*part + 1)
                                    {
                                        if let MimePartType::Html | MimePartType::Text =
                                            mime_part.mime_type
                                        {
                                            if let btree_map::Entry::Vacant(entry) = fetch_parts
                                                .entry(mime_part.blob_index + MESSAGE_PARTS)
                                            {
                                                entry.insert((mime_part, *part));
                                            }
                                        }
                                    }
                                });
                            }
                            if arguments.fetch_all_body_values || arguments.fetch_html_body_values {
                                message_data.html_body.iter().for_each(|part| {
                                    if let Some(mime_part) = message_data.mime_parts.get(*part + 1)
                                    {
                                        if let MimePartType::Html | MimePartType::Text =
                                            mime_part.mime_type
                                        {
                                            if let btree_map::Entry::Vacant(entry) = fetch_parts
                                                .entry(mime_part.blob_index + MESSAGE_PARTS)
                                            {
                                                entry.insert((mime_part, *part));
                                            }
                                        }
                                    }
                                });
                            }

                            if !fetch_parts.is_empty() {
                                JSONValue::Object(HashMap::from_iter(
                                    self.get_blobs(
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
                                        let (mime_part, part_id) =
                                            fetch_parts.get(&blob_entry.index).unwrap();
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
                                        let body_text = String::from_utf8(blob_entry.value)
                                            .map_or_else(
                                                |err| {
                                                    String::from_utf8_lossy(err.as_bytes())
                                                        .into_owned()
                                                },
                                                |s| s,
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
                                                            body_text.into(),
                                                            arguments.max_body_value_bytes,
                                                        )
                                                        .to_string()
                                                    } else {
                                                        truncate_text(
                                                            body_text.into(),
                                                            arguments.max_body_value_bytes,
                                                        )
                                                        .to_string()
                                                    },
                                                )
                                            },
                                        );

                                        (part_id.to_string(), JSONValue::Object(body_value))
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
                                message_outline.as_ref().unwrap(),
                                &message_data.mime_parts,
                                &arguments.body_properties,
                                message_raw.as_ref(),
                            ) {
                                body_structure
                            } else {
                                JSONValue::Null
                            }
                        }
                    };

                    if !value.is_null() {
                        entry.insert(value);
                    }
                }
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
    message_outline: &MessageOutline,
    mime_parts: &[MimePart<'x>],
    properties: &[JMAPMailBodyProperties<'y>],
    message_raw: Option<&Vec<u8>>,
) -> Option<JSONValue> {
    let mut parts_stack = Vec::with_capacity(5);
    let mut stack = Vec::new();

    let part_list = match &message_outline.body_structure {
        MessageStructure::Part(part_id) => {
            return Some(JSONValue::Object(add_body_part(
                account,
                document,
                (*part_id).into(),
                mime_parts.get(part_id + 1)?,
                properties,
                message_raw,
                message_outline.headers.get(0),
            )))
        }
        MessageStructure::List(part_list) => {
            parts_stack.push(add_body_part(
                account,
                document,
                None,
                mime_parts.get(0)?,
                properties,
                message_raw,
                message_outline.headers.get(0),
            ));
            part_list
        }
        MessageStructure::MultiPart((part_id, part_list)) => {
            parts_stack.push(add_body_part(
                account,
                document,
                None,
                mime_parts.get(0)?,
                properties,
                message_raw,
                message_outline.headers.get(0),
            ));
            parts_stack.push(add_body_part(
                account,
                document,
                None,
                mime_parts.get(part_id + 1)?,
                properties,
                message_raw,
                message_outline.headers.get(part_id + 1),
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
                    mime_parts.get(part_id + 1)?,
                    properties,
                    message_raw,
                    message_outline.headers.get(part_id + 1),
                ))),
                MessageStructure::MultiPart((part_id, next_part_list)) => {
                    parts_stack.push(add_body_part(
                        account,
                        document,
                        None,
                        mime_parts.get(part_id + 1)?,
                        properties,
                        message_raw,
                        message_outline.headers.get(part_id + 1),
                    ));
                    stack.push((part_list_iter, subparts));
                    part_list_iter = next_part_list.iter();
                    subparts = Vec::with_capacity(part_list.len());
                }
                MessageStructure::List(_) => (),
            }
        }

        if let Some((prev_part_list_iter, mut prev_subparts)) = stack.pop() {
            let mut prev_part = parts_stack.pop().unwrap();
            prev_part.insert("subparts".into(), JSONValue::Array(subparts));
            prev_subparts.push(JSONValue::Object(prev_part));
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
    message_outline: Option<&MessageOutline>,
) -> JSONValue {
    JSONValue::Array(
        parts
            .iter()
            .filter_map(|part_id| {
                Some(JSONValue::Object(add_body_part(
                    account,
                    document,
                    (*part_id).into(),
                    mime_parts.get(part_id + 1)?,
                    properties,
                    message_raw,
                    if let Some(message_outline) = message_outline {
                        message_outline.headers.get(part_id + 1)
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
    mime_part: &MimePart<'x>,
    properties: &[JMAPMailBodyProperties<'y>],
    message_raw: Option<&Vec<u8>>,
    headers_raw: Option<&RawHeaders<'y>>,
) -> HashMap<String, JSONValue> {
    let mut body_part = HashMap::with_capacity(properties.len());
    let mut headers_result: HashMap<String, Vec<JSONValue>> = HashMap::new();
    let has_raw_headers = headers_raw.is_some();

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
                            },
                            |str| str.trim().to_string(),
                        ),
                ))
            })
            .collect::<Vec<JSONValue>>()
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
                if let Some(value) = mime_part.headers.get(property) {
                    body_part.insert(property.to_string(), value.clone());
                }
            }

            JMAPMailBodyProperties::BlobId if part_id.is_some() => {
                body_part.insert(
                    "blobId".into(),
                    JSONValue::String(
                        BlobId::new(
                            account,
                            JMAP_MAIL,
                            document,
                            MESSAGE_PARTS + mime_part.blob_index,
                        )
                        .to_jmap_string(),
                    ),
                );
            }
            JMAPMailBodyProperties::Header(header) if has_raw_headers => {
                if let Some(offsets) = headers_raw.unwrap().get(&header.header) {
                    body_part.insert(
                        header.to_string(),
                        add_raw_header(
                            offsets,
                            message_raw.as_ref().unwrap(),
                            header.form.clone(),
                            header.all,
                        ),
                    );
                }
            }
            JMAPMailBodyProperties::Headers if has_raw_headers => {
                for (header, value) in headers_raw.unwrap() {
                    if let Entry::Vacant(entry) = headers_result.entry(header.as_str().to_string())
                    {
                        entry.insert(get_raw_header(value));
                    }
                }
            }
            JMAPMailBodyProperties::PartId => {
                if let Some(part_id) = part_id {
                    body_part.insert("partId".into(), JSONValue::Number(part_id as i64));
                }
            }
            _ => (),
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
                            let mut result: HashMap<String, JSONValue> = HashMap::with_capacity(2);
                            result.insert("name".into(), JSONValue::String((&header).clone()));
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

fn add_rfc_header(
    message_headers: &mut JMAPMailHeaders,
    header: RfcHeader,
    form: JMAPMailHeaderForm,
    all: bool,
) -> jmap_store::Result<JSONValue> {
    let (value, is_collection, is_grouped) = match &form {
        JMAPMailHeaderForm::Addresses | JMAPMailHeaderForm::GroupedAddresses => {
            if let Some(value) = message_headers.remove(&JMAPMailProperties::Header(
                JMAPMailHeaderProperty::new_rfc(header, JMAPMailHeaderForm::Addresses, false),
            )) {
                (value, false, false)
            } else if let Some(value) = message_headers.remove(&JMAPMailProperties::Header(
                JMAPMailHeaderProperty::new_rfc(header, JMAPMailHeaderForm::Addresses, true),
            )) {
                (value, true, false)
            } else if let Some(value) = message_headers.remove(&JMAPMailProperties::Header(
                JMAPMailHeaderProperty::new_rfc(
                    header,
                    JMAPMailHeaderForm::GroupedAddresses,
                    false,
                ),
            )) {
                (value, false, true)
            } else if let Some(value) = message_headers.remove(&JMAPMailProperties::Header(
                JMAPMailHeaderProperty::new_rfc(header, JMAPMailHeaderForm::GroupedAddresses, true),
            )) {
                (value, true, true)
            } else {
                (JSONValue::Null, false, false)
            }
        }
        _ => {
            if let Some(value) = message_headers.remove(&JMAPMailProperties::Header(
                JMAPMailHeaderProperty::new_rfc(header, form.clone(), all),
            )) {
                (value, all, false)
            } else if let Some(value) = message_headers.remove(&JMAPMailProperties::Header(
                JMAPMailHeaderProperty::new_rfc(header, form.clone(), !all),
            )) {
                (value, !all, false)
            } else {
                (JSONValue::Null, false, false)
            }
        }
    };

    Ok(match (header, form.clone()) {
        (RfcHeader::Date | RfcHeader::ResentDate, JMAPMailHeaderForm::Date)
        | (
            RfcHeader::Subject | RfcHeader::Comments | RfcHeader::Keywords | RfcHeader::ListId,
            JMAPMailHeaderForm::Text,
        ) => transform_json_string(value, all),
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
        ) => transform_json_stringlist(value, is_collection, all),
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
            value,
            is_grouped,
            is_collection,
            matches!(form, JMAPMailHeaderForm::GroupedAddresses),
            all,
        ),
        _ => return Err(JMAPError::InvalidArguments),
    })
}

fn add_raw_header(
    offsets: &[HeaderOffset],
    message_raw: &[u8],
    form: JMAPMailHeaderForm,
    all: bool,
) -> JSONValue {
    let mut header_values: Vec<HeaderValue> = offsets
        .iter()
        .skip(if !all && offsets.len() > 1 {
            offsets.len() - 1
        } else {
            0
        })
        .map(|offset| {
            (message_raw.get(offset.start..offset.end).map_or(
                HeaderValue::Empty,
                |bytes| match form {
                    JMAPMailHeaderForm::Raw => {
                        HeaderValue::Text(std::str::from_utf8(bytes).map_or_else(
                            |_| String::from_utf8_lossy(bytes).trim().to_string().into(),
                            |str| str.trim().to_string().into(),
                        ))
                    }
                    JMAPMailHeaderForm::Text => parse_unstructured(&mut MessageStream::new(bytes)),
                    JMAPMailHeaderForm::Addresses => parse_address(&mut MessageStream::new(bytes)),
                    JMAPMailHeaderForm::GroupedAddresses => {
                        parse_address(&mut MessageStream::new(bytes))
                    }
                    JMAPMailHeaderForm::MessageIds => parse_id(&mut MessageStream::new(bytes)),
                    JMAPMailHeaderForm::Date => parse_date(&mut MessageStream::new(bytes)),
                    JMAPMailHeaderForm::URLs => parse_address(&mut MessageStream::new(bytes)),
                },
            ))
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
            let (value, _) = header_to_jmap_text(header_values);
            value
        }
        JMAPMailHeaderForm::Addresses | JMAPMailHeaderForm::GroupedAddresses => {
            let (value, is_grouped, is_collection) = header_to_jmap_address(header_values, false);
            transform_json_emailaddress(
                value,
                is_grouped,
                is_collection,
                matches!(form, JMAPMailHeaderForm::GroupedAddresses),
                all,
            )
        }
        JMAPMailHeaderForm::MessageIds => {
            let (value, _) = header_to_jmap_id(header_values);
            value
        }
        JMAPMailHeaderForm::Date => {
            let (value, _) = header_to_jmap_date(header_values);
            value
        }
        JMAPMailHeaderForm::URLs => {
            let (value, _) = header_to_jmap_url(header_values);
            value
        }
    }
}

pub fn transform_json_emailaddress(
    value: JSONValue,
    is_grouped: bool,
    is_collection: bool,
    as_grouped: bool,
    as_collection: bool,
) -> JSONValue {
    if let JSONValue::Array(mut list) = value {
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
                let list_to_group = |list: Vec<JSONValue>| -> JSONValue {
                    let mut group = HashMap::new();
                    group.insert("name".to_string(), JSONValue::Null);
                    group.insert("addresses".to_string(), JSONValue::Array(list));
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
                let flatten_group = |list: Vec<JSONValue>| -> Vec<JSONValue> {
                    let mut addresses = Vec::with_capacity(list.len() * 2);
                    list.into_iter().for_each(|group| {
                        if let JSONValue::Object(mut group) = group {
                            if let Some(JSONValue::Array(mut group_addresses)) =
                                group.remove("addresses")
                            {
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

pub fn transform_json_stringlist(
    value: JSONValue,
    is_collection: bool,
    as_collection: bool,
) -> JSONValue {
    if let JSONValue::Array(mut list) = value {
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

pub fn transform_json_string(value: JSONValue, as_collection: bool) -> JSONValue {
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

    #[test]
    fn test_json_transform() {
        for (value, expected_result, expected_result_all) in [
            (
                JSONValue::String("hello".into()),
                JSONValue::String("hello".into()),
                JSONValue::Array(vec![JSONValue::String("hello".into())]),
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

        for (value, is_collection, expected_result, expected_result_all) in [
            (
                JSONValue::Array(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ]),
                false,
                JSONValue::Array(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ]),
                JSONValue::Array(vec![JSONValue::Array(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ])]),
            ),
            (
                JSONValue::Array(vec![
                    JSONValue::Array(vec![
                        JSONValue::String("hello".into()),
                        JSONValue::String("world".into()),
                    ]),
                    JSONValue::Array(vec![
                        JSONValue::String("hola".into()),
                        JSONValue::String("mundo".into()),
                    ]),
                ]),
                true,
                JSONValue::Array(vec![
                    JSONValue::String("hola".into()),
                    JSONValue::String("mundo".into()),
                ]),
                JSONValue::Array(vec![
                    JSONValue::Array(vec![
                        JSONValue::String("hello".into()),
                        JSONValue::String("world".into()),
                    ]),
                    JSONValue::Array(vec![
                        JSONValue::String("hola".into()),
                        JSONValue::String("mundo".into()),
                    ]),
                ]),
            ),
        ] {
            assert_eq!(
                super::transform_json_stringlist(value.clone(), is_collection, false),
                expected_result
            );
            assert_eq!(
                super::transform_json_stringlist(value, is_collection, true),
                expected_result_all
            );
        }

        fn make_email(name: &str, addr: &str) -> JSONValue {
            let mut email = HashMap::new();
            email.insert("name".to_string(), JSONValue::String(name.to_string()));
            email.insert("email".to_string(), JSONValue::String(addr.to_string()));
            JSONValue::Object(email)
        }

        fn make_group(name: Option<&str>, addresses: JSONValue) -> JSONValue {
            let mut email = HashMap::new();
            email.insert(
                "name".to_string(),
                name.map_or(JSONValue::Null, |name| JSONValue::String(name.to_string())),
            );
            email.insert("addresses".to_string(), addresses);
            JSONValue::Object(email)
        }

        fn make_list(value1: JSONValue, value2: JSONValue) -> JSONValue {
            JSONValue::Array(vec![value1, value2])
        }

        fn make_list_many(
            value1: JSONValue,
            value2: JSONValue,
            value3: JSONValue,
            value4: JSONValue,
        ) -> JSONValue {
            JSONValue::Array(vec![value1, value2, value3, value4])
        }

        fn make_list_single(value: JSONValue) -> JSONValue {
            JSONValue::Array(vec![value])
        }

        for (
            value,
            is_grouped,
            is_collection,
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
                false,
                false,
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
                false,
                true,
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
                true,
                false,
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
                true,
                true,
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
                super::transform_json_emailaddress(
                    value.clone(),
                    is_grouped,
                    is_collection,
                    false,
                    false
                ),
                expected_result_single_addr,
                "single+address"
            );
            assert_eq!(
                super::transform_json_emailaddress(
                    value.clone(),
                    is_grouped,
                    is_collection,
                    false,
                    true
                ),
                expected_result_all_addr,
                "all+address"
            );
            assert_eq!(
                super::transform_json_emailaddress(
                    value.clone(),
                    is_grouped,
                    is_collection,
                    true,
                    false
                ),
                expected_result_single_group,
                "single+group"
            );
            assert_eq!(
                super::transform_json_emailaddress(
                    value.clone(),
                    is_grouped,
                    is_collection,
                    true,
                    true
                ),
                expected_result_all_group,
                "all+group"
            );
        }
    }
}
