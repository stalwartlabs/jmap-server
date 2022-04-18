use std::{
    collections::{hash_map::Entry, HashMap},
    iter::FromIterator,
    vec,
};

use jmap::{blob::JMAPBlobStore, request::ParseRequest};
use jmap::{
    id::{BlobId, JMAPIdSerialize},
    json::JSONValue,
    JMAPError,
};
use mail_parser::{
    decoders::html::{html_to_text, text_to_html},
    parsers::{
        fields::thread::thread_name,
        preview::{preview_html, preview_text},
    },
    Addr, ContentType, Group, HeaderValue, Message, MessageAttachment, MessagePart, RfcHeader,
    RfcHeaders,
};
use nlp::lang::{LanguageDetector, MIN_LANGUAGE_SCORE};
use store::chrono::{LocalResult, SecondsFormat, TimeZone, Utc};
use store::{
    batch::{Document, MAX_ID_LENGTH, MAX_SORT_FIELD_LENGTH, MAX_TOKEN_LENGTH},
    blob::BlobIndex,
    field::{DefaultOptions, Options, Text},
    leb128::Leb128,
    serialize::StoreSerialize,
    AccountId, Integer, JMAPStore, LongInteger, Store, StoreError, Tag,
};

use crate::mail::{
    get::{
        add_body_parts, add_body_structure, add_body_value, add_raw_header,
        transform_json_emailaddress, transform_json_string, transform_json_stringlist,
        transform_rfc_header, MailGetArguments,
    },
    HeaderName, JMAPMailMimeHeaders, MailBodyProperties, MailHeaderForm, MailHeaderProperty,
    MailProperties, MessageData, MessageField, MessageOutline, MimePart, MimePartType,
    MESSAGE_DATA, MESSAGE_PARTS, MESSAGE_RAW,
};

pub struct MailParseRequest {
    pub account_id: AccountId,
    pub blob_ids: Vec<BlobId>,
    pub properties: Vec<MailProperties>,
    pub arguments: MailGetArguments,
}

pub trait JMAPMailParse {
    fn mail_parse(&self, request: ParseRequest) -> jmap::Result<JSONValue>;
}

impl<T> JMAPMailParse for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_parse(&self, mut request: ParseRequest) -> jmap::Result<JSONValue> {
        let mut parsed = HashMap::new();
        let mut not_parsable = Vec::new();
        let mut not_found = Vec::new();

        let request = MailParseRequest {
            account_id: request.account_id,
            blob_ids: request
                .arguments
                .remove("blobIds")
                .ok_or_else(|| {
                    JMAPError::InvalidArguments("Missing blobIds property.".to_string())
                })?
                .parse_array_items::<BlobId>(false)?
                .unwrap(),
            properties: request
                .arguments
                .remove("properties")
                .unwrap_or_default()
                .parse_array_items(true)?
                .unwrap_or_else(|| {
                    vec![
                        MailProperties::MessageId,
                        MailProperties::InReplyTo,
                        MailProperties::References,
                        MailProperties::Sender,
                        MailProperties::From,
                        MailProperties::To,
                        MailProperties::Cc,
                        MailProperties::Bcc,
                        MailProperties::ReplyTo,
                        MailProperties::Subject,
                        MailProperties::SentAt,
                        MailProperties::HasAttachment,
                        MailProperties::Preview,
                        MailProperties::BodyValues,
                        MailProperties::TextBody,
                        MailProperties::HtmlBody,
                        MailProperties::Attachments,
                    ]
                }),
            arguments: MailGetArguments::parse_arguments(request.arguments)?,
        };

        if request.blob_ids.len() > self.config.mail_parse_max_items {
            return Err(JMAPError::RequestTooLarge);
        }

        for blob_id in &request.blob_ids {
            if let Some(raw_message) =
                self.download_blob(request.account_id, blob_id, get_message_blob)?
            {
                if let Some(message) = Message::parse(&raw_message) {
                    parsed.insert(
                        blob_id.to_jmap_string(),
                        build_message_response(message, &raw_message, blob_id, &request),
                    );
                } else {
                    not_parsable.push(blob_id.to_jmap_string().into());
                }
            } else {
                not_found.push(blob_id.to_jmap_string().into());
            }
        }

        let mut result = HashMap::new();
        result.insert(
            "parsed".to_string(),
            if !parsed.is_empty() {
                parsed.into()
            } else {
                JSONValue::Null
            },
        );
        result.insert(
            "notParsable".to_string(),
            if !not_parsable.is_empty() {
                not_parsable.into()
            } else {
                JSONValue::Null
            },
        );
        result.insert(
            "notFound".to_string(),
            if !not_found.is_empty() {
                not_found.into()
            } else {
                JSONValue::Null
            },
        );
        Ok(result.into())
    }
}

pub fn get_message_blob(raw_message: &[u8], blob_index: BlobIndex) -> Option<Vec<u8>> {
    let message = Message::parse(raw_message)?;
    let mut blob_pos = 0;

    for (part_id, part) in message.parts.into_iter().enumerate() {
        match part {
            MessagePart::Html(html) => {
                if message.text_body.contains(&part_id) {
                    if blob_index == blob_pos {
                        return Some(html_to_text(html.body.as_ref()).into_bytes());
                    } else {
                        blob_pos += 1;
                    }
                }
                if blob_index == blob_pos {
                    return Some(html.body.into_owned().into_bytes());
                } else {
                    blob_pos += 1;
                }
            }
            MessagePart::Text(text) => {
                if message.html_body.contains(&part_id) {
                    if blob_index == blob_pos {
                        return Some(text_to_html(text.body.as_ref()).into_bytes());
                    } else {
                        blob_pos += 1;
                    }
                }
                if blob_index == blob_pos {
                    return Some(text.body.into_owned().into_bytes());
                } else {
                    blob_pos += 1;
                }
            }
            MessagePart::Binary(binary) | MessagePart::InlineBinary(binary) => {
                if blob_index == blob_pos {
                    return Some(binary.body.into_owned());
                } else {
                    blob_pos += 1;
                }
            }
            MessagePart::Message(nested_message) => {
                if blob_index == blob_pos {
                    return Some(match nested_message.body {
                        MessageAttachment::Parsed(message) => message.raw_message.into_owned(),
                        MessageAttachment::Raw(raw_message) => raw_message.into_owned(),
                    });
                } else {
                    blob_pos += 1;
                }
            }
            MessagePart::Multipart(_) => (),
        };
    }

    None
}

fn build_message_response(
    mut message: Message,
    raw_message: &[u8],
    blob_id: &BlobId,
    request: &MailParseRequest,
) -> JSONValue {
    let mut total_parts = message.parts.len();
    let mut mime_parts = Vec::with_capacity(total_parts + 1);
    let mut html_body = message.html_body;
    let mut text_body = message.text_body;
    let attachments = message.attachments;
    let mut message_outline = MessageOutline {
        body_offset: message.offset_body,
        body_structure: message.structure,
        headers: Vec::with_capacity(total_parts + 1),
        received_at: 0,
    };
    let mut has_attachments = false;

    let base_blob_id = BlobId::new_inner(blob_id.clone(), 0).unwrap_or_else(|| blob_id.clone());

    // Add MIME headers
    {
        let mut mime_headers = HashMap::with_capacity(5);

        for header_name in [
            RfcHeader::ContentType,
            RfcHeader::ContentDisposition,
            RfcHeader::ContentId,
            RfcHeader::ContentLanguage,
            RfcHeader::ContentLocation,
        ] {
            if let Some(header_value) = message.headers_rfc.remove(&header_name) {
                mime_header_to_jmap(&mut mime_headers, header_name, header_value);
            }
        }
        mime_parts.push(MimePart::new_other(mime_headers, 0, false));
    }

    message_outline.headers.push(
        message
            .headers_raw
            .into_iter()
            .map(|(k, v)| (k.into(), v))
            .collect(),
    );

    let mut extra_mime_parts = Vec::new();
    let mut blobs = Vec::new();

    // Extract blobs and build parts list
    for (part_id, part) in message.parts.into_iter().enumerate() {
        match part {
            MessagePart::Html(html) => {
                if let Some(pos) = text_body.iter().position(|&p| p == part_id) {
                    text_body[pos] = total_parts;
                    let value = html_to_text(html.body.as_ref()).into_bytes();
                    extra_mime_parts.push(MimePart::new_text(
                        empty_text_mime_headers(false, value.len()),
                        blobs.len() as BlobIndex,
                        false,
                    ));
                    blobs.push(value);
                    total_parts += 1;
                } else if !html_body.contains(&part_id) {
                    has_attachments = true;
                }
                mime_parts.push(MimePart::new_html(
                    mime_parts_to_jmap(html.headers_rfc, html.body.len()),
                    blobs.len() as BlobIndex,
                    html.is_encoding_problem,
                ));
                blobs.push(html.body.into_owned().into_bytes());
                message_outline.headers.push(
                    html.headers_raw
                        .into_iter()
                        .map(|(k, v)| (k.into(), v))
                        .collect(),
                );
            }
            MessagePart::Text(text) => {
                if let Some(pos) = html_body.iter().position(|&p| p == part_id) {
                    let value = text_to_html(text.body.as_ref());
                    let value_len = value.len();
                    extra_mime_parts.push(MimePart::new_html(
                        empty_text_mime_headers(true, value_len),
                        blobs.len() as BlobIndex,
                        false,
                    ));
                    blobs.push(value.into_bytes());
                    html_body[pos] = total_parts;
                    total_parts += 1;
                } else if !text_body.contains(&part_id) {
                    has_attachments = true;
                }
                mime_parts.push(MimePart::new_text(
                    mime_parts_to_jmap(text.headers_rfc, text.body.len()),
                    blobs.len() as BlobIndex,
                    text.is_encoding_problem,
                ));
                blobs.push(text.body.into_owned().into_bytes());
                message_outline.headers.push(
                    text.headers_raw
                        .into_iter()
                        .map(|(k, v)| (k.into(), v))
                        .collect(),
                );
            }
            MessagePart::Binary(binary) => {
                if !has_attachments {
                    has_attachments = true;
                }
                mime_parts.push(MimePart::new_other(
                    mime_parts_to_jmap(binary.headers_rfc, binary.body.len()),
                    blobs.len() as BlobIndex,
                    binary.is_encoding_problem,
                ));
                blobs.push(binary.body.into_owned());
                message_outline.headers.push(
                    binary
                        .headers_raw
                        .into_iter()
                        .map(|(k, v)| (k.into(), v))
                        .collect(),
                );
            }
            MessagePart::InlineBinary(binary) => {
                mime_parts.push(MimePart::new_other(
                    mime_parts_to_jmap(binary.headers_rfc, binary.body.len()),
                    blobs.len() as BlobIndex,
                    binary.is_encoding_problem,
                ));
                blobs.push(binary.body.into_owned());
                message_outline.headers.push(
                    binary
                        .headers_raw
                        .into_iter()
                        .map(|(k, v)| (k.into(), v))
                        .collect(),
                );
            }
            MessagePart::Message(nested_message) => {
                if !has_attachments {
                    has_attachments = true;
                }
                let blob_index = blobs.len();
                mime_parts.push(MimePart::new_other(
                    mime_parts_to_jmap(
                        nested_message.headers_rfc,
                        match nested_message.body {
                            MessageAttachment::Parsed(message) => {
                                let message_size = message.raw_message.len();
                                blobs.push(message.raw_message.into_owned());
                                message_size
                            }
                            MessageAttachment::Raw(raw_message) => {
                                let message_size = raw_message.len();
                                blobs.push(raw_message.into_owned());
                                message_size
                            }
                        },
                    ),
                    blob_index as BlobIndex,
                    false,
                ));
                message_outline.headers.push(
                    nested_message
                        .headers_raw
                        .into_iter()
                        .map(|(k, v)| (k.into(), v))
                        .collect(),
                );
            }
            MessagePart::Multipart(part) => {
                mime_parts.push(MimePart::new_other(
                    mime_parts_to_jmap(part.headers_rfc, 0),
                    0,
                    false,
                ));
                message_outline.headers.push(
                    part.headers_raw
                        .into_iter()
                        .map(|(k, v)| (k.into(), v))
                        .collect(),
                );
            }
        };
    }

    if !extra_mime_parts.is_empty() {
        mime_parts.append(&mut extra_mime_parts);
    }

    let mut result = HashMap::with_capacity(request.properties.len());

    for property in &request.properties {
        result.insert(
            property.to_string(),
            match property {
                MailProperties::Id
                | MailProperties::ThreadId
                | MailProperties::MailboxIds
                | MailProperties::ReceivedAt
                | MailProperties::Keywords => JSONValue::Null,

                MailProperties::BlobId => blob_id.to_jmap_string().into(),
                MailProperties::Size => raw_message.len().into(),
                MailProperties::MessageId
                | MailProperties::References
                | MailProperties::InReplyTo => {
                    if let Some(message_id) = message.headers_rfc.remove(&property.as_rfc_header())
                    {
                        let (value, is_collection) = header_to_jmap_id(message_id);
                        transform_json_stringlist(value, is_collection, false)
                    } else {
                        JSONValue::Null
                    }
                }
                MailProperties::Sender
                | MailProperties::From
                | MailProperties::To
                | MailProperties::Cc
                | MailProperties::Bcc
                | MailProperties::ReplyTo => {
                    if let Some(addr) = message.headers_rfc.remove(&property.as_rfc_header()) {
                        let (value, is_grouped, is_collection) =
                            header_to_jmap_address(addr, false);
                        transform_json_emailaddress(value, is_grouped, is_collection, false, false)
                    } else {
                        JSONValue::Null
                    }
                }
                MailProperties::Subject => {
                    if let Some(text) = message.headers_rfc.remove(&RfcHeader::Subject) {
                        let (value, _) = header_to_jmap_text(text);
                        transform_json_string(value, false)
                    } else {
                        JSONValue::Null
                    }
                }
                MailProperties::SentAt => {
                    if let Some(date) = message.headers_rfc.remove(&RfcHeader::Date) {
                        let (value, _) = header_to_jmap_date(date);
                        transform_json_string(value, false)
                    } else {
                        JSONValue::Null
                    }
                }
                MailProperties::Header(MailHeaderProperty {
                    form: form @ MailHeaderForm::Raw,
                    header,
                    all,
                })
                | MailProperties::Header(MailHeaderProperty {
                    form,
                    header: header @ HeaderName::Other(_),
                    all,
                }) => {
                    if let Some(offsets) = message_outline
                        .headers
                        .get_mut(0)
                        .and_then(|l| l.remove(header))
                    {
                        add_raw_header(&offsets, raw_message, form.clone(), *all)
                    } else {
                        JSONValue::Null
                    }
                }
                MailProperties::Header(MailHeaderProperty {
                    form,
                    header: HeaderName::Rfc(header),
                    all,
                }) => {
                    if let Some(header_value) = message.headers_rfc.remove(header) {
                        let (header_value, is_grouped, is_collection) = match header {
                            RfcHeader::MessageId
                            | RfcHeader::InReplyTo
                            | RfcHeader::References
                            | RfcHeader::ResentMessageId => {
                                let (header_value, is_collection) = header_to_jmap_id(header_value);
                                (header_value, false, is_collection)
                            }
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
                            | RfcHeader::ResentSender => {
                                header_to_jmap_address(header_value, false)
                            }
                            RfcHeader::Date | RfcHeader::ResentDate => {
                                let (header_value, is_collection) =
                                    header_to_jmap_date(header_value);
                                (header_value, false, is_collection)
                            }
                            RfcHeader::ListArchive
                            | RfcHeader::ListHelp
                            | RfcHeader::ListOwner
                            | RfcHeader::ListPost
                            | RfcHeader::ListSubscribe
                            | RfcHeader::ListUnsubscribe => {
                                let (header_value, is_collection) =
                                    header_to_jmap_url(header_value);
                                (header_value, false, is_collection)
                            }
                            RfcHeader::Subject => {
                                let (header_value, is_collection) =
                                    header_to_jmap_text(header_value);
                                (header_value, false, is_collection)
                            }
                            RfcHeader::Comments | RfcHeader::Keywords | RfcHeader::ListId => {
                                let (header_value, is_collection) =
                                    header_to_jmap_text(header_value);
                                (header_value, false, is_collection)
                            }
                            _ => (JSONValue::Null, false, false),
                        };

                        transform_rfc_header(
                            *header,
                            header_value,
                            form.clone(),
                            is_collection,
                            is_grouped,
                            *all,
                        )
                        .unwrap_or(JSONValue::Null)
                    } else {
                        JSONValue::Null
                    }
                }
                MailProperties::HasAttachment => has_attachments.into(),
                MailProperties::Preview => {
                    if !text_body.is_empty() {
                        preview_text(
                            String::from_utf8_lossy(
                                &blobs[text_body
                                    .get(0)
                                    .and_then(|p| mime_parts.get(p + 1))
                                    .unwrap()
                                    .blob_index as usize],
                            ),
                            256,
                        )
                        .to_string()
                        .into()
                    } else if !html_body.is_empty() {
                        preview_html(
                            String::from_utf8_lossy(
                                &blobs[html_body
                                    .get(0)
                                    .and_then(|p| mime_parts.get(p + 1))
                                    .unwrap()
                                    .blob_index as usize],
                            ),
                            256,
                        )
                        .to_string()
                        .into()
                    } else {
                        JSONValue::Null
                    }
                }
                MailProperties::BodyValues => {
                    let mut fetch_parts = Vec::new();
                    if request.arguments.fetch_all_body_values
                        || request.arguments.fetch_text_body_values
                    {
                        text_body.iter().for_each(|part| {
                            if let Some(mime_part) = mime_parts.get(*part + 1) {
                                if let MimePartType::Html | MimePartType::Text = mime_part.mime_type
                                {
                                    fetch_parts.push((mime_part, *part));
                                }
                            }
                        });
                    }
                    if request.arguments.fetch_all_body_values
                        || request.arguments.fetch_html_body_values
                    {
                        html_body.iter().for_each(|part| {
                            if let Some(mime_part) = mime_parts.get(*part + 1) {
                                if let MimePartType::Html | MimePartType::Text = mime_part.mime_type
                                {
                                    fetch_parts.push((mime_part, *part));
                                }
                            }
                        });
                    }

                    if !fetch_parts.is_empty() {
                        JSONValue::Object(HashMap::from_iter(fetch_parts.into_iter().map(
                            |(mime_part, part_id)| {
                                (
                                    part_id.to_string(),
                                    add_body_value(
                                        mime_part,
                                        String::from_utf8_lossy(
                                            &blobs[mime_part.blob_index as usize],
                                        )
                                        .into_owned(),
                                        &request.arguments,
                                    ),
                                )
                            },
                        )))
                    } else {
                        JSONValue::Null
                    }
                }
                MailProperties::TextBody => add_body_parts(
                    &text_body,
                    &mime_parts,
                    &request.arguments.body_properties,
                    Some(raw_message),
                    Some(&message_outline),
                    &base_blob_id,
                ),

                MailProperties::HtmlBody => add_body_parts(
                    &html_body,
                    &mime_parts,
                    &request.arguments.body_properties,
                    Some(raw_message),
                    Some(&message_outline),
                    &base_blob_id,
                ),

                MailProperties::Attachments => add_body_parts(
                    &attachments,
                    &mime_parts,
                    &request.arguments.body_properties,
                    Some(raw_message),
                    Some(&message_outline),
                    &base_blob_id,
                ),

                MailProperties::BodyStructure => {
                    if let Some(body_structure) = add_body_structure(
                        &message_outline,
                        &mime_parts,
                        &request.arguments.body_properties,
                        Some(raw_message),
                        &base_blob_id,
                    ) {
                        body_structure
                    } else {
                        JSONValue::Null
                    }
                }
            },
        );
    }

    JSONValue::Object(result)
}

pub fn build_message_document(
    document: &mut Document,
    raw_message: Vec<u8>,
    received_at: Option<i64>,
) -> store::Result<(Vec<String>, String)> {
    let message = Message::parse(&raw_message).ok_or_else(|| {
        StoreError::InvalidArguments("Failed to parse e-mail message.".to_string())
    })?;
    let mut total_parts = message.parts.len();
    let mut total_blobs = 0;
    let mut message_data = MessageData {
        properties: HashMap::with_capacity(message.headers_rfc.len() + 3),
        mime_parts: Vec::with_capacity(total_parts + 1),
        html_body: message.html_body,
        text_body: message.text_body,
        attachments: message.attachments,
    };
    let mut message_outline = MessageOutline {
        body_offset: message.offset_body,
        body_structure: message.structure,
        headers: Vec::with_capacity(total_parts + 1),
        received_at: received_at.unwrap_or_else(|| Utc::now().timestamp()),
    };
    let mut language_detector = LanguageDetector::new();
    let mut has_attachments = false;

    message_data
        .properties
        .insert(MailProperties::Size, message.raw_message.len().into());

    document.number(
        MessageField::Size,
        message.raw_message.len() as Integer,
        DefaultOptions::new().sort(),
    );

    message_data.properties.insert(
        MailProperties::ReceivedAt,
        if let LocalResult::Single(received_at) = Utc.timestamp_opt(message_outline.received_at, 0)
        {
            JSONValue::String(received_at.to_rfc3339_opts(SecondsFormat::Secs, true))
        } else {
            JSONValue::Null
        },
    );

    document.number(
        MessageField::ReceivedAt,
        message_outline.received_at as LongInteger,
        DefaultOptions::new().sort(),
    );

    let mut reference_ids = Vec::new();
    let mut mime_parts = HashMap::with_capacity(5);
    let mut base_subject = None;

    for (header_name, header_value) in message.headers_rfc {
        // Add headers to document
        parse_header(document, header_name, &header_value);

        // Build JMAP headers
        match header_name {
            RfcHeader::MessageId
            | RfcHeader::InReplyTo
            | RfcHeader::References
            | RfcHeader::ResentMessageId => {
                // Build a list containing all IDs that appear in the header
                match &header_value {
                    HeaderValue::Text(text) if text.len() <= MAX_ID_LENGTH => {
                        reference_ids.push(text.to_string())
                    }
                    HeaderValue::TextList(list) => {
                        reference_ids.extend(list.iter().filter_map(|text| {
                            if text.len() <= MAX_ID_LENGTH {
                                Some(text.to_string())
                            } else {
                                None
                            }
                        }));
                    }
                    HeaderValue::Collection(col) => {
                        for item in col {
                            match item {
                                HeaderValue::Text(text) if text.len() <= MAX_ID_LENGTH => {
                                    reference_ids.push(text.to_string())
                                }
                                HeaderValue::TextList(list) => {
                                    reference_ids.extend(list.iter().filter_map(|text| {
                                        if text.len() <= MAX_ID_LENGTH {
                                            Some(text.to_string())
                                        } else {
                                            None
                                        }
                                    }))
                                }
                                _ => (),
                            }
                        }
                    }
                    _ => (),
                }
                let (value, is_collection) = header_to_jmap_id(header_value);
                message_data.properties.insert(
                    MailProperties::Header(MailHeaderProperty::new_rfc(
                        header_name,
                        MailHeaderForm::MessageIds,
                        is_collection,
                    )),
                    value,
                );
            }
            RfcHeader::From | RfcHeader::To | RfcHeader::Cc | RfcHeader::Bcc => {
                // Build sort index
                add_addr_sort(document, header_name, &header_value);
                let (value, is_grouped, is_collection) =
                    header_to_jmap_address(header_value, false);
                message_data.properties.insert(
                    MailProperties::Header(MailHeaderProperty::new_rfc(
                        header_name,
                        if is_grouped {
                            MailHeaderForm::GroupedAddresses
                        } else {
                            MailHeaderForm::Addresses
                        },
                        is_collection,
                    )),
                    value,
                );
            }
            RfcHeader::ReplyTo
            | RfcHeader::Sender
            | RfcHeader::ResentTo
            | RfcHeader::ResentFrom
            | RfcHeader::ResentBcc
            | RfcHeader::ResentCc
            | RfcHeader::ResentSender => {
                let (value, is_grouped, is_collection) =
                    header_to_jmap_address(header_value, false);
                message_data.properties.insert(
                    MailProperties::Header(MailHeaderProperty::new_rfc(
                        header_name,
                        if is_grouped {
                            MailHeaderForm::GroupedAddresses
                        } else {
                            MailHeaderForm::Addresses
                        },
                        is_collection,
                    )),
                    value,
                );
            }
            RfcHeader::Date | RfcHeader::ResentDate => {
                let (value, is_collection) = header_to_jmap_date(header_value);
                message_data.properties.insert(
                    MailProperties::Header(MailHeaderProperty::new_rfc(
                        header_name,
                        MailHeaderForm::Date,
                        is_collection,
                    )),
                    value,
                );
            }
            RfcHeader::ListArchive
            | RfcHeader::ListHelp
            | RfcHeader::ListOwner
            | RfcHeader::ListPost
            | RfcHeader::ListSubscribe
            | RfcHeader::ListUnsubscribe => {
                let (value, is_collection) = header_to_jmap_url(header_value);
                message_data.properties.insert(
                    MailProperties::Header(MailHeaderProperty::new_rfc(
                        header_name,
                        MailHeaderForm::URLs,
                        is_collection,
                    )),
                    value,
                );
            }
            RfcHeader::Subject => {
                if let HeaderValue::Text(subject) = &header_value {
                    let (thread_name, language) = match thread_name(subject) {
                        thread_name if !thread_name.is_empty() => (
                            thread_name.to_string(),
                            language_detector.detect(thread_name, MIN_LANGUAGE_SCORE),
                        ),
                        _ => (
                            "!".to_string(),
                            language_detector.detect(subject, MIN_LANGUAGE_SCORE),
                        ),
                    };

                    document.text(
                        RfcHeader::Subject,
                        Text::fulltext_lang(subject.to_string(), language),
                        DefaultOptions::new(),
                    );

                    base_subject = Some(thread_name);
                }
                let (value, is_collection) = header_to_jmap_text(header_value);
                message_data.properties.insert(
                    MailProperties::Header(MailHeaderProperty::new_rfc(
                        header_name,
                        MailHeaderForm::Text,
                        is_collection,
                    )),
                    value,
                );
            }
            RfcHeader::Comments | RfcHeader::Keywords | RfcHeader::ListId => {
                let (value, is_collection) = header_to_jmap_text(header_value);
                message_data.properties.insert(
                    MailProperties::Header(MailHeaderProperty::new_rfc(
                        header_name,
                        MailHeaderForm::Text,
                        is_collection,
                    )),
                    value,
                );
            }
            RfcHeader::ContentType
            | RfcHeader::ContentDisposition
            | RfcHeader::ContentId
            | RfcHeader::ContentLanguage
            | RfcHeader::ContentLocation => {
                mime_header_to_jmap(&mut mime_parts, header_name, header_value);
            }
            RfcHeader::ContentTransferEncoding
            | RfcHeader::ContentDescription
            | RfcHeader::MimeVersion
            | RfcHeader::Received
            | RfcHeader::ReturnPath => (),
        }
    }

    message_data
        .mime_parts
        .push(MimePart::new_other(mime_parts, 0, false));
    message_outline.headers.push(
        message
            .headers_raw
            .into_iter()
            .map(|(k, v)| (k.into(), v))
            .collect(),
    );

    let mut extra_mime_parts = Vec::new();

    for (part_id, part) in message.parts.into_iter().enumerate() {
        match part {
            MessagePart::Html(html) => {
                let text = html_to_text(html.body.as_ref());
                let text_len = text.len();
                let html_len = html.body.len();
                let field =
                    if let Some(pos) = message_data.text_body.iter().position(|&p| p == part_id) {
                        message_data.text_body[pos] = total_parts;
                        extra_mime_parts.push(MimePart::new_text(
                            empty_text_mime_headers(false, text_len),
                            total_blobs,
                            false,
                        ));
                        total_parts += 1;
                        MessageField::Body
                    } else if message_data.html_body.contains(&part_id) {
                        MessageField::Body
                    } else {
                        has_attachments = true;
                        MessageField::Attachment
                    };

                document.text(
                    field.clone(),
                    Text::fulltext(text, &mut language_detector),
                    if field == MessageField::Body {
                        let blob_index = total_blobs;
                        total_blobs += 1;
                        DefaultOptions::new().store_blob(blob_index + MESSAGE_PARTS)
                    } else {
                        DefaultOptions::new()
                    },
                );

                document.text(
                    field,
                    Text::not_indexed(html.body.into_owned()),
                    DefaultOptions::new().store_blob(total_blobs + MESSAGE_PARTS),
                );

                message_data.mime_parts.push(MimePart::new_html(
                    mime_parts_to_jmap(html.headers_rfc, html_len),
                    total_blobs,
                    html.is_encoding_problem,
                ));
                message_outline.headers.push(
                    html.headers_raw
                        .into_iter()
                        .map(|(k, v)| (k.into(), v))
                        .collect(),
                );

                total_blobs += 1;
            }
            MessagePart::Text(text) => {
                let text_len = text.body.len();
                let field =
                    if let Some(pos) = message_data.html_body.iter().position(|&p| p == part_id) {
                        let html = text_to_html(text.body.as_ref());
                        let html_len = html.len();
                        document.text(
                            MessageField::Body,
                            Text::not_indexed(html),
                            DefaultOptions::new().store_blob(total_blobs + MESSAGE_PARTS),
                        );
                        extra_mime_parts.push(MimePart::new_html(
                            empty_text_mime_headers(true, html_len),
                            total_blobs,
                            false,
                        ));
                        message_data.html_body[pos] = total_parts;
                        total_blobs += 1;
                        total_parts += 1;
                        MessageField::Body
                    } else if message_data.text_body.contains(&part_id) {
                        MessageField::Body
                    } else {
                        has_attachments = true;
                        MessageField::Attachment
                    };

                document.text(
                    field,
                    Text::fulltext(text.body.into_owned(), &mut language_detector),
                    DefaultOptions::new().store_blob(total_blobs + MESSAGE_PARTS),
                );

                message_data.mime_parts.push(MimePart::new_text(
                    mime_parts_to_jmap(text.headers_rfc, text_len),
                    total_blobs,
                    text.is_encoding_problem,
                ));
                message_outline.headers.push(
                    text.headers_raw
                        .into_iter()
                        .map(|(k, v)| (k.into(), v))
                        .collect(),
                );

                total_blobs += 1;
            }
            MessagePart::Binary(binary) => {
                if !has_attachments {
                    has_attachments = true;
                }
                message_data.mime_parts.push(MimePart::new_other(
                    mime_parts_to_jmap(binary.headers_rfc, binary.body.len()),
                    total_blobs,
                    binary.is_encoding_problem,
                ));
                message_outline.headers.push(
                    binary
                        .headers_raw
                        .into_iter()
                        .map(|(k, v)| (k.into(), v))
                        .collect(),
                );

                document.binary(
                    MessageField::Attachment,
                    binary.body.into_owned(),
                    DefaultOptions::new().store_blob(total_blobs + MESSAGE_PARTS),
                );
                total_blobs += 1;
            }
            MessagePart::InlineBinary(binary) => {
                message_data.mime_parts.push(MimePart::new_other(
                    mime_parts_to_jmap(binary.headers_rfc, binary.body.len()),
                    total_blobs,
                    binary.is_encoding_problem,
                ));
                message_outline.headers.push(
                    binary
                        .headers_raw
                        .into_iter()
                        .map(|(k, v)| (k.into(), v))
                        .collect(),
                );
                document.binary(
                    MessageField::Attachment,
                    binary.body.into_owned(),
                    DefaultOptions::new().store_blob(total_blobs + MESSAGE_PARTS),
                );
                total_blobs += 1;
            }
            MessagePart::Message(nested_message) => {
                if !has_attachments {
                    has_attachments = true;
                }
                message_data.mime_parts.push(MimePart::new_other(
                    mime_parts_to_jmap(
                        nested_message.headers_rfc,
                        match nested_message.body {
                            MessageAttachment::Parsed(mut message) => {
                                parse_attached_message(
                                    document,
                                    &mut message,
                                    &mut language_detector,
                                );
                                let message_size = message.raw_message.len();
                                document.binary(
                                    MessageField::Attachment,
                                    message.raw_message.into_owned(),
                                    DefaultOptions::new().store_blob(total_blobs + MESSAGE_PARTS),
                                );
                                message_size
                            }
                            MessageAttachment::Raw(raw_message) => {
                                if let Some(message) = &mut Message::parse(raw_message.as_ref()) {
                                    parse_attached_message(
                                        document,
                                        message,
                                        &mut language_detector,
                                    )
                                }
                                let message_size = raw_message.len();
                                document.binary(
                                    MessageField::Attachment,
                                    raw_message.into_owned(),
                                    DefaultOptions::new().store_blob(total_blobs + MESSAGE_PARTS),
                                );
                                message_size
                            }
                        },
                    ),
                    total_blobs,
                    false,
                ));
                total_blobs += 1;
                message_outline.headers.push(
                    nested_message
                        .headers_raw
                        .into_iter()
                        .map(|(k, v)| (k.into(), v))
                        .collect(),
                );
            }
            MessagePart::Multipart(part) => {
                message_data.mime_parts.push(MimePart::new_other(
                    mime_parts_to_jmap(part.headers_rfc, 0),
                    0,
                    false,
                ));
                message_outline.headers.push(
                    part.headers_raw
                        .into_iter()
                        .map(|(k, v)| (k.into(), v))
                        .collect(),
                );
            }
        };
    }

    if !extra_mime_parts.is_empty() {
        message_data.mime_parts.append(&mut extra_mime_parts);
    }

    message_data.properties.insert(
        MailProperties::HasAttachment,
        JSONValue::Bool(has_attachments),
    );

    if has_attachments {
        document.tag(MessageField::Attachment, Tag::Id(0), DefaultOptions::new());
    }

    document.binary(
        MessageField::Internal,
        raw_message,
        DefaultOptions::new().store_blob(MESSAGE_RAW),
    );

    let mut message_data = message_data
        .serialize()
        .ok_or_else(|| StoreError::SerializeError("Failed to serialize message data".into()))?;
    let mut message_outline = message_outline
        .serialize()
        .ok_or_else(|| StoreError::SerializeError("Failed to serialize message outline".into()))?;
    let mut buf = Vec::with_capacity(
        message_data.len() + message_outline.len() + std::mem::size_of::<usize>(),
    );
    message_data.len().to_leb128_bytes(&mut buf);
    buf.append(&mut message_data);
    buf.append(&mut message_outline);

    document.binary(
        MessageField::Internal,
        buf,
        DefaultOptions::new().store_blob(MESSAGE_DATA),
    );

    if let Some(default_language) = language_detector.most_frequent_language() {
        document.set_default_language(*default_language);
    }

    // TODO search by "header exists"
    // TODO use content language when available
    // TODO index PDF, Doc, Excel, etc.

    Ok((
        reference_ids,
        base_subject.unwrap_or_else(|| "!".to_string()),
    ))
}

fn parse_attached_message(
    document: &mut Document,
    message: &mut Message,
    language_detector: &mut LanguageDetector,
) {
    if let Some(HeaderValue::Text(subject)) = message.headers_rfc.remove(&RfcHeader::Subject) {
        document.text(
            MessageField::Attachment,
            Text::fulltext(subject.into_owned(), language_detector),
            DefaultOptions::new(),
        );
    }
    for part in message.parts.drain(..) {
        match part {
            MessagePart::Text(text) => {
                document.text(
                    MessageField::Attachment,
                    Text::fulltext(text.body.into_owned(), language_detector),
                    DefaultOptions::new(),
                );
            }
            MessagePart::Html(html) => {
                document.text(
                    MessageField::Attachment,
                    Text::fulltext(html_to_text(&html.body), language_detector),
                    DefaultOptions::new(),
                );
            }
            _ => (),
        }
    }
}

fn parse_address(document: &mut Document, header_name: RfcHeader, address: &Addr) {
    if let Some(name) = &address.name {
        parse_text(document, header_name, name);
    };
    if let Some(ref addr) = address.address {
        if addr.len() <= MAX_TOKEN_LENGTH {
            document.text(
                header_name,
                Text::keyword(addr.to_lowercase()),
                DefaultOptions::new(),
            );
        }
    };
}

fn parse_address_group(document: &mut Document, header_name: RfcHeader, group: &Group) {
    if let Some(name) = &group.name {
        parse_text(document, header_name, name);
    };

    for address in group.addresses.iter() {
        parse_address(document, header_name, address);
    }
}

fn parse_text(document: &mut Document, header_name: RfcHeader, text: &str) {
    match header_name {
        RfcHeader::Keywords
        | RfcHeader::ContentLanguage
        | RfcHeader::MimeVersion
        | RfcHeader::MessageId
        | RfcHeader::References
        | RfcHeader::ContentId
        | RfcHeader::ResentMessageId => {
            if text.len() <= MAX_TOKEN_LENGTH {
                document.text(
                    header_name,
                    Text::keyword(text.to_lowercase()),
                    DefaultOptions::new(),
                );
            }
        }

        RfcHeader::Subject => (),

        _ => {
            document.text(
                header_name,
                Text::tokenized(text.to_string()),
                DefaultOptions::new(),
            );
        }
    }
}

fn parse_content_type(document: &mut Document, header_name: RfcHeader, content_type: &ContentType) {
    if content_type.c_type.len() <= MAX_TOKEN_LENGTH {
        document.text(
            header_name,
            Text::keyword(content_type.c_type.to_string()),
            DefaultOptions::new(),
        );
    }
    if let Some(subtype) = &content_type.c_subtype {
        if subtype.len() <= MAX_TOKEN_LENGTH {
            document.text(
                header_name,
                Text::keyword(subtype.to_string()),
                DefaultOptions::new(),
            );
        }
    }
    if let Some(attributes) = &content_type.attributes {
        for (key, value) in attributes {
            if key == "name" || key == "filename" {
                document.text(
                    header_name,
                    Text::tokenized(value.to_string()),
                    DefaultOptions::new(),
                );
            } else if value.len() <= MAX_TOKEN_LENGTH {
                document.text(
                    header_name,
                    Text::keyword(value.to_lowercase()),
                    DefaultOptions::new(),
                );
            }
        }
    }
}

#[allow(clippy::manual_flatten)]
fn add_addr_sort(document: &mut Document, header_name: RfcHeader, header_value: &HeaderValue) {
    let sort_parts = match if let HeaderValue::Collection(ref col) = header_value {
        col.first().unwrap_or(&HeaderValue::Empty)
    } else {
        header_value
    } {
        HeaderValue::Address(addr) => [&None, &addr.name, &addr.address],
        HeaderValue::AddressList(list) => list.first().map_or([&None, &None, &None], |addr| {
            [&None, &addr.name, &addr.address]
        }),
        HeaderValue::Group(group) => group
            .addresses
            .first()
            .map_or([&group.name, &None, &None], |addr| {
                [&group.name, &addr.name, &addr.address]
            }),
        HeaderValue::GroupList(list) => list.first().map_or([&None, &None, &None], |group| {
            group
                .addresses
                .first()
                .map_or([&group.name, &None, &None], |addr| {
                    [&group.name, &addr.name, &addr.address]
                })
        }),
        _ => [&None, &None, &None],
    };
    let text_len = sort_parts
        .iter()
        .map(|part| part.as_ref().map_or(0, |s| s.len()))
        .sum::<usize>();
    if text_len > 0 {
        let mut text = String::with_capacity(if text_len > MAX_SORT_FIELD_LENGTH {
            MAX_SORT_FIELD_LENGTH
        } else {
            text_len
        });
        'outer: for part in sort_parts {
            if let Some(s) = part {
                if !text.is_empty() {
                    text.push(' ');
                }

                for ch in s.chars() {
                    for ch in ch.to_lowercase() {
                        if text.len() >= MAX_SORT_FIELD_LENGTH {
                            break 'outer;
                        }
                        text.push(ch);
                    }
                }
            }
        }
        document.text(
            header_name,
            Text::tokenized(text),
            DefaultOptions::new().sort(),
        );
    };
}

fn parse_header(document: &mut Document, header_name: RfcHeader, header_value: &HeaderValue) {
    match header_value {
        HeaderValue::Address(address) => {
            parse_address(document, header_name, address);
        }
        HeaderValue::AddressList(address_list) => {
            for item in address_list {
                parse_address(document, header_name, item);
            }
        }
        HeaderValue::Group(group) => {
            parse_address_group(document, header_name, group);
        }
        HeaderValue::GroupList(group_list) => {
            for item in group_list {
                parse_address_group(document, header_name, item);
            }
        }
        HeaderValue::Text(text) => {
            parse_text(document, header_name, text);
        }
        HeaderValue::TextList(text_list) => {
            for item in text_list {
                parse_text(document, header_name, item);
            }
        }
        HeaderValue::DateTime(date_time) => {
            if date_time.is_valid() {
                document.number(
                    header_name,
                    date_time.to_timestamp().unwrap() as u64,
                    DefaultOptions::new().sort(),
                );
            }
        }
        HeaderValue::ContentType(content_type) => {
            parse_content_type(document, header_name, content_type);
        }
        HeaderValue::Collection(header_value) => {
            for item in header_value {
                parse_header(document, header_name, item);
            }
        }
        HeaderValue::Empty => (),
    }
}

pub fn header_to_jmap_date(header: HeaderValue) -> (JSONValue, bool) {
    match header {
        HeaderValue::DateTime(datetime) => (JSONValue::String(datetime.to_iso8601()), false),
        HeaderValue::Collection(list) => (
            JSONValue::Array(
                list.into_iter()
                    .filter_map(|datetime| {
                        if let HeaderValue::DateTime(datetime) = datetime {
                            Some(JSONValue::String(datetime.to_iso8601()))
                        } else {
                            None
                        }
                    })
                    .collect(),
            ),
            true,
        ),
        _ => (JSONValue::Null, false),
    }
}

pub fn header_to_jmap_id(header: HeaderValue) -> (JSONValue, bool) {
    match header {
        HeaderValue::Text(id) => (
            JSONValue::Array(vec![JSONValue::String(id.to_string())]),
            false,
        ),
        HeaderValue::TextList(ids) => (
            JSONValue::Array(
                ids.into_iter()
                    .map(|v| JSONValue::String(v.to_string()))
                    .collect(),
            ),
            false,
        ),
        HeaderValue::Collection(list) => (
            JSONValue::Array(
                list.into_iter()
                    .filter_map(|ids| match header_to_jmap_id(ids) {
                        (JSONValue::Null, _) => None,
                        (value, _) => Some(value),
                    })
                    .collect(),
            ),
            true,
        ),
        _ => (JSONValue::Null, false),
    }
}

pub fn header_to_jmap_text(header: HeaderValue) -> (JSONValue, bool) {
    match header {
        HeaderValue::Text(text) => (JSONValue::String(text.to_string()), false),
        HeaderValue::TextList(textlist) => (JSONValue::String(textlist.join(", ")), false),
        HeaderValue::Collection(list) => (
            JSONValue::Array(
                list.into_iter()
                    .filter_map(|ids| match header_to_jmap_text(ids) {
                        (JSONValue::Null, _) => None,
                        (value, _) => Some(value),
                    })
                    .collect(),
            ),
            true,
        ),
        _ => (JSONValue::Null, false),
    }
}

pub fn header_to_jmap_url(header: HeaderValue) -> (JSONValue, bool) {
    match header {
        HeaderValue::Address(Addr {
            address: Some(addr),
            ..
        }) if addr.contains(':') => (
            JSONValue::Array(vec![JSONValue::String(addr.to_string())]),
            false,
        ),
        HeaderValue::AddressList(textlist) => (
            JSONValue::Array(
                textlist
                    .into_iter()
                    .filter_map(|addr| match addr {
                        Addr {
                            address: Some(addr),
                            ..
                        } if addr.contains(':') => Some(JSONValue::String(addr.to_string())),
                        _ => None,
                    })
                    .collect(),
            ),
            false,
        ),
        HeaderValue::Collection(list) => (
            JSONValue::Array(
                list.into_iter()
                    .filter_map(|ids| match header_to_jmap_url(ids) {
                        (JSONValue::Null, _) => None,
                        (value, _) => Some(value),
                    })
                    .collect(),
            ),
            true,
        ),
        _ => (JSONValue::Null, false),
    }
}

pub fn header_to_jmap_address(
    header: HeaderValue,
    convert_to_group: bool,
) -> (JSONValue, bool, bool) {
    fn addr_to_jmap(addr: Addr) -> JSONValue {
        let mut jmap_addr = HashMap::with_capacity(2);
        jmap_addr.insert(
            "email".to_string(),
            JSONValue::String(addr.address.unwrap().to_string()),
        );
        jmap_addr.insert(
            "name".to_string(),
            addr.name
                .map_or(JSONValue::Null, |v| JSONValue::String(v.to_string())),
        );
        JSONValue::Object(jmap_addr)
    }

    fn addrlist_to_jmap(addrlist: Vec<Addr>) -> JSONValue {
        JSONValue::Array(
            addrlist
                .into_iter()
                .filter_map(|addr| match addr {
                    addr @ Addr {
                        address: Some(_), ..
                    } => Some(addr_to_jmap(addr)),
                    _ => None,
                })
                .collect(),
        )
    }

    fn group_to_jmap(group: Group) -> JSONValue {
        let mut jmap_addr = HashMap::with_capacity(2);
        jmap_addr.insert("addresses".to_string(), addrlist_to_jmap(group.addresses));
        jmap_addr.insert(
            "name".to_string(),
            group
                .name
                .map_or(JSONValue::Null, |v| JSONValue::String(v.to_string())),
        );
        JSONValue::Object(jmap_addr)
    }

    fn into_group(addresses: JSONValue) -> JSONValue {
        let mut email = HashMap::new();
        email.insert("name".to_string(), JSONValue::Null);
        email.insert("addresses".to_string(), addresses);
        JSONValue::Array(vec![JSONValue::Object(email)])
    }

    match header {
        HeaderValue::Address(
            addr @ Addr {
                address: Some(_), ..
            },
        ) => {
            let value = JSONValue::Array(vec![addr_to_jmap(addr)]);
            if !convert_to_group {
                (value, false, false)
            } else {
                (into_group(value), true, false)
            }
        }
        HeaderValue::AddressList(addrlist) => {
            let value = addrlist_to_jmap(addrlist);
            if !convert_to_group {
                (value, false, false)
            } else {
                (into_group(value), true, false)
            }
        }
        HeaderValue::Group(group) => (JSONValue::Array(vec![group_to_jmap(group)]), true, false),
        HeaderValue::GroupList(grouplist) => (
            JSONValue::Array(grouplist.into_iter().map(group_to_jmap).collect()),
            true,
            false,
        ),
        HeaderValue::Collection(list) => {
            let convert_to_group = list
                .iter()
                .any(|item| matches!(item, HeaderValue::Group(_) | HeaderValue::GroupList(_)));
            (
                JSONValue::Array(
                    list.into_iter()
                        .filter_map(|ids| match header_to_jmap_address(ids, convert_to_group) {
                            (JSONValue::Null, _, _) => None,
                            (value, _, _) => Some(value),
                        })
                        .collect(),
                ),
                convert_to_group,
                true,
            )
        }
        _ => (JSONValue::Null, false, false),
    }
}

fn empty_text_mime_headers(is_html: bool, size: usize) -> JMAPMailMimeHeaders {
    let mut mime_parts = HashMap::with_capacity(2);
    mime_parts.insert(MailBodyProperties::Size, size.into());
    mime_parts.insert(
        MailBodyProperties::Type,
        JSONValue::String(if is_html {
            "text/html".to_string()
        } else {
            "text/plain".to_string()
        }),
    );
    mime_parts
}

fn mime_parts_to_jmap(headers: RfcHeaders, size: usize) -> JMAPMailMimeHeaders {
    let mut mime_parts = HashMap::with_capacity(headers.len());
    if size > 0 {
        mime_parts.insert(MailBodyProperties::Size, size.into());
    }
    for (header, value) in headers {
        if let RfcHeader::ContentType
        | RfcHeader::ContentDisposition
        | RfcHeader::ContentId
        | RfcHeader::ContentLanguage
        | RfcHeader::ContentLocation = header
        {
            mime_header_to_jmap(&mut mime_parts, header, value);
        }
    }
    mime_parts
}

fn mime_header_to_jmap(
    mime_parts: &mut JMAPMailMimeHeaders,
    header: RfcHeader,
    value: HeaderValue,
) {
    match header {
        RfcHeader::ContentType => {
            if let HeaderValue::ContentType(content_type) = value {
                if let Some(mut attributes) = content_type.attributes {
                    if content_type.c_type == "text" {
                        if let Some(charset) = attributes.remove("charset") {
                            mime_parts.insert(
                                MailBodyProperties::Charset,
                                JSONValue::String(charset.to_string()),
                            );
                        }
                    }
                    if let Entry::Vacant(e) = mime_parts.entry(MailBodyProperties::Name) {
                        if let Some(name) = attributes.remove("name") {
                            e.insert(JSONValue::String(name.to_string()));
                        }
                    }
                }
                mime_parts.insert(
                    MailBodyProperties::Type,
                    if let Some(subtype) = content_type.c_subtype {
                        JSONValue::String(format!("{}/{}", content_type.c_type, subtype))
                    } else {
                        JSONValue::String(content_type.c_type.to_string())
                    },
                );
            }
        }
        RfcHeader::ContentDisposition => {
            if let HeaderValue::ContentType(content_disposition) = value {
                mime_parts.insert(
                    MailBodyProperties::Disposition,
                    JSONValue::String(content_disposition.c_type.to_string()),
                );
                if let Some(mut attributes) = content_disposition.attributes {
                    if let Some(name) = attributes.remove("filename") {
                        mime_parts.insert(
                            MailBodyProperties::Name,
                            JSONValue::String(name.to_string()),
                        );
                    }
                }
            }
        }
        RfcHeader::ContentId => match value {
            HeaderValue::Text(id) => {
                mime_parts.insert(MailBodyProperties::Cid, JSONValue::String(id.to_string()));
            }
            HeaderValue::TextList(mut ids) => {
                mime_parts.insert(
                    MailBodyProperties::Cid,
                    JSONValue::String(ids.pop().unwrap().to_string()),
                );
            }
            _ => {}
        },
        RfcHeader::ContentLanguage => match value {
            HeaderValue::Text(id) => {
                mime_parts.insert(
                    MailBodyProperties::Language,
                    JSONValue::Array(vec![JSONValue::String(id.to_string())]),
                );
            }
            HeaderValue::TextList(ids) => {
                mime_parts.insert(
                    MailBodyProperties::Language,
                    JSONValue::Array(
                        ids.into_iter()
                            .map(|v| JSONValue::String(v.to_string()))
                            .collect(),
                    ),
                );
            }
            _ => {}
        },
        RfcHeader::ContentLocation => match value {
            HeaderValue::Text(id) => {
                mime_parts.insert(
                    MailBodyProperties::Location,
                    JSONValue::String(id.to_string()),
                );
            }
            HeaderValue::TextList(mut ids) => {
                mime_parts.insert(
                    MailBodyProperties::Location,
                    JSONValue::String(ids.pop().unwrap().to_string()),
                );
            }
            _ => {}
        },
        _ => {}
    }
}
