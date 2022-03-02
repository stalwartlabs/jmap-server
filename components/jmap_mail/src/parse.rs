use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
    iter::FromIterator,
    vec,
};

use chrono::{FixedOffset, LocalResult, SecondsFormat, TimeZone, Utc};
use jmap_store::{
    blob::JMAPLocalBlobStore,
    id::{BlobId, JMAPIdSerialize},
    json::JSONValue,
    local_store::JMAPLocalStore,
    JMAPError,
};
use mail_parser::{
    decoders::html::{html_to_text, text_to_html},
    parsers::{
        fields::thread::thread_name,
        preview::{preview_html, preview_text},
    },
    Addr, ContentType, DateTime, Group, HeaderName, HeaderValue, Message, MessageAttachment,
    MessagePart, RfcHeader, RfcHeaders,
};
use nlp::lang::{LanguageDetector, MIN_LANGUAGE_SCORE};
use store::{
    batch::{WriteBatch, MAX_ID_LENGTH, MAX_SORT_FIELD_LENGTH, MAX_TOKEN_LENGTH},
    field::{FieldOptions, FullText, Text},
    leb128::Leb128,
    AccountId, BlobIndex, Integer, LongInteger, Store, Tag,
};

use crate::{
    get::{
        add_body_parts, add_body_structure, add_body_value, add_raw_header,
        transform_json_emailaddress, transform_json_string, transform_json_stringlist,
        transform_rfc_header,
    },
    import::{bincode_serialize, messagepack_serialize},
    JMAPMailBodyProperties, JMAPMailGetArguments, JMAPMailHeaderForm, JMAPMailHeaderProperty,
    JMAPMailMimeHeaders, JMAPMailParse, JMAPMailProperties, MessageData, MessageField,
    MessageOutline, MimePart, MimePartType, MESSAGE_DATA, MESSAGE_PARTS, MESSAGE_RAW,
};

pub struct JMAPMailParseRequest<'x> {
    pub account_id: AccountId,
    pub blob_ids: Vec<BlobId>,
    pub properties: Vec<JMAPMailProperties<'x>>,
    pub arguments: JMAPMailGetArguments<'x>,
}

#[derive(Debug)]
pub struct JMAPMailParseResponse {
    pub parsed: JSONValue,
    pub not_parsable: JSONValue,
    pub not_found: JSONValue,
}

impl From<JMAPMailParseResponse> for JSONValue {
    fn from(value: JMAPMailParseResponse) -> Self {
        let mut result = HashMap::new();
        result.insert("parsed".to_string(), value.parsed);
        result.insert("notParsable".to_string(), value.not_parsable);
        result.insert("notFound".to_string(), value.not_found);
        result.into()
    }
}

impl<'x, T> JMAPMailParse<'x> for JMAPLocalStore<T>
where
    T: Store<'x>,
{
    fn mail_parse(
        &'x self,
        mut request: JMAPMailParseRequest<'x>,
    ) -> jmap_store::Result<JMAPMailParseResponse> {
        let mut parsed = HashMap::new();
        let mut not_parsable = Vec::new();
        let mut not_found = Vec::new();

        if request.blob_ids.len() > self.mail_config.parse_max_items {
            return Err(JMAPError::RequestTooLarge);
        }

        if request.properties.is_empty() {
            request.properties = vec![
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
            ];
        }

        if request.arguments.body_properties.is_empty() {
            request.arguments.body_properties = vec![
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

        Ok(JMAPMailParseResponse {
            parsed: if !parsed.is_empty() {
                parsed.into()
            } else {
                JSONValue::Null
            },
            not_parsable: if !not_parsable.is_empty() {
                not_parsable.into()
            } else {
                JSONValue::Null
            },
            not_found: if !not_found.is_empty() {
                not_found.into()
            } else {
                JSONValue::Null
            },
        })
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
    request: &JMAPMailParseRequest,
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
    message_outline.headers.push(message.headers_raw);

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
                        blobs.len(),
                        false,
                    ));
                    blobs.push(value);
                    total_parts += 1;
                } else if !html_body.contains(&part_id) {
                    has_attachments = true;
                }
                mime_parts.push(MimePart::new_html(
                    mime_parts_to_jmap(html.headers_rfc, html.body.len()),
                    blobs.len(),
                    html.is_encoding_problem,
                ));
                blobs.push(html.body.into_owned().into_bytes());
                message_outline.headers.push(html.headers_raw);
            }
            MessagePart::Text(text) => {
                if let Some(pos) = html_body.iter().position(|&p| p == part_id) {
                    let value = text_to_html(text.body.as_ref());
                    let value_len = value.len();
                    extra_mime_parts.push(MimePart::new_html(
                        empty_text_mime_headers(true, value_len),
                        blobs.len(),
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
                    blobs.len(),
                    text.is_encoding_problem,
                ));
                blobs.push(text.body.into_owned().into_bytes());
                message_outline.headers.push(text.headers_raw);
            }
            MessagePart::Binary(binary) => {
                if !has_attachments {
                    has_attachments = true;
                }
                mime_parts.push(MimePart::new_other(
                    mime_parts_to_jmap(binary.headers_rfc, binary.body.len()),
                    blobs.len(),
                    binary.is_encoding_problem,
                ));
                blobs.push(binary.body.into_owned());
                message_outline.headers.push(binary.headers_raw);
            }
            MessagePart::InlineBinary(binary) => {
                mime_parts.push(MimePart::new_other(
                    mime_parts_to_jmap(binary.headers_rfc, binary.body.len()),
                    blobs.len(),
                    binary.is_encoding_problem,
                ));
                blobs.push(binary.body.into_owned());
                message_outline.headers.push(binary.headers_raw);
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
                    blob_index,
                    false,
                ));
                message_outline.headers.push(nested_message.headers_raw);
            }
            MessagePart::Multipart(part) => {
                mime_parts.push(MimePart::new_other(
                    mime_parts_to_jmap(part.headers_rfc, 0),
                    0,
                    false,
                ));
                message_outline.headers.push(part.headers_raw);
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
                JMAPMailProperties::Id
                | JMAPMailProperties::ThreadId
                | JMAPMailProperties::MailboxIds
                | JMAPMailProperties::ReceivedAt
                | JMAPMailProperties::Keywords => JSONValue::Null,

                JMAPMailProperties::BlobId => blob_id.to_jmap_string().into(),
                JMAPMailProperties::Size => raw_message.len().into(),
                JMAPMailProperties::MessageId
                | JMAPMailProperties::References
                | JMAPMailProperties::InReplyTo => {
                    if let Some(message_id) = message.headers_rfc.remove(&property.as_rfc_header())
                    {
                        let (value, is_collection) = header_to_jmap_id(message_id);
                        transform_json_stringlist(value, is_collection, false)
                    } else {
                        JSONValue::Null
                    }
                }
                JMAPMailProperties::Sender
                | JMAPMailProperties::From
                | JMAPMailProperties::To
                | JMAPMailProperties::Cc
                | JMAPMailProperties::Bcc
                | JMAPMailProperties::ReplyTo => {
                    if let Some(addr) = message.headers_rfc.remove(&property.as_rfc_header()) {
                        let (value, is_grouped, is_collection) =
                            header_to_jmap_address(addr, false);
                        transform_json_emailaddress(value, is_grouped, is_collection, false, false)
                    } else {
                        JSONValue::Null
                    }
                }
                JMAPMailProperties::Subject => {
                    if let Some(text) = message.headers_rfc.remove(&RfcHeader::Subject) {
                        let (value, _) = header_to_jmap_text(text);
                        transform_json_string(value, false)
                    } else {
                        JSONValue::Null
                    }
                }
                JMAPMailProperties::SentAt => {
                    if let Some(date) = message.headers_rfc.remove(&RfcHeader::Date) {
                        let (value, _) = header_to_jmap_date(date);
                        transform_json_string(value, false)
                    } else {
                        JSONValue::Null
                    }
                }
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
                        .headers
                        .get_mut(0)
                        .and_then(|l| l.remove(header))
                    {
                        add_raw_header(&offsets, raw_message, form.clone(), *all)
                    } else {
                        JSONValue::Null
                    }
                }
                JMAPMailProperties::Header(JMAPMailHeaderProperty {
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
                JMAPMailProperties::HasAttachment => has_attachments.into(),
                JMAPMailProperties::Preview => {
                    if !text_body.is_empty() {
                        preview_text(
                            String::from_utf8_lossy(
                                &blobs[text_body
                                    .get(0)
                                    .and_then(|p| mime_parts.get(p + 1))
                                    .unwrap()
                                    .blob_index],
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
                                    .blob_index],
                            ),
                            256,
                        )
                        .to_string()
                        .into()
                    } else {
                        JSONValue::Null
                    }
                }
                JMAPMailProperties::BodyValues => {
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
                                        String::from_utf8_lossy(&blobs[mime_part.blob_index])
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
                JMAPMailProperties::TextBody => add_body_parts(
                    &text_body,
                    &mime_parts,
                    &request.arguments.body_properties,
                    Some(raw_message),
                    Some(&message_outline),
                    &base_blob_id,
                ),

                JMAPMailProperties::HtmlBody => add_body_parts(
                    &html_body,
                    &mime_parts,
                    &request.arguments.body_properties,
                    Some(raw_message),
                    Some(&message_outline),
                    &base_blob_id,
                ),

                JMAPMailProperties::Attachments => add_body_parts(
                    &attachments,
                    &mime_parts,
                    &request.arguments.body_properties,
                    Some(raw_message),
                    Some(&message_outline),
                    &base_blob_id,
                ),

                JMAPMailProperties::BodyStructure => {
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

pub fn build_message_document<'x>(
    document: &mut WriteBatch<'x>,
    message: Message<'x>,
    received_at: Option<i64>,
) -> store::Result<(Vec<Cow<'x, str>>, String)> {
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
    };
    let mut language_detector = LanguageDetector::new();
    let mut has_attachments = false;

    message_data
        .properties
        .insert(JMAPMailProperties::Size, message.raw_message.len().into());

    document.integer(
        MessageField::Size,
        message.raw_message.len() as Integer,
        FieldOptions::Sort,
    );

    {
        let received_at = received_at.unwrap_or_else(|| Utc::now().timestamp());
        message_data.properties.insert(
            JMAPMailProperties::ReceivedAt,
            if let LocalResult::Single(received_at) = Utc.timestamp_opt(received_at, 0) {
                JSONValue::String(received_at.to_rfc3339_opts(SecondsFormat::Secs, true))
            } else {
                JSONValue::Null
            },
        );
        document.long_int(
            MessageField::ReceivedAt,
            received_at as LongInteger,
            FieldOptions::Sort,
        );
    }

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
                        reference_ids.push(text.clone())
                    }
                    HeaderValue::TextList(list) => {
                        reference_ids.extend(
                            list.iter()
                                .filter(|text| text.len() <= MAX_ID_LENGTH)
                                .cloned(),
                        );
                    }
                    HeaderValue::Collection(col) => {
                        for item in col {
                            match item {
                                HeaderValue::Text(text) if text.len() <= MAX_ID_LENGTH => {
                                    reference_ids.push(text.clone())
                                }
                                HeaderValue::TextList(list) => reference_ids.extend(
                                    list.iter()
                                        .filter(|text| text.len() <= MAX_ID_LENGTH)
                                        .cloned(),
                                ),
                                _ => (),
                            }
                        }
                    }
                    _ => (),
                }
                let (value, is_collection) = header_to_jmap_id(header_value);
                message_data.properties.insert(
                    JMAPMailProperties::Header(JMAPMailHeaderProperty::new_rfc(
                        header_name,
                        JMAPMailHeaderForm::MessageIds,
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
                    JMAPMailProperties::Header(JMAPMailHeaderProperty::new_rfc(
                        header_name,
                        if is_grouped {
                            JMAPMailHeaderForm::GroupedAddresses
                        } else {
                            JMAPMailHeaderForm::Addresses
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
                    JMAPMailProperties::Header(JMAPMailHeaderProperty::new_rfc(
                        header_name,
                        if is_grouped {
                            JMAPMailHeaderForm::GroupedAddresses
                        } else {
                            JMAPMailHeaderForm::Addresses
                        },
                        is_collection,
                    )),
                    value,
                );
            }
            RfcHeader::Date | RfcHeader::ResentDate => {
                let (value, is_collection) = header_to_jmap_date(header_value);
                message_data.properties.insert(
                    JMAPMailProperties::Header(JMAPMailHeaderProperty::new_rfc(
                        header_name,
                        JMAPMailHeaderForm::Date,
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
                    JMAPMailProperties::Header(JMAPMailHeaderProperty::new_rfc(
                        header_name,
                        JMAPMailHeaderForm::URLs,
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
                        Text::Full(FullText::new_lang(subject.to_string().into(), language)),
                        FieldOptions::None,
                    );

                    base_subject = Some(thread_name);
                }
                let (value, is_collection) = header_to_jmap_text(header_value);
                message_data.properties.insert(
                    JMAPMailProperties::Header(JMAPMailHeaderProperty::new_rfc(
                        header_name,
                        JMAPMailHeaderForm::Text,
                        is_collection,
                    )),
                    value,
                );
            }
            RfcHeader::Comments | RfcHeader::Keywords | RfcHeader::ListId => {
                let (value, is_collection) = header_to_jmap_text(header_value);
                message_data.properties.insert(
                    JMAPMailProperties::Header(JMAPMailHeaderProperty::new_rfc(
                        header_name,
                        JMAPMailHeaderForm::Text,
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
    message_outline.headers.push(message.headers_raw);

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
                    Text::Full(FullText::new(text.into(), &mut language_detector)),
                    if field == MessageField::Body {
                        let blob_index = total_blobs;
                        total_blobs += 1;
                        FieldOptions::StoreAsBlob(blob_index + MESSAGE_PARTS)
                    } else {
                        FieldOptions::None
                    },
                );

                document.text(
                    field,
                    Text::Default(html.body),
                    FieldOptions::StoreAsBlob(total_blobs + MESSAGE_PARTS),
                );

                message_data.mime_parts.push(MimePart::new_html(
                    mime_parts_to_jmap(html.headers_rfc, html_len),
                    total_blobs,
                    html.is_encoding_problem,
                ));
                message_outline.headers.push(html.headers_raw);

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
                            Text::Default(html.into()),
                            FieldOptions::StoreAsBlob(total_blobs + MESSAGE_PARTS),
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
                    Text::Full(FullText::new(text.body, &mut language_detector)),
                    FieldOptions::StoreAsBlob(total_blobs + MESSAGE_PARTS),
                );

                message_data.mime_parts.push(MimePart::new_text(
                    mime_parts_to_jmap(text.headers_rfc, text_len),
                    total_blobs,
                    text.is_encoding_problem,
                ));
                message_outline.headers.push(text.headers_raw);

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
                message_outline.headers.push(binary.headers_raw);

                document.binary(
                    MessageField::Attachment,
                    binary.body,
                    FieldOptions::StoreAsBlob(total_blobs + MESSAGE_PARTS),
                );
                total_blobs += 1;
            }
            MessagePart::InlineBinary(binary) => {
                message_data.mime_parts.push(MimePart::new_other(
                    mime_parts_to_jmap(binary.headers_rfc, binary.body.len()),
                    total_blobs,
                    binary.is_encoding_problem,
                ));
                message_outline.headers.push(binary.headers_raw);
                document.binary(
                    MessageField::Attachment,
                    binary.body,
                    FieldOptions::StoreAsBlob(total_blobs + MESSAGE_PARTS),
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
                                    message.raw_message,
                                    FieldOptions::StoreAsBlob(total_blobs + MESSAGE_PARTS),
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
                                    raw_message,
                                    FieldOptions::StoreAsBlob(total_blobs + MESSAGE_PARTS),
                                );
                                message_size
                            }
                        },
                    ),
                    total_blobs,
                    false,
                ));
                total_blobs += 1;
                message_outline.headers.push(nested_message.headers_raw);
            }
            MessagePart::Multipart(part) => {
                message_data.mime_parts.push(MimePart::new_other(
                    mime_parts_to_jmap(part.headers_rfc, 0),
                    0,
                    false,
                ));
                message_outline.headers.push(part.headers_raw);
            }
        };
    }

    if !extra_mime_parts.is_empty() {
        message_data.mime_parts.append(&mut extra_mime_parts);
    }

    message_data.properties.insert(
        JMAPMailProperties::HasAttachment,
        JSONValue::Bool(has_attachments),
    );

    if has_attachments {
        document.tag(MessageField::Attachment, Tag::Id(0), FieldOptions::None);
    }

    document.binary(
        MessageField::Internal,
        message.raw_message,
        FieldOptions::StoreAsBlob(MESSAGE_RAW),
    );

    let mut message_data = messagepack_serialize(&message_data)?;
    let mut message_outline = bincode_serialize(&message_outline)?;
    let mut buf = Vec::with_capacity(
        message_data.len() + message_outline.len() + std::mem::size_of::<usize>(),
    );
    message_data.len().to_leb128_bytes(&mut buf);
    buf.append(&mut message_data);
    buf.append(&mut message_outline);

    document.binary(
        MessageField::Internal,
        buf.into(),
        FieldOptions::StoreAsBlob(MESSAGE_DATA),
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

fn parse_attached_message<'x>(
    document: &mut WriteBatch<'x>,
    message: &mut Message,
    language_detector: &mut LanguageDetector,
) {
    if let Some(HeaderValue::Text(subject)) = message.headers_rfc.remove(&RfcHeader::Subject) {
        document.text(
            MessageField::Attachment,
            Text::Full(FullText::new(
                subject.into_owned().into(),
                language_detector,
            )),
            FieldOptions::None,
        );
    }
    for part in message.parts.drain(..) {
        match part {
            MessagePart::Text(text) => {
                document.text(
                    MessageField::Attachment,
                    Text::Full(FullText::new(
                        text.body.into_owned().into(),
                        language_detector,
                    )),
                    FieldOptions::None,
                );
            }
            MessagePart::Html(html) => {
                document.text(
                    MessageField::Attachment,
                    Text::Full(FullText::new(
                        html_to_text(&html.body).into(),
                        language_detector,
                    )),
                    FieldOptions::None,
                );
            }
            _ => (),
        }
    }
}

fn parse_address<'x>(document: &mut WriteBatch<'x>, header_name: RfcHeader, address: &Addr<'x>) {
    if let Some(name) = &address.name {
        parse_text(document, header_name, name);
    };
    if let Some(ref addr) = address.address {
        if addr.len() <= MAX_TOKEN_LENGTH {
            document.text(
                header_name,
                Text::Keyword(addr.to_lowercase().into()),
                FieldOptions::None,
            );
        }
    };
}

fn parse_address_group<'x>(
    document: &mut WriteBatch<'x>,
    header_name: RfcHeader,
    group: &Group<'x>,
) {
    if let Some(name) = &group.name {
        parse_text(document, header_name, name);
    };

    for address in group.addresses.iter() {
        parse_address(document, header_name, address);
    }
}

fn parse_text<'x>(document: &mut WriteBatch<'x>, header_name: RfcHeader, text: &str) {
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
                    Text::Keyword(text.to_lowercase().into()),
                    FieldOptions::None,
                );
            }
        }

        RfcHeader::Subject => (),

        _ => {
            document.text(
                header_name,
                Text::Tokenized(text.to_string().into()),
                FieldOptions::None,
            );
        }
    }
}

fn parse_content_type<'x>(
    document: &mut WriteBatch<'x>,
    header_name: RfcHeader,
    content_type: &ContentType<'x>,
) {
    if content_type.c_type.len() <= MAX_TOKEN_LENGTH {
        document.text(
            header_name,
            Text::Keyword(content_type.c_type.clone()),
            FieldOptions::None,
        );
    }
    if let Some(subtype) = &content_type.c_subtype {
        if subtype.len() <= MAX_TOKEN_LENGTH {
            document.text(
                header_name,
                Text::Keyword(subtype.clone()),
                FieldOptions::None,
            );
        }
    }
    if let Some(attributes) = &content_type.attributes {
        for (key, value) in attributes {
            if key == "name" || key == "filename" {
                document.text(
                    header_name,
                    Text::Tokenized(value.clone()),
                    FieldOptions::None,
                );
            } else if value.len() <= MAX_TOKEN_LENGTH {
                document.text(
                    header_name,
                    Text::Keyword(value.to_lowercase().into()),
                    FieldOptions::None,
                );
            }
        }
    }
}

fn parse_datetime(document: &mut WriteBatch, header_name: RfcHeader, date_time: &DateTime) {
    if (0..23).contains(&date_time.tz_hour)
        && (0..59).contains(&date_time.tz_minute)
        && (1970..2500).contains(&date_time.year)
        && (1..12).contains(&date_time.month)
        && (1..31).contains(&date_time.day)
        && (0..23).contains(&date_time.hour)
        && (0..59).contains(&date_time.minute)
        && (0..59).contains(&date_time.second)
    {
        if let LocalResult::Single(datetime) | LocalResult::Ambiguous(datetime, _) =
            FixedOffset::west_opt(
                ((date_time.tz_hour as i32 * 3600i32) + date_time.tz_minute as i32)
                    * if date_time.tz_before_gmt { 1i32 } else { -1i32 },
            )
            .unwrap_or_else(|| FixedOffset::east(0))
            .ymd_opt(date_time.year as i32, date_time.month, date_time.day)
            .and_hms_opt(date_time.hour, date_time.minute, date_time.second)
        {
            document.long_int(header_name, datetime.timestamp() as u64, FieldOptions::Sort);
        }
    }
}

#[allow(clippy::manual_flatten)]
fn add_addr_sort<'x>(
    document: &mut WriteBatch<'x>,
    header_name: RfcHeader,
    header_value: &HeaderValue<'x>,
) {
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
            Text::Tokenized(text.into()),
            FieldOptions::Sort,
        );
    };
}

fn parse_header<'x>(
    document: &mut WriteBatch<'x>,
    header_name: RfcHeader,
    header_value: &HeaderValue<'x>,
) {
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
            parse_datetime(document, header_name, date_time);
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

fn empty_text_mime_headers<'x>(is_html: bool, size: usize) -> JMAPMailMimeHeaders<'x> {
    let mut mime_parts = HashMap::with_capacity(2);
    mime_parts.insert(JMAPMailBodyProperties::Size, size.into());
    mime_parts.insert(
        JMAPMailBodyProperties::Type,
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
        mime_parts.insert(JMAPMailBodyProperties::Size, size.into());
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

fn mime_header_to_jmap<'x>(
    mime_parts: &mut JMAPMailMimeHeaders<'x>,
    header: RfcHeader,
    value: HeaderValue<'x>,
) {
    match header {
        RfcHeader::ContentType => {
            if let HeaderValue::ContentType(content_type) = value {
                if let Some(mut attributes) = content_type.attributes {
                    if content_type.c_type == "text" {
                        if let Some(charset) = attributes.remove("charset") {
                            mime_parts.insert(
                                JMAPMailBodyProperties::Charset,
                                JSONValue::String(charset.to_string()),
                            );
                        }
                    }
                    if let Entry::Vacant(e) = mime_parts.entry(JMAPMailBodyProperties::Name) {
                        if let Some(name) = attributes.remove("name") {
                            e.insert(JSONValue::String(name.to_string()));
                        }
                    }
                }
                mime_parts.insert(
                    JMAPMailBodyProperties::Type,
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
                    JMAPMailBodyProperties::Disposition,
                    JSONValue::String(content_disposition.c_type.to_string()),
                );
                if let Some(mut attributes) = content_disposition.attributes {
                    if let Some(name) = attributes.remove("filename") {
                        mime_parts.insert(
                            JMAPMailBodyProperties::Name,
                            JSONValue::String(name.to_string()),
                        );
                    }
                }
            }
        }
        RfcHeader::ContentId => match value {
            HeaderValue::Text(id) => {
                mime_parts.insert(
                    JMAPMailBodyProperties::Cid,
                    JSONValue::String(id.to_string()),
                );
            }
            HeaderValue::TextList(mut ids) => {
                mime_parts.insert(
                    JMAPMailBodyProperties::Cid,
                    JSONValue::String(ids.pop().unwrap().to_string()),
                );
            }
            _ => {}
        },
        RfcHeader::ContentLanguage => match value {
            HeaderValue::Text(id) => {
                mime_parts.insert(
                    JMAPMailBodyProperties::Language,
                    JSONValue::Array(vec![JSONValue::String(id.to_string())]),
                );
            }
            HeaderValue::TextList(ids) => {
                mime_parts.insert(
                    JMAPMailBodyProperties::Language,
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
                    JMAPMailBodyProperties::Location,
                    JSONValue::String(id.to_string()),
                );
            }
            HeaderValue::TextList(mut ids) => {
                mime_parts.insert(
                    JMAPMailBodyProperties::Location,
                    JSONValue::String(ids.pop().unwrap().to_string()),
                );
            }
            _ => {}
        },
        _ => {}
    }
}
