use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
};

use chrono::{FixedOffset, LocalResult, SecondsFormat, TimeZone, Utc};
use jmap_store::json::JSONValue;
use mail_parser::{
    decoders::html::{html_to_text, text_to_html},
    parsers::fields::thread::thread_name,
    Addr, ContentType, DateTime, Group, HeaderValue, Message, MessageAttachment, MessagePart,
    RfcHeader, RfcHeaders,
};
use nlp::lang::{LanguageDetector, MIN_LANGUAGE_SCORE};
use store::{
    batch::{DocumentWriter, MAX_ID_LENGTH, MAX_SORT_FIELD_LENGTH, MAX_TOKEN_LENGTH},
    field::{FieldOptions, FullText, Text},
    Integer, LongInteger, StoreError, Tag, UncommittedDocumentId,
};

use crate::{
    JMAPMailBodyProperties, JMAPMailHeaderForm, JMAPMailHeaderProperty, JMAPMailMimeHeaders,
    JMAPMailProperties, MessageBody, MessageField, MessageRawHeaders, MimePart, MESSAGE_BODY,
    MESSAGE_BODY_STRUCTURE, MESSAGE_HEADERS_RAW, MESSAGE_PARTS, MESSAGE_RAW,
};

pub fn build_message_document<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
    message: Message<'x>,
    received_at: Option<i64>,
) -> store::Result<(Vec<Cow<'x, str>>, String)> {
    let mut total_parts = message.parts.len();
    let mut total_blobs = 0;
    let mut message_body = MessageBody {
        properties: HashMap::with_capacity(message.headers_rfc.len() + 3),
        mime_parts: Vec::with_capacity(total_parts + 1),
        html_body: message.html_body,
        text_body: message.text_body,
        attachments: message.attachments,
    };
    let mut parts_headers_raw = Vec::with_capacity(total_parts + 1);
    let mut language_detector = LanguageDetector::new();
    let mut has_attachments = false;

    message_body.properties.insert(
        JMAPMailProperties::Size,
        JSONValue::Number(message.raw_message.len() as i64),
    );

    document.add_integer(
        MessageField::Size.into(),
        message.raw_message.len() as Integer,
        FieldOptions::Sort,
    );

    {
        let received_at = received_at.unwrap_or_else(|| Utc::now().timestamp());
        message_body.properties.insert(
            JMAPMailProperties::ReceivedAt,
            if let LocalResult::Single(received_at) = Utc.timestamp_opt(received_at, 0) {
                JSONValue::String(received_at.to_rfc3339_opts(SecondsFormat::Secs, true))
            } else {
                JSONValue::Null
            },
        );
        document.add_long_int(
            MessageField::ReceivedAt.into(),
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
                let (value, is_all) = header_to_jmap_id(header_value);
                message_body.properties.insert(
                    JMAPMailProperties::Header(JMAPMailHeaderProperty::new_rfc(
                        header_name,
                        JMAPMailHeaderForm::MessageIds,
                        is_all,
                    )),
                    value,
                );
            }
            RfcHeader::From | RfcHeader::To | RfcHeader::Cc | RfcHeader::Bcc => {
                // Build sort index
                add_addr_sort(document, header_name, &header_value);
                let (value, is_group, is_all) = header_to_jmap_address(header_value, false);
                message_body.properties.insert(
                    JMAPMailProperties::Header(JMAPMailHeaderProperty::new_rfc(
                        header_name,
                        if is_group {
                            JMAPMailHeaderForm::GroupedAddresses
                        } else {
                            JMAPMailHeaderForm::Addresses
                        },
                        is_all,
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
                let (value, is_group, is_all) = header_to_jmap_address(header_value, false);
                message_body.properties.insert(
                    JMAPMailProperties::Header(JMAPMailHeaderProperty::new_rfc(
                        header_name,
                        if is_group {
                            JMAPMailHeaderForm::GroupedAddresses
                        } else {
                            JMAPMailHeaderForm::Addresses
                        },
                        is_all,
                    )),
                    value,
                );
            }
            RfcHeader::Date | RfcHeader::ResentDate => {
                let (value, is_all) = header_to_jmap_date(header_value);
                message_body.properties.insert(
                    JMAPMailProperties::Header(JMAPMailHeaderProperty::new_rfc(
                        header_name,
                        JMAPMailHeaderForm::Date,
                        is_all,
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
                let (value, is_all) = header_to_jmap_url(header_value);
                message_body.properties.insert(
                    JMAPMailProperties::Header(JMAPMailHeaderProperty::new_rfc(
                        header_name,
                        JMAPMailHeaderForm::URLs,
                        is_all,
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

                    document.add_text(
                        RfcHeader::Subject.into(),
                        Text::Full(FullText::new_lang(subject.to_string().into(), language)),
                        FieldOptions::None,
                    );

                    base_subject = Some(thread_name);
                }
                let (value, is_all) = header_to_jmap_text(header_value);
                message_body.properties.insert(
                    JMAPMailProperties::Header(JMAPMailHeaderProperty::new_rfc(
                        header_name,
                        JMAPMailHeaderForm::Text,
                        is_all,
                    )),
                    value,
                );
            }
            RfcHeader::Comments | RfcHeader::Keywords | RfcHeader::ListId => {
                let (value, is_all) = header_to_jmap_text(header_value);
                message_body.properties.insert(
                    JMAPMailProperties::Header(JMAPMailHeaderProperty::new_rfc(
                        header_name,
                        JMAPMailHeaderForm::Text,
                        is_all,
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

    message_body
        .mime_parts
        .push(MimePart::new_other(mime_parts, 0, false));
    parts_headers_raw.push(message.headers_raw);

    let mut extra_mime_parts = Vec::new();

    for (part_id, part) in message.parts.into_iter().enumerate() {
        match part {
            MessagePart::Html(html) => {
                let text = html_to_text(html.body.as_ref());
                let text_len = text.len();
                let html_len = html.body.len();
                let field =
                    if let Some(pos) = message_body.text_body.iter().position(|&p| p == part_id) {
                        message_body.text_body[pos] = total_parts;
                        extra_mime_parts.push(MimePart::new_text(
                            empty_text_mime_headers(false, text_len),
                            total_blobs,
                            false,
                        ));
                        total_parts += 1;
                        MessageField::Body
                    } else if message_body.html_body.contains(&part_id) {
                        MessageField::Body
                    } else {
                        has_attachments = true;
                        MessageField::Attachment
                    };

                document.add_text(
                    field.clone().into(),
                    Text::Full(FullText::new(text.into(), &mut language_detector)),
                    if field == MessageField::Body {
                        let blob_index = total_blobs;
                        total_blobs += 1;
                        FieldOptions::StoreAsBlob(blob_index + MESSAGE_PARTS)
                    } else {
                        FieldOptions::None
                    },
                );

                document.add_text(
                    field.into(),
                    Text::Default(html.body),
                    FieldOptions::StoreAsBlob(total_blobs + MESSAGE_PARTS),
                );

                message_body.mime_parts.push(MimePart::new_html(
                    mime_parts_to_jmap(html.headers_rfc, html_len),
                    total_blobs,
                    html.is_encoding_problem,
                ));
                parts_headers_raw.push(html.headers_raw);

                total_blobs += 1;
            }
            MessagePart::Text(text) => {
                let text_len = text.body.len();
                let field =
                    if let Some(pos) = message_body.html_body.iter().position(|&p| p == part_id) {
                        let html = text_to_html(text.body.as_ref());
                        let html_len = html.len();
                        document.add_text(
                            MessageField::Body.into(),
                            Text::Default(html.into()),
                            FieldOptions::StoreAsBlob(total_blobs + MESSAGE_PARTS),
                        );
                        extra_mime_parts.push(MimePart::new_html(
                            empty_text_mime_headers(true, html_len),
                            total_blobs,
                            false,
                        ));
                        message_body.html_body[pos] = total_parts;
                        total_blobs += 1;
                        total_parts += 1;
                        MessageField::Body
                    } else if message_body.text_body.contains(&part_id) {
                        MessageField::Body
                    } else {
                        has_attachments = true;
                        MessageField::Attachment
                    };

                document.add_text(
                    field.into(),
                    Text::Full(FullText::new(text.body, &mut language_detector)),
                    FieldOptions::StoreAsBlob(total_blobs + MESSAGE_PARTS),
                );

                message_body.mime_parts.push(MimePart::new_text(
                    mime_parts_to_jmap(text.headers_rfc, text_len),
                    total_blobs,
                    text.is_encoding_problem,
                ));
                parts_headers_raw.push(text.headers_raw);

                total_blobs += 1;
            }
            MessagePart::Binary(binary) => {
                if !has_attachments {
                    has_attachments = true;
                }
                message_body.mime_parts.push(MimePart::new_other(
                    mime_parts_to_jmap(binary.headers_rfc, binary.body.len()),
                    total_blobs,
                    binary.is_encoding_problem,
                ));
                parts_headers_raw.push(binary.headers_raw);

                document.add_binary(
                    MessageField::Attachment.into(),
                    binary.body,
                    FieldOptions::StoreAsBlob(total_blobs + MESSAGE_PARTS),
                );
                total_blobs += 1;
            }
            MessagePart::InlineBinary(binary) => {
                message_body.mime_parts.push(MimePart::new_other(
                    mime_parts_to_jmap(binary.headers_rfc, binary.body.len()),
                    total_blobs,
                    binary.is_encoding_problem,
                ));
                parts_headers_raw.push(binary.headers_raw);
                document.add_binary(
                    MessageField::Attachment.into(),
                    binary.body,
                    FieldOptions::StoreAsBlob(total_blobs + MESSAGE_PARTS),
                );
                total_blobs += 1;
            }
            MessagePart::Message(nested_message) => {
                if !has_attachments {
                    has_attachments = true;
                }
                message_body.mime_parts.push(MimePart::new_other(
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
                                document.add_binary(
                                    MessageField::Attachment.into(),
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
                                document.add_binary(
                                    MessageField::Attachment.into(),
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
                parts_headers_raw.push(nested_message.headers_raw);
            }
            MessagePart::Multipart(part) => {
                message_body.mime_parts.push(MimePart::new_other(
                    mime_parts_to_jmap(part.headers_rfc, 0),
                    0,
                    false,
                ));
                parts_headers_raw.push(part.headers_raw);
            }
        };
    }

    if !extra_mime_parts.is_empty() {
        message_body.mime_parts.append(&mut extra_mime_parts);
    }

    message_body.properties.insert(
        JMAPMailProperties::HasAttachment,
        JSONValue::Bool(has_attachments),
    );

    if has_attachments {
        document.set_tag(MessageField::Attachment.into(), Tag::Id(0));
    }

    document.add_binary(
        MessageField::Internal.into(),
        message.raw_message,
        FieldOptions::StoreAsBlob(MESSAGE_RAW),
    );

    document.add_binary(
        MessageField::Internal.into(),
        bincode::serialize(&MessageRawHeaders {
            size: message.offset_body,
            headers: parts_headers_raw,
        })
        .map_err(|e| StoreError::SerializeError(e.to_string()))?
        .into(),
        FieldOptions::StoreAsBlob(MESSAGE_HEADERS_RAW),
    );

    document.add_binary(
        MessageField::Internal.into(),
        bincode::serialize(&message_body)
            .map_err(|e| StoreError::SerializeError(e.to_string()))?
            .into(),
        FieldOptions::StoreAsBlob(MESSAGE_BODY),
    );

    document.add_binary(
        MessageField::Internal.into(),
        bincode::serialize(&message.structure)
            .map_err(|e| StoreError::SerializeError(e.to_string()))?
            .into(),
        FieldOptions::StoreAsBlob(MESSAGE_BODY_STRUCTURE),
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
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
    message: &mut Message,
    language_detector: &mut LanguageDetector,
) {
    if let Some(HeaderValue::Text(subject)) = message.headers_rfc.remove(&RfcHeader::Subject) {
        document.add_text(
            MessageField::Attachment.into(),
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
                document.add_text(
                    MessageField::Attachment.into(),
                    Text::Full(FullText::new(
                        text.body.into_owned().into(),
                        language_detector,
                    )),
                    FieldOptions::None,
                );
            }
            MessagePart::Html(html) => {
                document.add_text(
                    MessageField::Attachment.into(),
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

fn parse_address<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
    header_name: RfcHeader,
    address: &Addr<'x>,
) {
    if let Some(name) = &address.name {
        parse_text(document, header_name, name);
    };
    if let Some(ref addr) = address.address {
        if addr.len() <= MAX_TOKEN_LENGTH {
            document.add_text(
                header_name.into(),
                Text::Keyword(addr.to_lowercase().into()),
                FieldOptions::None,
            );
        }
    };
}

fn parse_address_group<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
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

fn parse_text<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
    header_name: RfcHeader,
    text: &str,
) {
    match header_name {
        RfcHeader::Keywords
        | RfcHeader::ContentLanguage
        | RfcHeader::MimeVersion
        | RfcHeader::MessageId
        | RfcHeader::References
        | RfcHeader::ContentId
        | RfcHeader::ResentMessageId => {
            if text.len() <= MAX_TOKEN_LENGTH {
                document.add_text(
                    header_name.into(),
                    Text::Keyword(text.to_lowercase().into()),
                    FieldOptions::None,
                );
            }
        }

        RfcHeader::Subject => (),

        _ => {
            document.add_text(
                header_name.into(),
                Text::Tokenized(text.to_string().into()),
                FieldOptions::None,
            );
        }
    }
}

fn parse_content_type<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
    header_name: RfcHeader,
    content_type: &ContentType<'x>,
) {
    if content_type.c_type.len() <= MAX_TOKEN_LENGTH {
        document.add_text(
            header_name.into(),
            Text::Keyword(content_type.c_type.clone()),
            FieldOptions::None,
        );
    }
    if let Some(subtype) = &content_type.c_subtype {
        if subtype.len() <= MAX_TOKEN_LENGTH {
            document.add_text(
                header_name.into(),
                Text::Keyword(subtype.clone()),
                FieldOptions::None,
            );
        }
    }
    if let Some(attributes) = &content_type.attributes {
        for (key, value) in attributes {
            if key == "name" || key == "filename" {
                document.add_text(
                    header_name.into(),
                    Text::Tokenized(value.clone()),
                    FieldOptions::None,
                );
            } else if value.len() <= MAX_TOKEN_LENGTH {
                document.add_text(
                    header_name.into(),
                    Text::Keyword(value.to_lowercase().into()),
                    FieldOptions::None,
                );
            }
        }
    }
}

fn parse_datetime(
    document: &mut DocumentWriter<impl UncommittedDocumentId>,
    header_name: RfcHeader,
    date_time: &DateTime,
) {
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
            document.add_long_int(
                header_name.into(),
                datetime.timestamp() as u64,
                FieldOptions::Sort,
            );
        }
    }
}

#[allow(clippy::manual_flatten)]
fn add_addr_sort<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
    header_name: RfcHeader,
    header_value: &HeaderValue<'x>,
) {
    let sort_parts = match if let HeaderValue::Collection(ref col) = header_value {
        col.first().unwrap_or(&HeaderValue::Empty)
    } else {
        &header_value
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
        document.add_text(
            header_name.into(),
            Text::Tokenized(text.into()),
            FieldOptions::Sort,
        );
    };
}

fn parse_header<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
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
                    addr
                    @ Addr {
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
    mime_parts.insert(JMAPMailBodyProperties::Size, JSONValue::Number(size as i64));
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
        mime_parts.insert(JMAPMailBodyProperties::Size, JSONValue::Number(size as i64));
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
