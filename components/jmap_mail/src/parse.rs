use std::{borrow::Cow, collections::HashMap};

use chrono::{FixedOffset, LocalResult, TimeZone, Utc};
use jmap_store::json::JSONValue;
use mail_parser::{
    decoders::html::{html_to_text, text_to_html},
    parsers::fields::thread::thread_name,
    Addr, ContentType, DateTime, Group, HeaderName, HeaderValue, Message, MessageAttachment,
    MessagePart,
};
use nlp::lang::{LanguageDetector, MIN_LANGUAGE_SCORE};
use store::{
    batch::{DocumentWriter, MAX_ID_LENGTH, MAX_SORT_FIELD_LENGTH, MAX_TOKEN_LENGTH},
    field::Text,
    FieldNumber, Integer, LongInteger, StoreError, Tag, UncommittedDocumentId,
};

use crate::{JMAPMailProperties, MessageField, MessageParts};

pub fn build_message_document<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
    message: Message<'x>,
    received_at: Option<i64>,
) -> store::Result<(Vec<Cow<'x, str>>, String)> {
    let mut message_parts = MessageParts {
        html_body: message.html_body,
        text_body: message.text_body,
        attachments: message.attachments,
        offset_body: message.offset_body,
        size: message.raw_message.len(),
        received_at: received_at.unwrap_or_else(|| Utc::now().timestamp()),
        has_attachments: false,
    };
    let mut total_message_parts = message.parts.len();
    let mut nested_headers = Vec::with_capacity(message.parts.len());
    let mut language_detector = LanguageDetector::new();

    document.add_integer(
        MessageField::Size.into(),
        0,
        message_parts.size as Integer,
        false,
        true,
    );
    document.add_long_int(
        MessageField::ReceivedAt.into(),
        0,
        message_parts.received_at as LongInteger,
        false,
        true,
    );

    let mut reference_ids = Vec::new();
    let mut jmap_headers = HashMap::with_capacity(message.headers_rfc.len());
    let mut mime_headers = HashMap::with_capacity(5);
    let mut base_subject = None;

    for (header_name, header_value) in message.headers_rfc {
        // Add headers to document
        parse_header(document, header_name, &header_value);

        // Build JMAP headers
        match header_name {
            HeaderName::MessageId
            | HeaderName::InReplyTo
            | HeaderName::References
            | HeaderName::ResentMessageId => {
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

                jmap_headers.insert(header_name, header_to_jmap_id(header_value));
            }
            HeaderName::From | HeaderName::To | HeaderName::Cc | HeaderName::Bcc => {
                // Build sort index
                add_addr_sort(document, header_name, &header_value);
                jmap_headers.insert(header_name, header_to_jmap_address(header_value));
            }
            HeaderName::ReplyTo
            | HeaderName::Sender
            | HeaderName::ResentTo
            | HeaderName::ResentFrom
            | HeaderName::ResentBcc
            | HeaderName::ResentCc
            | HeaderName::ResentSender => {
                jmap_headers.insert(header_name, header_to_jmap_address(header_value));
            }
            HeaderName::Date | HeaderName::ResentDate => {
                jmap_headers.insert(header_name, header_to_jmap_date(header_value));
            }
            HeaderName::ListArchive
            | HeaderName::ListHelp
            | HeaderName::ListOwner
            | HeaderName::ListPost
            | HeaderName::ListSubscribe
            | HeaderName::ListUnsubscribe => {
                jmap_headers.insert(header_name, header_to_jmap_url(header_value));
            }
            HeaderName::Subject => {
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
                        HeaderName::Subject.into(),
                        0,
                        Text::Full((subject.to_string().into(), language)),
                        false,
                        false,
                    );

                    base_subject = Some(thread_name);
                }
                jmap_headers.insert(header_name, header_to_jmap_text(header_value));
            }
            HeaderName::Comments | HeaderName::Keywords | HeaderName::ListId => {
                jmap_headers.insert(header_name, header_to_jmap_text(header_value));
            }
            HeaderName::ContentType
            | HeaderName::ContentDisposition
            | HeaderName::ContentId
            | HeaderName::ContentLanguage
            | HeaderName::ContentLocation
            | HeaderName::ContentTransferEncoding
            | HeaderName::ContentDescription
            | HeaderName::MimeVersion => {
                mime_headers.insert(header_name, header_value);
            }
            HeaderName::Received | HeaderName::ReturnPath | HeaderName::Other => (), // Ignore
        }
    }

    nested_headers.push(mime_headers);

    for (part_id, part) in message.parts.into_iter().enumerate() {
        match part {
            MessagePart::Html(html) => {
                nested_headers.push(html.headers);

                let field =
                    if let Some(pos) = message_parts.text_body.iter().position(|&p| p == part_id) {
                        message_parts.text_body[pos] = total_message_parts;
                        MessageField::Body
                    } else if message_parts.html_body.contains(&part_id) {
                        MessageField::Body
                    } else {
                        message_parts.has_attachments = true;
                        MessageField::Attachment
                    };

                document.add_text(
                    field.clone().into(),
                    total_message_parts as FieldNumber,
                    Text::Full(
                        language_detector.detect_wrap(html_to_text(html.body.as_ref()).into()),
                    ),
                    true,
                    field == MessageField::Body,
                );

                document.add_text(
                    field.into(),
                    part_id as FieldNumber,
                    Text::Default(html.body),
                    true,
                    false,
                );
                total_message_parts += 1;
            }
            MessagePart::Text(text) => {
                nested_headers.push(text.headers);
                let field =
                    if let Some(pos) = message_parts.html_body.iter().position(|&p| p == part_id) {
                        document.add_text(
                            MessageField::Body.into(),
                            total_message_parts as FieldNumber,
                            Text::Default(text_to_html(text.body.as_ref()).into()),
                            true,
                            false,
                        );
                        message_parts.html_body[pos] = total_message_parts;
                        total_message_parts += 1;
                        MessageField::Body
                    } else if message_parts.text_body.contains(&part_id) {
                        MessageField::Body
                    } else {
                        message_parts.has_attachments = true;
                        MessageField::Attachment
                    };

                document.add_text(
                    field.into(),
                    part_id as FieldNumber,
                    Text::Full(language_detector.detect_wrap(text.body)),
                    true,
                    false,
                );
            }
            MessagePart::Binary(binary) => {
                if !message_parts.has_attachments {
                    message_parts.has_attachments = true;
                }
                nested_headers.push(binary.headers);
                document.add_blob(
                    MessageField::Attachment.into(),
                    part_id as FieldNumber,
                    binary.body,
                );
            }
            MessagePart::InlineBinary(binary) => {
                nested_headers.push(binary.headers);
                document.add_blob(
                    MessageField::Attachment.into(),
                    part_id as FieldNumber,
                    binary.body,
                );
            }
            MessagePart::Message(nested_message) => {
                if !message_parts.has_attachments {
                    message_parts.has_attachments = true;
                }
                nested_headers.push(nested_message.headers);
                match nested_message.body {
                    MessageAttachment::Parsed(mut message) => {
                        parse_attached_message(
                            document,
                            &mut message,
                            part_id,
                            &mut language_detector,
                        );
                        document.add_blob(
                            MessageField::Attachment.into(),
                            part_id as FieldNumber,
                            message.raw_message,
                        );
                    }
                    MessageAttachment::Raw(raw_message) => {
                        if let Some(message) = &mut Message::parse(raw_message.as_ref()) {
                            parse_attached_message(
                                document,
                                message,
                                part_id,
                                &mut language_detector,
                            )
                        }
                        document.add_blob(
                            MessageField::Attachment.into(),
                            part_id as FieldNumber,
                            raw_message,
                        );
                    }
                }
            }
            MessagePart::Multipart(header) => {
                nested_headers.push(header);
            }
        };
    }

    if message_parts.has_attachments {
        document.set_tag(MessageField::Attachment.into(), Tag::Id(0));
    }

    document.add_blob(
        MessageField::Internal.into(),
        crate::MESSAGE_HEADERS,
        bincode::serialize(&jmap_headers)
            .map_err(|e| StoreError::SerializeError(e.to_string()))?
            .into(),
    );

    document.add_blob(
        MessageField::Internal.into(),
        crate::MESSAGE_RAW,
        message.raw_message,
    );

    document.add_blob(
        MessageField::Internal.into(),
        crate::MESSAGE_HEADERS_OTHER,
        bincode::serialize(&message.headers_other)
            .map_err(|e| StoreError::SerializeError(e.to_string()))?
            .into(),
    );

    document.add_blob(
        MessageField::Internal.into(),
        crate::MESSAGE_HEADERS_OFFSETS,
        bincode::serialize(&message.headers_offsets)
            .map_err(|e| StoreError::SerializeError(e.to_string()))?
            .into(),
    );

    document.add_blob(
        MessageField::Internal.into(),
        crate::MESSAGE_HEADERS_NESTED,
        bincode::serialize(&nested_headers)
            .map_err(|e| StoreError::SerializeError(e.to_string()))?
            .into(),
    );

    document.add_blob(
        MessageField::Internal.into(),
        crate::MESSAGE_PARTS,
        bincode::serialize(&message_parts)
            .map_err(|e| StoreError::SerializeError(e.to_string()))?
            .into(),
    );

    document.add_blob(
        MessageField::Internal.into(),
        crate::MESSAGE_STRUCTURE,
        bincode::serialize(&message.structure)
            .map_err(|e| StoreError::SerializeError(e.to_string()))?
            .into(),
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
    part_id: usize,
    language_detector: &mut LanguageDetector,
) {
    if let Some(HeaderValue::Text(subject)) = message.headers_rfc.remove(&HeaderName::Subject) {
        document.add_text(
            MessageField::Attachment.into(),
            part_id as FieldNumber,
            Text::Full(language_detector.detect_wrap(subject.into_owned().into())),
            false,
            false,
        );
    }
    for part in message.parts.drain(..) {
        match part {
            MessagePart::Text(text) => {
                document.add_text(
                    MessageField::Attachment.into(),
                    part_id as FieldNumber,
                    Text::Full(language_detector.detect_wrap(text.body.into_owned().into())),
                    false,
                    false,
                );
            }
            MessagePart::Html(html) => {
                document.add_text(
                    MessageField::Attachment.into(),
                    part_id as FieldNumber,
                    Text::Full(language_detector.detect_wrap(html_to_text(&html.body).into())),
                    false,
                    false,
                );
            }
            _ => (),
        }
    }
}

fn parse_address<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
    header_name: HeaderName,
    address: &Addr<'x>,
) {
    if let Some(name) = &address.name {
        parse_text(document, header_name, name);
    };
    if let Some(ref addr) = address.address {
        if addr.len() <= MAX_TOKEN_LENGTH {
            document.add_text(
                header_name.into(),
                0,
                Text::Keyword(addr.to_lowercase().into()),
                false,
                false,
            );
        }
    };
}

fn parse_address_group<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
    header_name: HeaderName,
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
    header_name: HeaderName,
    text: &str,
) {
    match header_name {
        HeaderName::Keywords
        | HeaderName::ContentLanguage
        | HeaderName::MimeVersion
        | HeaderName::MessageId
        | HeaderName::References
        | HeaderName::ContentId
        | HeaderName::ResentMessageId => {
            if text.len() <= MAX_TOKEN_LENGTH {
                document.add_text(
                    header_name.into(),
                    0,
                    Text::Keyword(text.to_lowercase().into()),
                    false,
                    false,
                );
            }
        }

        HeaderName::Subject => (),

        _ => {
            document.add_text(
                header_name.into(),
                0,
                Text::Tokenized(text.to_string().into()),
                false,
                false,
            );
        }
    }
}

fn parse_content_type<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
    header_name: HeaderName,
    content_type: &ContentType<'x>,
) {
    if content_type.c_type.len() <= MAX_TOKEN_LENGTH {
        document.add_text(
            header_name.into(),
            0,
            Text::Keyword(content_type.c_type.clone()),
            false,
            false,
        );
    }
    if let Some(subtype) = &content_type.c_subtype {
        if subtype.len() <= MAX_TOKEN_LENGTH {
            document.add_text(
                header_name.into(),
                0,
                Text::Keyword(subtype.clone()),
                false,
                false,
            );
        }
    }
    if let Some(attributes) = &content_type.attributes {
        for (key, value) in attributes {
            if key == "name" || key == "filename" {
                document.add_text(
                    header_name.into(),
                    0,
                    Text::Tokenized(value.clone()),
                    false,
                    false,
                );
            } else if value.len() <= MAX_TOKEN_LENGTH {
                document.add_text(
                    header_name.into(),
                    0,
                    Text::Keyword(value.to_lowercase().into()),
                    false,
                    false,
                );
            }
        }
    }
}

fn parse_datetime(
    document: &mut DocumentWriter<impl UncommittedDocumentId>,
    header_name: HeaderName,
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
                0,
                datetime.timestamp() as u64,
                false,
                true,
            );
        }
    }
}

#[allow(clippy::manual_flatten)]
fn add_addr_sort<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
    header_name: HeaderName,
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
            0,
            Text::Tokenized(text.into()),
            false,
            true,
        );
    };
}

fn parse_header<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
    header_name: HeaderName,
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

pub fn header_to_jmap_date(header: HeaderValue) -> JSONValue<JMAPMailProperties> {
    match header {
        HeaderValue::DateTime(datetime) => JSONValue::String(datetime.to_iso8601().into()),
        HeaderValue::Collection(list) => JSONValue::Array(
            list.into_iter()
                .filter_map(|datetime| {
                    if let HeaderValue::DateTime(datetime) = datetime {
                        Some(JSONValue::String(datetime.to_iso8601().into()))
                    } else {
                        None
                    }
                })
                .collect(),
        ),
        _ => JSONValue::Null,
    }
}

pub fn header_to_jmap_id(header: HeaderValue) -> JSONValue<JMAPMailProperties> {
    match header {
        HeaderValue::Text(id) => JSONValue::Array(vec![JSONValue::String(id)]),
        HeaderValue::TextList(ids) => {
            JSONValue::Array(ids.into_iter().map(JSONValue::String).collect())
        }
        HeaderValue::Collection(list) => JSONValue::Array(
            list.into_iter()
                .filter_map(|ids| match header_to_jmap_id(ids) {
                    JSONValue::Null => None,
                    value => Some(value),
                })
                .collect(),
        ),
        _ => JSONValue::Null,
    }
}

pub fn header_to_jmap_text(header: HeaderValue) -> JSONValue<JMAPMailProperties> {
    match header {
        HeaderValue::Text(text) => JSONValue::String(text),
        HeaderValue::TextList(textlist) => JSONValue::String(textlist.join(", ").into()),
        HeaderValue::Collection(list) => JSONValue::Array(
            list.into_iter()
                .filter_map(|ids| match header_to_jmap_text(ids) {
                    JSONValue::Null => None,
                    value => Some(value),
                })
                .collect(),
        ),
        _ => JSONValue::Null,
    }
}

pub fn header_to_jmap_url(header: HeaderValue) -> JSONValue<JMAPMailProperties> {
    match header {
        HeaderValue::Address(Addr {
            address: Some(addr),
            ..
        }) if addr.contains(':') => JSONValue::Array(vec![JSONValue::String(addr)]),
        HeaderValue::AddressList(textlist) => JSONValue::Array(
            textlist
                .into_iter()
                .filter_map(|addr| match addr {
                    Addr {
                        address: Some(addr),
                        ..
                    } if addr.contains(':') => Some(JSONValue::String(addr)),
                    _ => None,
                })
                .collect(),
        ),
        HeaderValue::Collection(list) => JSONValue::Array(
            list.into_iter()
                .filter_map(|ids| match header_to_jmap_url(ids) {
                    JSONValue::Null => None,
                    value => Some(value),
                })
                .collect(),
        ),
        _ => JSONValue::Null,
    }
}

pub fn header_to_jmap_address<'x>(header: HeaderValue<'x>) -> JSONValue<'x, JMAPMailProperties> {
    let addr_to_jmap = |addr: Addr<'x>| -> JSONValue<'x, JMAPMailProperties> {
        let mut jmap_addr = HashMap::with_capacity(2);
        jmap_addr.insert(
            JMAPMailProperties::Email,
            JSONValue::String(addr.address.unwrap()),
        );
        jmap_addr.insert(
            JMAPMailProperties::Name,
            if let Some(name) = addr.name {
                JSONValue::String(name)
            } else {
                JSONValue::Null
            },
        );
        JSONValue::Properties(jmap_addr)
    };

    let addrlist_to_jmap = |addrlist: Vec<Addr<'x>>| -> JSONValue<'x, JMAPMailProperties> {
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
    };

    let group_to_jmap = |group: Group<'x>| -> JSONValue<'x, JMAPMailProperties> {
        let mut jmap_addr = HashMap::with_capacity(2);
        jmap_addr.insert(
            JMAPMailProperties::Addresses,
            addrlist_to_jmap(group.addresses),
        );
        jmap_addr.insert(
            JMAPMailProperties::Name,
            if let Some(name) = group.name {
                JSONValue::String(name)
            } else {
                JSONValue::Null
            },
        );
        JSONValue::Properties(jmap_addr)
    };

    match header {
        HeaderValue::Address(
            addr @ Addr {
                address: Some(_), ..
            },
        ) => JSONValue::Array(vec![addr_to_jmap(addr)]),
        HeaderValue::AddressList(addrlist) => addrlist_to_jmap(addrlist),
        HeaderValue::Group(group) => group_to_jmap(group),

        HeaderValue::GroupList(grouplist) => {
            JSONValue::Array(grouplist.into_iter().map(group_to_jmap).collect())
        }
        HeaderValue::Collection(list) => JSONValue::Array(
            list.into_iter()
                .filter_map(|ids| match header_to_jmap_address(ids) {
                    JSONValue::Null => None,
                    value => Some(value),
                })
                .collect(),
        ),
        _ => JSONValue::Null,
    }
}
