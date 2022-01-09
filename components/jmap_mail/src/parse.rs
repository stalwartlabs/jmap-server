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
    field::{FieldOptions, FullText, Text},
    Integer, LongInteger, StoreError, Tag, UncommittedDocumentId,
};

use crate::{
    JMAPMailProperties, MessageField, MessageMetadata, MessageRawHeaders, MESSAGE_HEADERS,
    MESSAGE_HEADERS_PARTS, MESSAGE_HEADERS_RAW, MESSAGE_METADATA, MESSAGE_PARTS, MESSAGE_RAW,
    MESSAGE_STRUCTURE,
};

pub fn build_message_document<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
    message: Message<'x>,
    received_at: Option<i64>,
) -> store::Result<(Vec<Cow<'x, str>>, String)> {
    let mut message_metadata = MessageMetadata {
        html_body: message.html_body,
        text_body: message.text_body,
        attachments: message.attachments,
        size: message.raw_message.len(),
        received_at: received_at.unwrap_or_else(|| Utc::now().timestamp()),
        has_attachments: false,
    };
    let mut total_parts = message.parts.len();
    let mut part_headers = Vec::with_capacity(message.parts.len());
    let mut language_detector = LanguageDetector::new();

    document.add_integer(
        MessageField::Size.into(),
        message_metadata.size as Integer,
        FieldOptions::Sort,
    );
    document.add_long_int(
        MessageField::ReceivedAt.into(),
        message_metadata.received_at as LongInteger,
        FieldOptions::Sort,
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
                jmap_headers.insert(header_name, header_to_jmap_address(header_value, false));
            }
            HeaderName::ReplyTo
            | HeaderName::Sender
            | HeaderName::ResentTo
            | HeaderName::ResentFrom
            | HeaderName::ResentBcc
            | HeaderName::ResentCc
            | HeaderName::ResentSender => {
                jmap_headers.insert(header_name, header_to_jmap_address(header_value, false));
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
                        Text::Full(FullText::new_lang(subject.to_string().into(), language)),
                        FieldOptions::None,
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

    part_headers.push(mime_headers);

    for (part_id, part) in message.parts.into_iter().enumerate() {
        match part {
            MessagePart::Html(html) => {
                part_headers.push(html.headers);

                let field = if let Some(pos) = message_metadata
                    .text_body
                    .iter()
                    .position(|&p| p == part_id)
                {
                    message_metadata.text_body[pos] = total_parts;
                    MessageField::Body
                } else if message_metadata.html_body.contains(&part_id) {
                    MessageField::Body
                } else {
                    message_metadata.has_attachments = true;
                    MessageField::Attachment
                };

                document.add_text(
                    field.clone().into(),
                    Text::Full(FullText::new(
                        html_to_text(html.body.as_ref()).into(),
                        &mut language_detector,
                    )),
                    if field == MessageField::Body {
                        let blob_id = total_parts;
                        total_parts += 1;
                        FieldOptions::BlobStore(blob_id + MESSAGE_PARTS)
                    } else {
                        FieldOptions::None
                    },
                );

                document.add_text(
                    field.into(),
                    Text::Default(html.body),
                    FieldOptions::BlobStore(part_id + MESSAGE_PARTS),
                );
            }
            MessagePart::Text(text) => {
                part_headers.push(text.headers);
                let field = if let Some(pos) = message_metadata
                    .html_body
                    .iter()
                    .position(|&p| p == part_id)
                {
                    document.add_text(
                        MessageField::Body.into(),
                        Text::Default(text_to_html(text.body.as_ref()).into()),
                        FieldOptions::BlobStore(total_parts + MESSAGE_PARTS),
                    );
                    message_metadata.html_body[pos] = total_parts;
                    total_parts += 1;
                    MessageField::Body
                } else if message_metadata.text_body.contains(&part_id) {
                    MessageField::Body
                } else {
                    message_metadata.has_attachments = true;
                    MessageField::Attachment
                };

                document.add_text(
                    field.into(),
                    Text::Full(FullText::new(text.body, &mut language_detector)),
                    FieldOptions::BlobStore(part_id + MESSAGE_PARTS),
                );
            }
            MessagePart::Binary(binary) => {
                if !message_metadata.has_attachments {
                    message_metadata.has_attachments = true;
                }
                part_headers.push(binary.headers);
                document.add_blob(
                    MessageField::Attachment.into(),
                    binary.body,
                    FieldOptions::BlobStore(part_id + MESSAGE_PARTS),
                );
            }
            MessagePart::InlineBinary(binary) => {
                part_headers.push(binary.headers);
                document.add_blob(
                    MessageField::Attachment.into(),
                    binary.body,
                    FieldOptions::BlobStore(part_id + MESSAGE_PARTS),
                );
            }
            MessagePart::Message(nested_message) => {
                if !message_metadata.has_attachments {
                    message_metadata.has_attachments = true;
                }
                part_headers.push(nested_message.headers);
                match nested_message.body {
                    MessageAttachment::Parsed(mut message) => {
                        parse_attached_message(document, &mut message, &mut language_detector);
                        document.add_blob(
                            MessageField::Attachment.into(),
                            message.raw_message,
                            FieldOptions::BlobStore(part_id + MESSAGE_PARTS),
                        );
                    }
                    MessageAttachment::Raw(raw_message) => {
                        if let Some(message) = &mut Message::parse(raw_message.as_ref()) {
                            parse_attached_message(document, message, &mut language_detector)
                        }
                        document.add_blob(
                            MessageField::Attachment.into(),
                            raw_message,
                            FieldOptions::BlobStore(part_id + MESSAGE_PARTS),
                        );
                    }
                }
            }
            MessagePart::Multipart(header) => {
                part_headers.push(header);
            }
        };
    }

    if message_metadata.has_attachments {
        document.set_tag(MessageField::Attachment.into(), Tag::Id(0));
    }

    document.add_blob(
        MessageField::Internal.into(),
        bincode::serialize(&jmap_headers)
            .map_err(|e| StoreError::SerializeError(e.to_string()))?
            .into(),
        FieldOptions::BlobStore(MESSAGE_HEADERS),
    );

    document.add_blob(
        MessageField::Internal.into(),
        message.raw_message,
        FieldOptions::BlobStore(MESSAGE_RAW),
    );

    document.add_blob(
        MessageField::Internal.into(),
        bincode::serialize(&MessageRawHeaders {
            size: message.offset_body,
            headers: message.headers_raw,
        })
        .map_err(|e| StoreError::SerializeError(e.to_string()))?
        .into(),
        FieldOptions::BlobStore(MESSAGE_HEADERS_RAW),
    );

    document.add_blob(
        MessageField::Internal.into(),
        bincode::serialize(&part_headers)
            .map_err(|e| StoreError::SerializeError(e.to_string()))?
            .into(),
        FieldOptions::BlobStore(MESSAGE_HEADERS_PARTS),
    );

    document.add_blob(
        MessageField::Internal.into(),
        bincode::serialize(&message_metadata)
            .map_err(|e| StoreError::SerializeError(e.to_string()))?
            .into(),
        FieldOptions::BlobStore(MESSAGE_METADATA),
    );

    document.add_blob(
        MessageField::Internal.into(),
        bincode::serialize(&message.structure)
            .map_err(|e| StoreError::SerializeError(e.to_string()))?
            .into(),
        FieldOptions::BlobStore(MESSAGE_STRUCTURE),
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
    if let Some(HeaderValue::Text(subject)) = message.headers_rfc.remove(&HeaderName::Subject) {
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
                Text::Keyword(addr.to_lowercase().into()),
                FieldOptions::None,
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
                    Text::Keyword(text.to_lowercase().into()),
                    FieldOptions::None,
                );
            }
        }

        HeaderName::Subject => (),

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
    header_name: HeaderName,
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
                datetime.timestamp() as u64,
                FieldOptions::Sort,
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
            Text::Tokenized(text.into()),
            FieldOptions::Sort,
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

pub fn header_to_jmap_address<'x>(
    header: HeaderValue<'x>,
    convert_to_group: bool,
) -> JSONValue<'x, JMAPMailProperties> {
    fn addr_to_jmap<'x>(addr: Addr<'x>) -> JSONValue<'x, JMAPMailProperties> {
        let mut jmap_addr = HashMap::with_capacity(2);
        jmap_addr.insert(
            JMAPMailProperties::Email,
            JSONValue::String(addr.address.unwrap()),
        );
        jmap_addr.insert(
            JMAPMailProperties::Name,
            addr.name.map_or(JSONValue::Null, JSONValue::String),
        );
        JSONValue::Properties(jmap_addr)
    }

    fn addrlist_to_jmap<'x>(addrlist: Vec<Addr<'x>>) -> JSONValue<'x, JMAPMailProperties> {
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

    fn group_to_jmap<'x>(group: Group<'x>) -> JSONValue<'x, JMAPMailProperties> {
        let mut jmap_addr = HashMap::with_capacity(2);
        jmap_addr.insert(
            JMAPMailProperties::Addresses,
            addrlist_to_jmap(group.addresses),
        );
        jmap_addr.insert(
            JMAPMailProperties::Name,
            group.name.map_or(JSONValue::Null, JSONValue::String),
        );
        JSONValue::Properties(jmap_addr)
    }

    fn into_group<'x>(
        addresses: JSONValue<'x, JMAPMailProperties<'x>>,
    ) -> JSONValue<'x, JMAPMailProperties<'x>> {
        let mut email = HashMap::new();
        email.insert(JMAPMailProperties::Name, JSONValue::Null);
        email.insert(JMAPMailProperties::Addresses, addresses);
        JSONValue::Array(vec![JSONValue::Properties(email)])
    }

    match header {
        HeaderValue::Address(
            addr @ Addr {
                address: Some(_), ..
            },
        ) => {
            let value = JSONValue::Array(vec![addr_to_jmap(addr)]);
            if !convert_to_group {
                value
            } else {
                into_group(value)
            }
        }
        HeaderValue::AddressList(addrlist) => {
            let value = addrlist_to_jmap(addrlist);
            if !convert_to_group {
                value
            } else {
                into_group(value)
            }
        }
        HeaderValue::Group(group) => JSONValue::Array(vec![group_to_jmap(group)]),
        HeaderValue::GroupList(grouplist) => {
            JSONValue::Array(grouplist.into_iter().map(group_to_jmap).collect())
        }
        HeaderValue::Collection(list) => {
            let convert_to_group = list
                .iter()
                .any(|item| matches!(item, HeaderValue::Group(_) | HeaderValue::GroupList(_)));
            JSONValue::Array(
                list.into_iter()
                    .filter_map(|ids| match header_to_jmap_address(ids, convert_to_group) {
                        JSONValue::Null => None,
                        value => Some(value),
                    })
                    .collect(),
            )
        }
        _ => JSONValue::Null,
    }
}
