use std::borrow::Cow;

use chrono::{FixedOffset, LocalResult, TimeZone, Utc};
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

use crate::{MessageField, MessageParts};

fn parse_address<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
    header_name: HeaderName,
    address: Addr<'x>,
) {
    if let Some(name) = address.name {
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
    mut group: Group<'x>,
) {
    if let Some(name) = group.name {
        parse_text(document, header_name, name);
    };

    for address in group.addresses.drain(..) {
        parse_address(document, header_name, address);
    }
}

fn parse_text<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
    header_name: HeaderName,
    text: Cow<'x, str>,
) {
    match header_name {
        HeaderName::Keywords | HeaderName::ContentLanguage | HeaderName::MimeVersion => {
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

        HeaderName::Subject
        | HeaderName::MessageId
        | HeaderName::References
        | HeaderName::ContentId
        | HeaderName::ResentMessageId => (),

        _ => {
            document.add_text(header_name.into(), 0, Text::Tokenized(text), false, false);
        }
    }
}

fn parse_content_type<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
    header_name: HeaderName,
    content_type: ContentType<'x>,
) {
    if content_type.c_type.len() <= MAX_TOKEN_LENGTH {
        document.add_text(
            header_name.into(),
            0,
            Text::Keyword(content_type.c_type),
            false,
            false,
        );
    }
    if let Some(subtype) = content_type.c_subtype {
        if subtype.len() <= MAX_TOKEN_LENGTH {
            document.add_text(header_name.into(), 0, Text::Keyword(subtype), false, false);
        }
    }
    if let Some(attributes) = content_type.attributes {
        for (key, value) in attributes {
            if key == "name" || key == "filename" {
                document.add_text(header_name.into(), 0, Text::Tokenized(value), false, false);
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
    date_time: DateTime,
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
    header_value: HeaderValue<'x>,
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

pub fn build_message_document<'x>(
    document: &mut DocumentWriter<'x, impl UncommittedDocumentId>,
    mut message: Message<'x>,
    received_at: Option<i64>,
) -> store::Result<(Vec<Cow<'x, str>>, String)> {
    let mut message_parts = MessageParts {
        html_body: message.html_body,
        text_body: message.text_body,
        attachments: message.attachments,
        offset_body: message.offset_body,
        size: message.raw_message.len(),
        received_at: received_at.unwrap_or_else(|| Utc::now().timestamp()),
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
    document.add_blob(
        MessageField::Internal.into(),
        crate::MESSAGE_HEADERS,
        bincode::serialize(&message.headers_rfc)
            .map_err(|e| StoreError::SerializeError(e.to_string()))?
            .into(),
    );
    document.add_blob(
        MessageField::Internal.into(),
        crate::MESSAGE_RAW,
        message.raw_message,
    );

    // Obtain thread name
    let thread_name = message
        .headers_rfc
        .remove(&HeaderName::Subject)
        .and_then(|subject| {
            if let HeaderValue::Text(subject) = subject {
                let (thread_name, language) = match thread_name(&subject) {
                    thread_name if !thread_name.is_empty() => (
                        thread_name.to_string(),
                        language_detector.detect(thread_name, MIN_LANGUAGE_SCORE),
                    ),
                    _ => (
                        "!".to_string(),
                        language_detector.detect(&subject, MIN_LANGUAGE_SCORE),
                    ),
                };

                document.add_text(
                    HeaderName::Subject.into(),
                    0,
                    Text::Full((subject, language)),
                    false,
                    false,
                );

                Some(thread_name)
            } else {
                None
            }
        })
        .unwrap_or_else(|| "!".to_string());

    // Build a list containing all IDs that appear in the header
    let mut reference_ids = Vec::new();
    for header_name in [
        HeaderName::MessageId,
        HeaderName::InReplyTo,
        HeaderName::References,
        HeaderName::ResentMessageId,
    ] {
        match message
            .headers_rfc
            .remove(&header_name)
            .unwrap_or(HeaderValue::Empty)
        {
            HeaderValue::Text(text) if text.len() <= MAX_ID_LENGTH => reference_ids.push(text),
            HeaderValue::TextList(mut list) => {
                reference_ids.extend(list.drain(..).filter(|text| text.len() <= MAX_ID_LENGTH));
            }
            HeaderValue::Collection(col) => {
                for item in col {
                    match item {
                        HeaderValue::Text(text) if text.len() <= MAX_ID_LENGTH => {
                            reference_ids.push(text)
                        }
                        HeaderValue::TextList(mut list) => reference_ids
                            .extend(list.drain(..).filter(|text| text.len() <= MAX_ID_LENGTH)),
                        _ => (),
                    }
                }
            }
            _ => (),
        }
    }

    for (header_name, header_value) in message.headers_rfc {
        if let HeaderName::From | HeaderName::To | HeaderName::Cc | HeaderName::Bcc = header_name {
            add_addr_sort(document, header_name, &header_value);
        }
        parse_header(document, header_name, header_value);
    }

    let mut has_attachment = false;
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
                        has_attachment = true;
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
                        has_attachment = true;
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
                if !has_attachment {
                    has_attachment = true;
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
                if !has_attachment {
                    has_attachment = true;
                }
                nested_headers.push(nested_message.headers);
                match nested_message.body {
                    MessageAttachment::Parsed(mut message) => {
                        if let Some(HeaderValue::Text(subject)) =
                            message.headers_rfc.remove(&HeaderName::Subject)
                        {
                            document.add_text(
                                MessageField::Attachment.into(),
                                part_id as FieldNumber,
                                Text::Full(language_detector.detect_wrap(subject)),
                                false,
                                false,
                            );
                        }
                        for part in message.parts {
                            match part {
                                MessagePart::Text(text) => {
                                    document.add_text(
                                        MessageField::Attachment.into(),
                                        part_id as FieldNumber,
                                        Text::Full(language_detector.detect_wrap(text.body)),
                                        false,
                                        false,
                                    );
                                }
                                MessagePart::Html(html) => {
                                    document.add_text(
                                        MessageField::Attachment.into(),
                                        part_id as FieldNumber,
                                        Text::Full(
                                            language_detector
                                                .detect_wrap(html_to_text(&html.body).into()),
                                        ),
                                        false,
                                        false,
                                    );
                                }
                                _ => (),
                            }
                        }
                        document.add_blob(
                            MessageField::Attachment.into(),
                            part_id as FieldNumber,
                            message.raw_message,
                        );
                    }
                    MessageAttachment::Raw(raw_message) => {
                        if let Some(message) = &Message::parse(raw_message.as_ref()) {
                            if let Some(HeaderValue::Text(subject)) =
                                message.headers_rfc.get(&HeaderName::Subject)
                            {
                                document.add_text(
                                    MessageField::Attachment.into(),
                                    part_id as FieldNumber,
                                    Text::Full(
                                        language_detector.detect_wrap(subject.to_string().into()),
                                    ),
                                    false,
                                    false,
                                );
                            }
                            for part in &message.parts {
                                match part {
                                    MessagePart::Text(text) => {
                                        document.add_text(
                                            MessageField::Attachment.into(),
                                            part_id as FieldNumber,
                                            Text::Full(
                                                language_detector
                                                    .detect_wrap(text.body.to_string().into()),
                                            ),
                                            false,
                                            false,
                                        );
                                    }
                                    MessagePart::Html(html) => {
                                        document.add_text(
                                            MessageField::Attachment.into(),
                                            part_id as FieldNumber,
                                            Text::Full(
                                                language_detector
                                                    .detect_wrap(html_to_text(&html.body).into()),
                                            ),
                                            false,
                                            false,
                                        );
                                    }
                                    _ => (),
                                }
                            }
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

    if has_attachment {
        document.set_tag(MessageField::Attachment.into(), Tag::Id(0));
    }

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
        crate::MESSAGE_HEADERS_PARTS,
        bincode::serialize(&message_parts)
            .map_err(|e| StoreError::SerializeError(e.to_string()))?
            .into(),
    );

    document.add_blob(
        MessageField::Internal.into(),
        crate::MESSAGE_HEADERS_STRUCTURE,
        bincode::serialize(&message.structure)
            .map_err(|e| StoreError::SerializeError(e.to_string()))?
            .into(),
    );

    if let Some(default_language) = language_detector.most_frequent_language() {
        document.set_default_language(*default_language);
    }

    // TODO use content language when available
    // TODO index PDF, Doc, Excel, etc.

    Ok((reference_ids, thread_name))
}
