use std::borrow::Cow;

use chrono::{FixedOffset, LocalResult, TimeZone, Utc};
use mail_parser::{
    Addr, ContentType, DateTime, Group, HeaderName, HeaderValue, Message, MessagePart,
};
use store::{
    document::{
        DocumentBuilder, IndexOptions, OptionValue, MAX_ID_LENGTH, MAX_SORT_FIELD_LENGTH,
        MAX_TOKEN_LENGTH,
    },
};

use crate::MailField;

#[derive(Debug)]
pub struct MessageParseError;

#[inline(always)]
fn to_mail_field(name: &HeaderName) -> u8 {
    MailField::HeaderField as u8 + *name as u8
}

fn parse_address<'x>(
    builder: &mut DocumentBuilder<'x>,
    header_name: &HeaderName,
    address: Addr<'x>,
) {
    if let Some(name) = address.name {
        parse_text(builder, header_name, name);
    };
    if let Some(ref addr) = address.address {
        if addr.len() <= MAX_TOKEN_LENGTH {
            builder.add_text(
                to_mail_field(header_name),
                addr.to_lowercase().into(),
                <OptionValue>::None,
            );
        }
    };
}

fn parse_address_group<'x>(
    builder: &mut DocumentBuilder<'x>,
    header_name: &HeaderName,
    mut group: Group<'x>,
) {
    if let Some(name) = group.name {
        parse_text(builder, header_name, name);
    };

    for address in group.addresses.drain(..) {
        parse_address(builder, header_name, address);
    }
}

fn parse_text<'x>(builder: &mut DocumentBuilder<'x>, header_name: &HeaderName, text: Cow<'x, str>) {
    match header_name {
        HeaderName::Subject => {
            builder.add_text(
                to_mail_field(header_name),
                text.to_lowercase().into(),
                <OptionValue>::FullText | <OptionValue>::Sortable,
            );
        }

        HeaderName::Keywords | HeaderName::ContentLanguage | HeaderName::MimeVersion => {
            if text.len() <= MAX_TOKEN_LENGTH {
                builder.add_text(
                    to_mail_field(header_name),
                    text.to_lowercase().into(),
                    <OptionValue>::None,
                );
            }
        }

        HeaderName::MessageId
        | HeaderName::References
        | HeaderName::ContentId
        | HeaderName::ResentMessageId => {
            if text.len() <= MAX_ID_LENGTH {
                builder.add_text(to_mail_field(header_name), text, <OptionValue>::None);
            }
        }

        _ => {
            builder.add_text(to_mail_field(header_name), text, <OptionValue>::Tokenized);
        }
    }
}

fn parse_content_type<'x>(
    builder: &mut DocumentBuilder<'x>,
    header_name: &HeaderName,
    content_type: ContentType<'x>,
) {
    if content_type.c_type.len() <= MAX_TOKEN_LENGTH {
        builder.add_text(
            to_mail_field(header_name),
            content_type.c_type,
            <OptionValue>::None,
        );
    }
    if let Some(subtype) = content_type.c_subtype {
        if subtype.len() <= MAX_TOKEN_LENGTH {
            builder.add_text(to_mail_field(header_name), subtype, <OptionValue>::None);
        }
    }
    if let Some(attributes) = content_type.attributes {
        for (key, value) in attributes {
            if key == "name" || key == "filename" {
                builder.add_text(to_mail_field(header_name), value, <OptionValue>::Tokenized);
            } else if value.len() <= MAX_TOKEN_LENGTH {
                builder.add_text(
                    to_mail_field(header_name),
                    value.to_lowercase().into(),
                    <OptionValue>::None,
                );
            }
        }
    }
}

fn parse_datetime<'x>(
    builder: &mut DocumentBuilder<'x>,
    header_name: &HeaderName,
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
            builder.add_long_int(
                to_mail_field(header_name),
                datetime.timestamp() as u64,
                <OptionValue>::Sortable,
            );
        }
    }
}

#[allow(clippy::manual_flatten)]
fn add_addr_sort<'x>(
    builder: &mut DocumentBuilder<'x>,
    header_name: &HeaderName,
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
        builder.add_text(
            to_mail_field(header_name),
            text.into(),
            <OptionValue>::Sortable,
        );
    };
}

fn parse_header<'x>(
    builder: &mut DocumentBuilder<'x>,
    header_name: &HeaderName,
    header_value: HeaderValue<'x>,
) {
    match header_value {
        HeaderValue::Address(address) => {
            parse_address(builder, header_name, address);
        }
        HeaderValue::AddressList(address_list) => {
            for item in address_list {
                parse_address(builder, header_name, item);
            }
        }
        HeaderValue::Group(group) => {
            parse_address_group(builder, header_name, group);
        }
        HeaderValue::GroupList(group_list) => {
            for item in group_list {
                parse_address_group(builder, header_name, item);
            }
        }
        HeaderValue::Text(text) => {
            parse_text(builder, header_name, text);
        }
        HeaderValue::TextList(text_list) => {
            for item in text_list {
                parse_text(builder, header_name, item);
            }
        }
        HeaderValue::DateTime(date_time) => {
            parse_datetime(builder, header_name, date_time);
        }
        HeaderValue::ContentType(content_type) => {
            parse_content_type(builder, header_name, content_type);
        }
        HeaderValue::Collection(header_value) => {
            for item in header_value {
                parse_header(builder, header_name, item);
            }
        }
        HeaderValue::Empty => (),
    }
}

pub fn parse_message(bytes: &[u8]) -> Result<DocumentBuilder, MessageParseError> {
    let message = Message::parse(bytes).ok_or(MessageParseError)?;
    let mut builder = DocumentBuilder::new();

    builder.add_integer(
        MailField::Size as u8,
        bytes.len() as u32,
        <OptionValue>::Sortable,
    );
    builder.add_long_int(
        MailField::ReceivedAt as u8,
        Utc::now().timestamp() as u64,
        <OptionValue>::Sortable,
    );

    for (header_name, header_value) in message.headers {
        if let HeaderName::From | HeaderName::To | HeaderName::Cc | HeaderName::Bcc = header_name {
            add_addr_sort(&mut builder, &header_name, &header_value);
        }
        parse_header(&mut builder, &header_name, header_value);
    }

    let mut set_pos: usize = 0;
    for text_body in message.text_body {
        match text_body {
            mail_parser::InlinePart::Text(body) => {
                builder.add_text(
                    MailField::Body as u8,
                    body.contents,
                    <OptionValue>::FullText
                        | <OptionValue>::Stored
                        | <OptionValue>::set_pos(set_pos),
                );
                set_pos += 1;
            }
            mail_parser::InlinePart::InlineBinary(_) => todo!(),
        }
    }

    set_pos = 0;
    for html_body in message.html_body {
        match html_body {
            mail_parser::InlinePart::Text(body) => {
                builder.add_text(
                    MailField::Body as u8,
                    body.contents,
                    <OptionValue>::Stored | <OptionValue>::set_pos(set_pos),
                );
                set_pos += 1;
            }
            mail_parser::InlinePart::InlineBinary(_) => (), //TODO: add attachment
        }
    }

    set_pos = 0;
    for attachment in message.attachments {
        match attachment {
            MessagePart::Text(text) => {
                builder.add_text(
                    MailField::Attachment as u8,
                    text.contents,
                    <OptionValue>::Stored
                        | <OptionValue>::FullText
                        | <OptionValue>::set_pos(set_pos),
                );
            }
            MessagePart::Binary(blob) | MessagePart::InlineBinary(blob) => {
                builder.add_blob(
                    MailField::Attachment as u8,
                    blob.contents,
                    <OptionValue>::Stored | <OptionValue>::set_pos(set_pos),
                );
            }
            MessagePart::Message(_) => (), //TODO: parse message
        }
        set_pos += 1;
    }

    Ok(builder)
}
