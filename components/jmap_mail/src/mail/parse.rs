use std::{
    collections::{hash_map::Entry, HashMap},
    iter::FromIterator,
    vec,
};

use jmap::{
    id::{blob::JMAPBlob, JMAPIdSerialize},
    jmap_store::{blob::InnerBlobFnc, parse::ParseObject},
    protocol::json::JSONValue,
    request::parse::ParseRequest,
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
use nlp::{
    lang::{LanguageDetector, MIN_LANGUAGE_SCORE},
    Language,
};
use store::{
    batch::{Document, MAX_ID_LENGTH, MAX_SORT_FIELD_LENGTH, MAX_TOKEN_LENGTH},
    blob::BlobId,
    field::{IndexOptions, Options, Text},
    leb128::Leb128,
    serialize::StoreSerialize,
    AccountId, Integer, JMAPStore, LongInteger, Store, StoreError, Tag,
};
use store::{
    chrono::{LocalResult, SecondsFormat, TimeZone, Utc},
    DocumentId,
};

use crate::mail::{
    get::{
        add_body_parts, add_body_structure, add_body_value, add_raw_header,
        transform_json_emailaddress, transform_json_string, transform_json_stringlist,
        transform_rfc_header, MailGetArguments,
    },
    HeaderName, JMAPMailMimeHeaders, MailBodyProperty, MailHeaderForm, MailHeaderProperty,
    MailProperty, MessageData, MessageField, MessageOutline, MimePart, MimePartType,
};

pub struct ParseMail {
    pub account_id: AccountId,
    pub properties: Vec<MailProperty>,
    pub arguments: MailGetArguments,
}

impl<'y, T> ParseObject<'y, T> for ParseMail
where
    T: for<'x> Store<'x> + 'static,
{
    fn new(_store: &'y JMAPStore<T>, request: &mut ParseRequest) -> jmap::Result<Self> {
        Ok(ParseMail {
            account_id: request.account_id,
            properties: request
                .arguments
                .remove("properties")
                .unwrap_or_default()
                .parse_array_items(true)?
                .unwrap_or_else(|| {
                    vec![
                        MailProperty::MessageId,
                        MailProperty::InReplyTo,
                        MailProperty::References,
                        MailProperty::Sender,
                        MailProperty::From,
                        MailProperty::To,
                        MailProperty::Cc,
                        MailProperty::Bcc,
                        MailProperty::ReplyTo,
                        MailProperty::Subject,
                        MailProperty::SentAt,
                        MailProperty::HasAttachment,
                        MailProperty::Preview,
                        MailProperty::BodyValues,
                        MailProperty::TextBody,
                        MailProperty::HtmlBody,
                        MailProperty::Attachments,
                    ]
                }),
            arguments: MailGetArguments::parse_arguments(std::mem::take(&mut request.arguments))?,
        })
    }

    fn parse_blob(&self, blob_id: JMAPBlob, blob: Vec<u8>) -> jmap::Result<Option<JSONValue>> {
        Ok(Message::parse(&blob)
            .map(|message| self.build_message_response(message, &blob_id, &blob)))
    }

    fn inner_blob_fnc() -> InnerBlobFnc {
        get_message_blob
    }
}

impl ParseMail {
    fn build_message_response(
        &self,
        mut message: Message,
        blob_id: &JMAPBlob,
        raw_message: &[u8],
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
            mime_parts.push(MimePart::new_part(mime_headers));
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
                            blobs.len().into(),
                            false,
                        ));
                        blobs.push(value);
                        total_parts += 1;
                    } else if !html_body.contains(&part_id) {
                        has_attachments = true;
                    }
                    mime_parts.push(MimePart::new_html(
                        mime_parts_to_jmap(html.headers_rfc, html.body.len()),
                        blobs.len().into(),
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
                            blobs.len().into(),
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
                        blobs.len().into(),
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
                    mime_parts.push(MimePart::new_binary(
                        mime_parts_to_jmap(binary.headers_rfc, binary.body.len()),
                        blobs.len().into(),
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
                    mime_parts.push(MimePart::new_binary(
                        mime_parts_to_jmap(binary.headers_rfc, binary.body.len()),
                        blobs.len().into(),
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
                    mime_parts.push(MimePart::new_binary(
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
                        blob_index.into(),
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
                    mime_parts.push(MimePart::new_part(mime_parts_to_jmap(part.headers_rfc, 0)));
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

        let mut result = HashMap::with_capacity(self.properties.len());

        for property in &self.properties {
            result.insert(
                property.to_string(),
                match property {
                    MailProperty::Id
                    | MailProperty::ThreadId
                    | MailProperty::MailboxIds
                    | MailProperty::ReceivedAt
                    | MailProperty::Keywords => JSONValue::Null,

                    MailProperty::BlobId => blob_id.to_jmap_string().into(),
                    MailProperty::Size => raw_message.len().into(),
                    MailProperty::MessageId
                    | MailProperty::References
                    | MailProperty::InReplyTo => {
                        if let Some(message_id) =
                            message.headers_rfc.remove(&property.as_rfc_header())
                        {
                            let (value, is_collection) = header_to_jmap_id(message_id);
                            transform_json_stringlist(value, is_collection, false)
                        } else {
                            JSONValue::Null
                        }
                    }
                    MailProperty::Sender
                    | MailProperty::From
                    | MailProperty::To
                    | MailProperty::Cc
                    | MailProperty::Bcc
                    | MailProperty::ReplyTo => {
                        if let Some(addr) = message.headers_rfc.remove(&property.as_rfc_header()) {
                            let (value, is_grouped, is_collection) =
                                header_to_jmap_address(addr, false);
                            transform_json_emailaddress(
                                value,
                                is_grouped,
                                is_collection,
                                false,
                                false,
                            )
                        } else {
                            JSONValue::Null
                        }
                    }
                    MailProperty::Subject => {
                        if let Some(text) = message.headers_rfc.remove(&RfcHeader::Subject) {
                            let (value, _) = header_to_jmap_text(text);
                            transform_json_string(value, false)
                        } else {
                            JSONValue::Null
                        }
                    }
                    MailProperty::SentAt => {
                        if let Some(date) = message.headers_rfc.remove(&RfcHeader::Date) {
                            let (value, _) = header_to_jmap_date(date);
                            transform_json_string(value, false)
                        } else {
                            JSONValue::Null
                        }
                    }
                    MailProperty::Header(MailHeaderProperty {
                        form: form @ MailHeaderForm::Raw,
                        header,
                        all,
                    })
                    | MailProperty::Header(MailHeaderProperty {
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
                    MailProperty::Header(MailHeaderProperty {
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
                                    let (header_value, is_collection) =
                                        header_to_jmap_id(header_value);
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
                    MailProperty::HasAttachment => has_attachments.into(),
                    MailProperty::Preview => {
                        if !text_body.is_empty() {
                            preview_text(
                                String::from_utf8_lossy(
                                    &blobs[text_body
                                        .get(0)
                                        .and_then(|p| mime_parts.get(p + 1))
                                        .unwrap()
                                        .blob_id
                                        .unwrap()
                                        .size as usize],
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
                                        .blob_id
                                        .unwrap()
                                        .size as usize],
                                ),
                                256,
                            )
                            .to_string()
                            .into()
                        } else {
                            JSONValue::Null
                        }
                    }
                    MailProperty::BodyValues => {
                        let mut fetch_parts = Vec::new();
                        if self.arguments.fetch_all_body_values
                            || self.arguments.fetch_text_body_values
                        {
                            text_body.iter().for_each(|part| {
                                if let Some(mime_part) = mime_parts.get(*part + 1) {
                                    if let MimePartType::Html | MimePartType::Text =
                                        mime_part.mime_type
                                    {
                                        fetch_parts.push((mime_part, *part));
                                    }
                                }
                            });
                        }
                        if self.arguments.fetch_all_body_values
                            || self.arguments.fetch_html_body_values
                        {
                            html_body.iter().for_each(|part| {
                                if let Some(mime_part) = mime_parts.get(*part + 1) {
                                    if let MimePartType::Html | MimePartType::Text =
                                        mime_part.mime_type
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
                                                &blobs[mime_part.blob_id.unwrap().size as usize],
                                            )
                                            .into_owned(),
                                            &self.arguments,
                                        ),
                                    )
                                },
                            )))
                        } else {
                            JSONValue::Null
                        }
                    }
                    MailProperty::TextBody => add_body_parts(
                        &text_body,
                        &mime_parts,
                        &self.arguments.body_properties,
                        Some(raw_message),
                        Some(&message_outline),
                        &blob_id.id,
                    ),

                    MailProperty::HtmlBody => add_body_parts(
                        &html_body,
                        &mime_parts,
                        &self.arguments.body_properties,
                        Some(raw_message),
                        Some(&message_outline),
                        &blob_id.id,
                    ),

                    MailProperty::Attachments => add_body_parts(
                        &attachments,
                        &mime_parts,
                        &self.arguments.body_properties,
                        Some(raw_message),
                        Some(&message_outline),
                        &blob_id.id,
                    ),

                    MailProperty::BodyStructure => {
                        if let Some(body_structure) = add_body_structure(
                            &message_outline,
                            &mime_parts,
                            &self.arguments.body_properties,
                            Some(raw_message),
                            &blob_id.id,
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
}

pub fn get_message_blob(raw_message: &[u8], blob_index: u32) -> Option<Vec<u8>> {
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

pub trait MessageParser {
    fn parse_attached_message(&mut self, message: &mut Message);
    fn parse_address(&mut self, header_name: RfcHeader, address: &Addr);
    fn parse_address_group(&mut self, header_name: RfcHeader, group: &Group);
    fn parse_text(&mut self, header_name: RfcHeader, text: &str);
    fn parse_content_type(&mut self, header_name: RfcHeader, content_type: &ContentType);
    fn add_addr_sort(&mut self, header_name: RfcHeader, header_value: &HeaderValue);
    fn parse_header(&mut self, header_name: RfcHeader, header_value: &HeaderValue);
}

impl MessageParser for Document {
    fn parse_attached_message(&mut self, message: &mut Message) {
        if let Some(HeaderValue::Text(subject)) = message.headers_rfc.remove(&RfcHeader::Subject) {
            self.text(
                MessageField::Attachment,
                subject.into_owned(),
                Language::Unknown,
                IndexOptions::new().full_text(0),
            );
        }
        for part in message.parts.drain(..) {
            match part {
                MessagePart::Text(text) => {
                    self.text(
                        MessageField::Attachment,
                        text.body.into_owned(),
                        Language::Unknown,
                        IndexOptions::new().full_text(0),
                    );
                }
                MessagePart::Html(html) => {
                    self.text(
                        MessageField::Attachment,
                        html_to_text(&html.body),
                        Language::Unknown,
                        IndexOptions::new().full_text(0),
                    );
                }
                _ => (),
            }
        }
    }

    fn parse_address(&mut self, header_name: RfcHeader, address: &Addr) {
        if let Some(name) = &address.name {
            self.parse_text(header_name, name);
        };
        if let Some(ref addr) = address.address {
            if addr.len() <= MAX_TOKEN_LENGTH {
                self.text(
                    header_name,
                    addr.to_lowercase(),
                    Language::Unknown,
                    IndexOptions::new().keyword(),
                );
            }
        };
    }

    fn parse_address_group(&mut self, header_name: RfcHeader, group: &Group) {
        if let Some(name) = &group.name {
            self.parse_text(header_name, name);
        };

        for address in group.addresses.iter() {
            self.parse_address(header_name, address);
        }
    }

    fn parse_text(&mut self, header_name: RfcHeader, text: &str) {
        match header_name {
            RfcHeader::Keywords
            | RfcHeader::ContentLanguage
            | RfcHeader::MimeVersion
            | RfcHeader::MessageId
            | RfcHeader::References
            | RfcHeader::ContentId
            | RfcHeader::ResentMessageId => {
                if text.len() <= MAX_TOKEN_LENGTH {
                    self.text(
                        header_name,
                        text.to_lowercase(),
                        Language::Unknown,
                        IndexOptions::new().keyword(),
                    );
                }
            }

            RfcHeader::Subject => (),

            _ => {
                self.text(
                    header_name,
                    text.to_string(),
                    Language::Unknown,
                    IndexOptions::new().tokenize(),
                );
            }
        }
    }

    fn parse_content_type(&mut self, header_name: RfcHeader, content_type: &ContentType) {
        if content_type.c_type.len() <= MAX_TOKEN_LENGTH {
            self.text(
                header_name,
                content_type.c_type.to_string(),
                Language::Unknown,
                IndexOptions::new().keyword(),
            );
        }
        if let Some(subtype) = &content_type.c_subtype {
            if subtype.len() <= MAX_TOKEN_LENGTH {
                self.text(
                    header_name,
                    subtype.to_string(),
                    Language::Unknown,
                    IndexOptions::new().keyword(),
                );
            }
        }
        if let Some(attributes) = &content_type.attributes {
            for (key, value) in attributes {
                if key == "name" || key == "filename" {
                    self.text(
                        header_name,
                        value.to_string(),
                        Language::Unknown,
                        IndexOptions::new().tokenize(),
                    );
                } else if value.len() <= MAX_TOKEN_LENGTH {
                    self.text(
                        header_name,
                        value.to_lowercase(),
                        Language::Unknown,
                        IndexOptions::new().keyword(),
                    );
                }
            }
        }
    }

    #[allow(clippy::manual_flatten)]
    fn add_addr_sort(&mut self, header_name: RfcHeader, header_value: &HeaderValue) {
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
            self.text(
                header_name,
                text,
                Language::Unknown,
                IndexOptions::new().tokenize().sort(),
            );
        };
    }

    fn parse_header(&mut self, header_name: RfcHeader, header_value: &HeaderValue) {
        match header_value {
            HeaderValue::Address(address) => {
                self.parse_address(header_name, address);
            }
            HeaderValue::AddressList(address_list) => {
                for item in address_list {
                    self.parse_address(header_name, item);
                }
            }
            HeaderValue::Group(group) => {
                self.parse_address_group(header_name, group);
            }
            HeaderValue::GroupList(group_list) => {
                for item in group_list {
                    self.parse_address_group(header_name, item);
                }
            }
            HeaderValue::Text(text) => {
                self.parse_text(header_name, text);
            }
            HeaderValue::TextList(text_list) => {
                for item in text_list {
                    self.parse_text(header_name, item);
                }
            }
            HeaderValue::DateTime(date_time) => {
                if date_time.is_valid() {
                    self.number(
                        header_name,
                        date_time.to_timestamp() as u64,
                        IndexOptions::new().sort(),
                    );
                }
            }
            HeaderValue::ContentType(content_type) => {
                self.parse_content_type(header_name, content_type);
            }
            HeaderValue::Collection(header_value) => {
                for item in header_value {
                    self.parse_header(header_name, item);
                }
            }
            HeaderValue::Empty => (),
        }
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

pub fn empty_text_mime_headers(is_html: bool, size: usize) -> JMAPMailMimeHeaders {
    let mut mime_parts = HashMap::with_capacity(2);
    mime_parts.insert(MailBodyProperty::Size, size.into());
    mime_parts.insert(
        MailBodyProperty::Type,
        JSONValue::String(if is_html {
            "text/html".to_string()
        } else {
            "text/plain".to_string()
        }),
    );
    mime_parts
}

pub fn mime_parts_to_jmap(headers: RfcHeaders, size: usize) -> JMAPMailMimeHeaders {
    let mut mime_parts = HashMap::with_capacity(headers.len());
    if size > 0 {
        mime_parts.insert(MailBodyProperty::Size, size.into());
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

pub fn mime_header_to_jmap(
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
                                MailBodyProperty::Charset,
                                JSONValue::String(charset.to_string()),
                            );
                        }
                    }
                    if let Entry::Vacant(e) = mime_parts.entry(MailBodyProperty::Name) {
                        if let Some(name) = attributes.remove("name") {
                            e.insert(JSONValue::String(name.to_string()));
                        }
                    }
                }
                mime_parts.insert(
                    MailBodyProperty::Type,
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
                    MailBodyProperty::Disposition,
                    JSONValue::String(content_disposition.c_type.to_string()),
                );
                if let Some(mut attributes) = content_disposition.attributes {
                    if let Some(name) = attributes.remove("filename") {
                        mime_parts
                            .insert(MailBodyProperty::Name, JSONValue::String(name.to_string()));
                    }
                }
            }
        }
        RfcHeader::ContentId => match value {
            HeaderValue::Text(id) => {
                mime_parts.insert(MailBodyProperty::Cid, JSONValue::String(id.to_string()));
            }
            HeaderValue::TextList(mut ids) => {
                mime_parts.insert(
                    MailBodyProperty::Cid,
                    JSONValue::String(ids.pop().unwrap().to_string()),
                );
            }
            _ => {}
        },
        RfcHeader::ContentLanguage => match value {
            HeaderValue::Text(id) => {
                mime_parts.insert(
                    MailBodyProperty::Language,
                    JSONValue::Array(vec![JSONValue::String(id.to_string())]),
                );
            }
            HeaderValue::TextList(ids) => {
                mime_parts.insert(
                    MailBodyProperty::Language,
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
                    MailBodyProperty::Location,
                    JSONValue::String(id.to_string()),
                );
            }
            HeaderValue::TextList(mut ids) => {
                mime_parts.insert(
                    MailBodyProperty::Location,
                    JSONValue::String(ids.pop().unwrap().to_string()),
                );
            }
            _ => {}
        },
        _ => {}
    }
}
