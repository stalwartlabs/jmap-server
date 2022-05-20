use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
    iter::FromIterator,
    vec,
};

use jmap::{
    id::{blob::JMAPBlob, JMAPIdSerialize},
    jmap_store::blob::InnerBlobFnc,
    protocol::json::JSONValue,
};
use mail_parser::{
    decoders::html::{html_to_text, text_to_html},
    parsers::preview::{preview_html, preview_text},
    Addr, Group, HeaderValue, Message, MessageAttachment, MessagePart, RfcHeader, RfcHeaders,
};

use store::{AccountId, JMAPStore, Store};

use crate::mail::{
    get::{
        add_body_parts, add_body_structure, add_body_value, add_raw_header,
        transform_json_emailaddress, transform_json_string, transform_json_stringlist,
        transform_rfc_header, MailGetArguments,
    },
    BodyProperty, HeaderForm, HeaderName, HeaderProperty, MessageOutline, MimeHeaders, MimePart,
    MimePartType, Property,
};

use super::get::transform_json_date;

pub struct ParseMail {
    pub account_id: AccountId,
    pub properties: Vec<Property>,
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
                        Property::MessageId,
                        Property::InReplyTo,
                        Property::References,
                        Property::Sender,
                        Property::From,
                        Property::To,
                        Property::Cc,
                        Property::Bcc,
                        Property::ReplyTo,
                        Property::Subject,
                        Property::SentAt,
                        Property::HasAttachment,
                        Property::Preview,
                        Property::BodyValues,
                        Property::TextBody,
                        Property::HtmlBody,
                        Property::Attachments,
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
        get_message_part
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
                    Property::Id
                    | Property::ThreadId
                    | Property::MailboxIds
                    | Property::ReceivedAt
                    | Property::Keywords => JSONValue::Null,

                    Property::BlobId => blob_id.to_jmap_string().into(),
                    Property::Size => raw_message.len().into(),
                    Property::MessageId | Property::References | Property::InReplyTo => {
                        if let Some(message_id) =
                            message.headers_rfc.remove(&property.as_rfc_header())
                        {
                            let (value, is_collection) = header_to_jmap_id(message_id);
                            transform_json_stringlist(value, is_collection, false)
                        } else {
                            JSONValue::Null
                        }
                    }
                    Property::Sender
                    | Property::From
                    | Property::To
                    | Property::Cc
                    | Property::Bcc
                    | Property::ReplyTo => {
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
                    Property::Subject => {
                        if let Some(text) = message.headers_rfc.remove(&RfcHeader::Subject) {
                            let (value, _) = header_to_jmap_text(text);
                            transform_json_string(value, false)
                        } else {
                            JSONValue::Null
                        }
                    }
                    Property::SentAt => {
                        if let Some(date) = message.headers_rfc.remove(&RfcHeader::Date) {
                            let (value, _) = header_to_jmap_date(date);
                            transform_json_date(value, false)
                        } else {
                            JSONValue::Null
                        }
                    }
                    Property::Header(HeaderProperty {
                        form: form @ HeaderForm::Raw,
                        header,
                        all,
                    })
                    | Property::Header(HeaderProperty {
                        form,
                        header: header @ HeaderName::Other(_),
                        all,
                    }) => {
                        if let Some(offsets) = message_outline
                            .headers
                            .get_mut(0)
                            .and_then(|l| l.remove(header))
                        {
                            add_raw_header(&offsets, raw_message, *form, *all)
                        } else {
                            JSONValue::Null
                        }
                    }
                    Property::Header(HeaderProperty {
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
                                *form,
                                is_collection,
                                is_grouped,
                                *all,
                            )
                            .unwrap_or(JSONValue::Null)
                        } else {
                            JSONValue::Null
                        }
                    }
                    Property::HasAttachment => has_attachments.into(),
                    Property::Preview => {
                        if !text_body.is_empty() {
                            preview_text(
                                String::from_utf8_lossy(
                                    &blobs[text_body
                                        .get(0)
                                        .and_then(|p| mime_parts.get(p + 1))
                                        .unwrap()
                                        .blob_id
                                        .as_ref()
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
                                        .as_ref()
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
                    Property::BodyValues => {
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
                                                &blobs[mime_part.blob_id.as_ref().unwrap().size
                                                    as usize],
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
                    Property::TextBody => add_body_parts(
                        &text_body,
                        &mime_parts,
                        &self.arguments.body_properties,
                        Some(raw_message),
                        Some(&message_outline),
                        Some(&blob_id.id),
                    ),

                    Property::HtmlBody => add_body_parts(
                        &html_body,
                        &mime_parts,
                        &self.arguments.body_properties,
                        Some(raw_message),
                        Some(&message_outline),
                        Some(&blob_id.id),
                    ),

                    Property::Attachments => add_body_parts(
                        &attachments,
                        &mime_parts,
                        &self.arguments.body_properties,
                        Some(raw_message),
                        Some(&message_outline),
                        Some(&blob_id.id),
                    ),

                    Property::BodyStructure => {
                        if let Some(body_structure) = add_body_structure(
                            &message_outline,
                            &mime_parts,
                            &self.arguments.body_properties,
                            Some(raw_message),
                            Some(&blob_id.id),
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

/*
// remove in import
pub fn get_message_part(raw_message: &[u8], part_id: u32) -> Option<Cow<[u8]>> {
    let mut message = Message::parse(raw_message)?;
    let part_id = part_id as usize;
    let total_parts = message.parts.len();

    if part_id < total_parts {
        match message.parts.swap_remove(part_id) {
            MessagePart::Text(part) | MessagePart::Html(part) => match part.body {
                Cow::Borrowed(text) => Cow::Borrowed(text.as_bytes()),
                Cow::Owned(text) => Cow::Owned(text.into_bytes()),
            }
            .into(),
            MessagePart::Binary(binary) | MessagePart::InlineBinary(binary) => binary.body.into(),
            MessagePart::Message(nested_message) => match nested_message.body {
                MessageAttachment::Parsed(message) => message.raw_message,
                MessageAttachment::Raw(raw_message) => raw_message,
            }
            .into(),
            MessagePart::Multipart(_) => None,
        }
    } else {
        let mut num_conversions = 0;
        for (part_pos, part) in message.parts.into_iter().enumerate() {
            match part {
                MessagePart::Html(html) => {
                    if message.text_body.contains(&part_pos) {
                        if total_parts + num_conversions == part_id {
                            return Cow::from(html_to_text(html.body.as_ref()).into_bytes()).into();
                        } else {
                            num_conversions += 1;
                        }
                    }
                }
                MessagePart::Text(text) => {
                    if message.html_body.contains(&part_pos) {
                        if total_parts + num_conversions == part_id {
                            return Cow::from(text_to_html(text.body.as_ref()).into_bytes()).into();
                        } else {
                            num_conversions += 1;
                        }
                    }
                }
                _ => (),
            }
        }
        None
    }
}
*/
