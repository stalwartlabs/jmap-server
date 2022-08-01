use super::{
    conv::{HeaderValueInto, IntoForm},
    get::{AsBodyParts, AsBodyStructure, AsEmailHeaders, BlobResult, JMAPGetMail},
    schema::{BodyProperty, Email, HeaderForm, Property, Value},
    GetRawHeader,
};
use crate::mail::{MimePart, MimePartType};
use jmap::{
    error::method::MethodError,
    jmap_store::get::GetObject,
    types::{blob::JMAPBlob, jmap::JMAPId},
};
use mail_parser::{
    decoders::html::{html_to_text, text_to_html},
    parsers::preview::{preview_html, preview_text},
    Header, HeaderName, HeaderValue, Message, MessageAttachment, PartType, RfcHeader,
};
use std::{borrow::Cow, sync::Arc};
use store::{
    ahash::AHashSet,
    core::{acl::ACLToken, vec_map::VecMap},
    JMAPStore, Store,
};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct EmailParseRequest {
    #[serde(skip)]
    pub acl: Option<Arc<ACLToken>>,

    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "blobIds")]
    blob_ids: AHashSet<JMAPBlob>,

    #[serde(rename = "properties")]
    properties: Option<Vec<Property>>,

    #[serde(rename = "bodyProperties")]
    #[serde(skip_serializing_if = "Option::is_none")]
    body_properties: Option<Vec<BodyProperty>>,

    #[serde(rename = "fetchTextBodyValues")]
    #[serde(skip_serializing_if = "Option::is_none")]
    fetch_text_body_values: Option<bool>,

    #[serde(rename = "fetchHTMLBodyValues")]
    #[serde(skip_serializing_if = "Option::is_none")]
    fetch_html_body_values: Option<bool>,

    #[serde(rename = "fetchAllBodyValues")]
    #[serde(skip_serializing_if = "Option::is_none")]
    fetch_all_body_values: Option<bool>,

    #[serde(rename = "maxBodyValueBytes")]
    max_body_value_bytes: Option<usize>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct EmailParseResponse {
    #[serde(rename = "accountId")]
    account_id: JMAPId,

    #[serde(rename = "parsed")]
    #[serde(skip_serializing_if = "VecMap::is_empty")]
    parsed: VecMap<JMAPBlob, Email>,

    #[serde(rename = "notParsable")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    not_parsable: Vec<JMAPBlob>,

    #[serde(rename = "notFound")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    not_found: Vec<JMAPBlob>,
}

struct EmailParseProperties {
    properties: Vec<Property>,
    body_properties: Vec<BodyProperty>,
    fetch_text_body_values: bool,
    fetch_html_body_values: bool,
    fetch_all_body_values: bool,
    max_body_value_bytes: usize,
}

pub trait JMAPMailParse<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_parse(&self, request: EmailParseRequest) -> jmap::Result<EmailParseResponse>;
}

impl<T> JMAPMailParse<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_parse(&self, mut request: EmailParseRequest) -> jmap::Result<EmailParseResponse> {
        if request.blob_ids.len() > self.config.mail_parse_max_items {
            return Err(MethodError::RequestTooLarge);
        }
        let mut response = EmailParseResponse {
            account_id: request.account_id,
            parsed: VecMap::with_capacity(request.blob_ids.len()),
            not_parsable: Vec::new(),
            not_found: Vec::new(),
        };
        let parse_properties = EmailParseProperties {
            properties: request
                .properties
                .and_then(|p| if !p.is_empty() { Some(p) } else { None })
                .unwrap_or_else(Email::default_properties),
            body_properties: request
                .body_properties
                .take()
                .and_then(|p| if !p.is_empty() { Some(p) } else { None })
                .unwrap_or_else(Email::default_body_properties),
            fetch_text_body_values: request.fetch_text_body_values.unwrap_or(false),
            fetch_html_body_values: request.fetch_html_body_values.unwrap_or(false),
            fetch_all_body_values: request.fetch_all_body_values.unwrap_or(false),
            max_body_value_bytes: request.max_body_value_bytes.unwrap_or(0),
        };

        let acl = request.acl.unwrap();
        let account_id = request.account_id.get_document_id();
        for blob_id in request.blob_ids {
            if let BlobResult::Blob(blob) = self.mail_blob_get(account_id, &acl, &blob_id)? {
                if let Some(message) = Message::parse(&blob) {
                    let email = message.into_parsed_email(&parse_properties, &blob_id, &blob);
                    response.parsed.append(blob_id, email);
                } else {
                    response.not_parsable.push(blob_id);
                }
            } else {
                response.not_found.push(blob_id);
            }
        }

        Ok(response)
    }
}

trait IntoParsedEmail {
    fn into_parsed_email(
        self,
        request: &EmailParseProperties,
        blob_id: &JMAPBlob,
        raw_message: &[u8],
    ) -> Email;
}

impl IntoParsedEmail for Message<'_> {
    fn into_parsed_email(
        mut self,
        request: &EmailParseProperties,
        blob_id: &JMAPBlob,
        raw_message: &[u8],
    ) -> Email {
        let mut mime_parts = Vec::with_capacity(self.parts.len());
        let html_body = self.html_body;
        let text_body = self.text_body;
        let attachments = self.attachments;
        let mut has_attachments = false;

        // Add MIME headers
        let mut headers;
        {
            let root_part = &mut self.parts[0];
            let mut mime_headers = Vec::with_capacity(4);
            headers = Vec::with_capacity(root_part.headers.len());

            for header in std::mem::take(&mut root_part.headers) {
                if matches!(&header.name, HeaderName::Rfc(name) if [
                    RfcHeader::ContentType,
                    RfcHeader::ContentDisposition,
                    RfcHeader::ContentId,
                    RfcHeader::ContentLanguage,
                    RfcHeader::ContentLocation,
                ].contains(name))
                {
                    headers.push(Header {
                        name: header.name.clone(),
                        value: HeaderValue::Empty,
                        offset_start: header.offset_start,
                        offset_end: header.offset_end,
                    });
                    mime_headers.push(header);
                } else {
                    headers.push(header);
                }
            }

            root_part.headers = mime_headers;
        }

        let mut blobs = Vec::new();

        // Extract blobs and build parts list
        for (part_id, part) in self.parts.into_iter().enumerate() {
            let (mime_type, part_size) = match part.body {
                PartType::Html(html) => {
                    if !text_body.contains(&part_id) && !html_body.contains(&part_id) {
                        has_attachments = true;
                    }
                    let value = (
                        MimePartType::Html {
                            blob_id: blobs.len().into(),
                        },
                        html.len(),
                    );
                    blobs.push(html.into_owned().into_bytes());
                    value
                }
                PartType::Text(text) => {
                    if !text_body.contains(&part_id) && !html_body.contains(&part_id) {
                        has_attachments = true;
                    }
                    let value = (
                        MimePartType::Text {
                            blob_id: blobs.len().into(),
                        },
                        text.len(),
                    );
                    blobs.push(text.into_owned().into_bytes());
                    value
                }
                PartType::Binary(binary) => {
                    if !has_attachments {
                        has_attachments = true;
                    }
                    let value = (
                        MimePartType::Other {
                            blob_id: blobs.len().into(),
                        },
                        binary.len(),
                    );
                    blobs.push(binary.into_owned());
                    value
                }
                PartType::InlineBinary(binary) => {
                    let value = (
                        MimePartType::Other {
                            blob_id: blobs.len().into(),
                        },
                        binary.len(),
                    );
                    blobs.push(binary.into_owned());
                    value
                }
                PartType::Message(nested_message) => {
                    if !has_attachments {
                        has_attachments = true;
                    }
                    let blob_index = blobs.len();

                    (
                        MimePartType::Other {
                            blob_id: blob_index.into(),
                        },
                        match nested_message {
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
                    )
                }
                PartType::Multipart(subparts) => (MimePartType::MultiPart { subparts }, 0),
            };

            mime_parts.push(MimePart::from_headers(
                part.headers,
                mime_type,
                part.is_encoding_problem,
                part_size,
            ));
        }

        let mut email = VecMap::with_capacity(request.properties.len());

        for property in &request.properties {
            let value = match property {
                Property::BlobId => Some(blob_id.into()),
                Property::Size => Some(raw_message.len().into()),
                Property::MessageId | Property::InReplyTo | Property::References => headers
                    .get_header(property.as_rfc_header())
                    .and_then(|p| p.into_form(&HeaderForm::MessageIds, false)),
                Property::Sender
                | Property::From
                | Property::To
                | Property::Cc
                | Property::Bcc
                | Property::ReplyTo => headers
                    .get_header(property.as_rfc_header())
                    .and_then(|p| p.into_form(&HeaderForm::Addresses, false)),
                Property::Subject => headers
                    .get_header(RfcHeader::Subject)
                    .and_then(|p| p.into_form(&HeaderForm::Text, false)),
                Property::SentAt => headers
                    .get_header(RfcHeader::Date)
                    .and_then(|p| p.into_form(&HeaderForm::Date, false)),
                Property::Headers => Value::Headers {
                    value: if let Some(root_part) = mime_parts.get(0) {
                        root_part.as_email_headers(raw_message)
                    } else {
                        Vec::new()
                    },
                }
                .into(),
                Property::Header(header) => match (&header.header, &header.form) {
                    (super::HeaderName::Other(_), _)
                    | (super::HeaderName::Rfc(_), HeaderForm::Raw) => {
                        if let Some(offsets) = headers.get_raw_header(&header.header) {
                            header
                                .form
                                .parse_offsets(&offsets, raw_message, header.all)
                                .into_form(&header.form, header.all)
                        } else if header.all {
                            Value::TextList { value: Vec::new() }.into()
                        } else {
                            None
                        }
                    }
                    (super::HeaderName::Rfc(header_name), _) => headers
                        .iter_mut()
                        .filter_map(|h| {
                            if matches!(&h.name, HeaderName::Rfc(name) if name == header_name)
                                && !matches!(h.value, HeaderValue::Empty)
                            {
                                let value = std::mem::take(&mut h.value);
                                match header.form {
                                    HeaderForm::Raw | HeaderForm::Text => value.into_text(),
                                    HeaderForm::URLs => value.into_url(),
                                    HeaderForm::MessageIds => value.into_keyword(),
                                    HeaderForm::Addresses | HeaderForm::GroupedAddresses => {
                                        value.into_address()
                                    }
                                    HeaderForm::Date => value.into_date(),
                                }
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<super::HeaderValue>>()
                        .into_form(&header.form, header.all),
                },
                /*Property::Header(header) => {
                    if let Some(offsets) = headers.get_raw_header(&header.header) {
                        header
                            .form
                            .parse_offsets(&offsets, raw_message, header.all)
                            .into_form(&header.form, header.all)
                    } else if header.all {
                        Value::TextList { value: Vec::new() }.into()
                    } else {
                        None
                    }
                }*/
                Property::HasAttachment => Some(has_attachments.into()),
                Property::Preview => {
                    if !text_body.is_empty() || !html_body.is_empty() {
                        let parts = if !text_body.is_empty() {
                            &text_body
                        } else {
                            &html_body
                        };

                        #[allow(clippy::type_complexity)]
                        let (preview_fnc, blob_id): (
                            fn(Cow<str>, usize) -> Cow<str>,
                            _,
                        ) = match &parts
                            .get(0)
                            .and_then(|p| mime_parts.get(*p))
                            .unwrap()
                            .mime_type
                        {
                            MimePartType::Text { blob_id } => (preview_text, blob_id),
                            MimePartType::Html { blob_id } => (preview_html, blob_id),
                            _ => unreachable!(),
                        };

                        Value::Text {
                            value: preview_fnc(
                                String::from_utf8_lossy(&blobs[blob_id.size as usize]),
                                256,
                            )
                            .into_owned(),
                        }
                        .into()
                    } else {
                        None
                    }
                }
                Property::BodyValues => {
                    let mut body_values = VecMap::new();
                    for (part_id, mime_part) in mime_parts.iter().enumerate() {
                        if ((mime_part.mime_type.is_html()
                            && (request.fetch_all_body_values || request.fetch_html_body_values))
                            || (mime_part.mime_type.is_text()
                                && (request.fetch_all_body_values
                                    || request.fetch_text_body_values)))
                            && (text_body.contains(&part_id) || html_body.contains(&part_id))
                        {
                            body_values.append(
                                part_id.to_string(),
                                mime_part.as_body_value(
                                    String::from_utf8(
                                        blobs[mime_part.mime_type.blob_id().unwrap().size as usize]
                                            .iter()
                                            .filter(|&&ch| ch != b'\r')
                                            .copied()
                                            .collect::<Vec<_>>(),
                                    )
                                    .map_or_else(
                                        |err| String::from_utf8_lossy(err.as_bytes()).into_owned(),
                                        |s| s,
                                    ),
                                    request.max_body_value_bytes,
                                ),
                            );
                        }
                    }
                    Value::BodyValues { value: body_values }.into()
                }
                Property::TextBody => Some(
                    mime_parts
                        .as_body_parts(
                            &text_body,
                            &request.body_properties,
                            Some(raw_message),
                            Some(&blob_id.id),
                        )
                        .into(),
                ),
                Property::HtmlBody => Some(
                    mime_parts
                        .as_body_parts(
                            &html_body,
                            &request.body_properties,
                            Some(raw_message),
                            Some(&blob_id.id),
                        )
                        .into(),
                ),
                Property::Attachments => Some(
                    mime_parts
                        .as_body_parts(
                            &attachments,
                            &request.body_properties,
                            Some(raw_message),
                            Some(&blob_id.id),
                        )
                        .into(),
                ),
                Property::BodyStructure => mime_parts
                    .as_body_structure(
                        &request.body_properties,
                        Some(raw_message),
                        Some(&blob_id.id),
                    )
                    .map(|b| b.into()),
                Property::Id
                | Property::ThreadId
                | Property::MailboxIds
                | Property::Keywords
                | Property::ReceivedAt
                | Property::Invalid(_) => None,
            };

            email.append(property.clone(), value.unwrap_or_default());
        }

        Email { properties: email }
    }
}

pub fn get_message_part(mut message: Message, part_id: u32, as_text: bool) -> Option<Cow<[u8]>> {
    let part_id = part_id as usize;
    let total_parts = message.parts.len();

    if part_id < total_parts {
        match message.parts.swap_remove(part_id).body {
            PartType::Text(text) => match text {
                Cow::Borrowed(text) => Cow::Borrowed(text.as_bytes()),
                Cow::Owned(text) => Cow::Owned(text.into_bytes()),
            }
            .into(),
            PartType::Html(html) => {
                if !as_text {
                    match html {
                        Cow::Borrowed(text) => Cow::Borrowed(text.as_bytes()),
                        Cow::Owned(text) => Cow::Owned(text.into_bytes()),
                    }
                    .into()
                } else {
                    Some(html_to_text(html.as_ref()).into_bytes().into())
                }
            }
            PartType::Binary(binary) | PartType::InlineBinary(binary) => binary.into(),
            PartType::Message(nested_message) => match nested_message {
                MessageAttachment::Parsed(message) => message.raw_message,
                MessageAttachment::Raw(raw_message) => raw_message,
            }
            .into(),
            PartType::Multipart(_) => None,
        }
    } else {
        let mut num_conversions = 0;
        for (part_pos, part) in message.parts.into_iter().enumerate() {
            match part.body {
                PartType::Html(html) => {
                    if message.text_body.contains(&part_pos) {
                        if total_parts + num_conversions == part_id {
                            return Cow::from(html_to_text(html.as_ref()).into_bytes()).into();
                        } else {
                            num_conversions += 1;
                        }
                    }
                }
                PartType::Text(text) => {
                    if message.html_body.contains(&part_pos) {
                        if total_parts + num_conversions == part_id {
                            return Cow::from(text_to_html(text.as_ref()).into_bytes()).into();
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

trait GetHeaderValues {
    fn get_header(&mut self, header_name: RfcHeader) -> Option<Vec<HeaderValue>>;
}

impl GetHeaderValues for Vec<Header<'_>> {
    fn get_header(&mut self, header_name: RfcHeader) -> Option<Vec<HeaderValue>> {
        let mut headers = Vec::new();
        for header in self.iter_mut() {
            if matches!(&header.name, HeaderName::Rfc(name) if &header_name == name)
                && !matches!(&header.value, HeaderValue::Empty)
            {
                headers.push(std::mem::take(&mut header.value));
            }
        }

        if !headers.is_empty() {
            headers.into()
        } else {
            None
        }
    }
}
