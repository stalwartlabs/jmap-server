use std::{borrow::Cow, collections::HashMap};

use crate::mail::{HeaderName, MessageData, MessageField, MessageOutline, MimePart, MimePartType};
use jmap::{
    id::{blob::JMAPBlob, jmap::JMAPId},
    jmap_store::{
        get::{GetHelper, GetObject},
        orm::JMAPOrm,
    },
    request::{
        get::{GetRequest, GetResponse},
        MaybeIdReference,
    },
};
use mail_parser::{
    parsers::preview::{preview_html, preview_text, truncate_html, truncate_text},
    HeaderOffset, HeaderValue, MessageStructure, RfcHeader,
};
use store::serialize::leb128::Leb128;
use store::{blob::BlobId, JMAPStore};
use store::{
    core::{collection::Collection, error::StoreError},
    serialize::StoreDeserialize,
};
use store::{DocumentId, Store};

use super::{
    conv::{from_timestamp, IntoForm},
    schema::{
        BodyProperty, Email, EmailBodyPart, EmailBodyValue, EmailHeader, EmailValue, HeaderForm,
        HeaderProperty, Property,
    },
};

enum FetchRaw {
    Header,
    All,
    None,
}

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct GetArguments {
    #[serde(rename = "bodyProperties")]
    pub body_properties: Option<Vec<BodyProperty>>,

    #[serde(rename = "fetchTextBodyValues")]
    pub fetch_text_body_values: Option<bool>,

    #[serde(rename = "fetchHTMLBodyValues")]
    pub fetch_html_body_values: Option<bool>,

    #[serde(rename = "fetchAllBodyValues")]
    pub fetch_all_body_values: Option<bool>,

    #[serde(rename = "maxBodyValueBytes")]
    pub max_body_value_bytes: Option<usize>,
}

impl GetObject for Email {
    type GetArguments = GetArguments;

    fn default_properties() -> Vec<Self::Property> {
        vec![
            Property::Id,
            Property::BlobId,
            Property::ThreadId,
            Property::MailboxIds,
            Property::Keywords,
            Property::Size,
            Property::ReceivedAt,
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
    }
}

impl Email {
    pub fn default_body_properties() -> Vec<BodyProperty> {
        vec![
            BodyProperty::PartId,
            BodyProperty::BlobId,
            BodyProperty::Size,
            BodyProperty::Name,
            BodyProperty::Type,
            BodyProperty::Charset,
            BodyProperty::Disposition,
            BodyProperty::Cid,
            BodyProperty::Language,
            BodyProperty::Location,
        ]
    }
}

pub trait JMAPGetMail<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_get(&self, request: GetRequest<Email>) -> jmap::Result<GetResponse<Email>>;
}

impl<T> JMAPGetMail<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_get(&self, request: GetRequest<Email>) -> jmap::Result<GetResponse<Email>> {
        // Initialize helpers
        let account_id = request.account_id.as_ref().unwrap().get_document_id();
        let mut helper = GetHelper::new(
            self,
            request,
            Some(|ids: Vec<DocumentId>| {
                Ok(self
                    .get_multi_document_value(
                        account_id,
                        Collection::Mail,
                        ids.iter().copied(),
                        MessageField::ThreadId.into(),
                    )?
                    .into_iter()
                    .zip(ids)
                    .filter_map(
                        |(thread_id, document_id): (Option<DocumentId>, DocumentId)| {
                            JMAPId::from_parts(thread_id?, document_id).into()
                        },
                    )
                    .collect::<Vec<JMAPId>>())
            }),
        )?;

        // Process arguments
        let body_properties = helper
            .request
            .arguments
            .body_properties
            .take()
            .and_then(|p| if !p.is_empty() { Some(p) } else { None })
            .unwrap_or_else(Email::default_body_properties);
        let fetch_text_body_values = helper
            .request
            .arguments
            .fetch_text_body_values
            .unwrap_or(false);
        let fetch_html_body_values = helper
            .request
            .arguments
            .fetch_html_body_values
            .unwrap_or(false);
        let fetch_all_body_values = helper
            .request
            .arguments
            .fetch_all_body_values
            .unwrap_or(false);
        let max_body_value_bytes = helper.request.arguments.max_body_value_bytes.unwrap_or(0);
        let fetch_raw = if body_properties
            .iter()
            .any(|prop| matches!(prop, BodyProperty::Headers | BodyProperty::Header(_)))
        {
            FetchRaw::All
        } else if helper.properties.iter().any(|prop| {
            matches!(
                prop,
                Property::Header(HeaderProperty {
                    form: HeaderForm::Raw,
                    ..
                }) | Property::Header(HeaderProperty {
                    header: HeaderName::Other(_),
                    ..
                }) | Property::BodyStructure
            )
        }) {
            FetchRaw::Header
        } else {
            FetchRaw::None
        };

        // Get items
        let response = helper.get(|id, properties| {
            let document_id = id.get_document_id();

            // Fetch message metadat
            let message_data_bytes = self
                .blob_get(
                    &self
                        .get_document_value::<BlobId>(
                            account_id,
                            Collection::Mail,
                            document_id,
                            MessageField::Metadata.into(),
                        )?
                        .ok_or(StoreError::DataCorruption)?,
                )?
                .ok_or(StoreError::DataCorruption)?;

            let (message_data_len, read_bytes) = usize::from_leb128_bytes(&message_data_bytes[..])
                .ok_or(StoreError::DataCorruption)?;

            // Deserialize message data
            let mut message_data = MessageData::deserialize(
                &message_data_bytes[read_bytes..read_bytes + message_data_len],
            )
            .ok_or(StoreError::DataCorruption)?;

            // Fetch raw message only if needed
            let (raw_message, message_outline) = match &fetch_raw {
                FetchRaw::All => (
                    Some(
                        self.blob_get(&message_data.raw_message)?
                            .ok_or(StoreError::DataCorruption)?,
                    ),
                    Some(
                        MessageOutline::deserialize(
                            &message_data_bytes[read_bytes + message_data_len..],
                        )
                        .ok_or(StoreError::DataCorruption)?,
                    ),
                ),
                FetchRaw::Header => {
                    let message_outline = MessageOutline::deserialize(
                        &message_data_bytes[read_bytes + message_data_len..],
                    )
                    .ok_or(StoreError::DataCorruption)?;
                    (
                        Some(
                            self.blob_get_range(
                                &message_data.raw_message,
                                0..message_outline.body_offset as u32,
                            )?
                            .ok_or(StoreError::DataCorruption)?,
                        ),
                        Some(message_outline),
                    )
                }
                FetchRaw::None => (None, None),
            };

            // Fetch ORM
            let fields = self
                .get_orm::<Email>(account_id, document_id)?
                .ok_or_else(|| StoreError::InternalError("ORM not found for Email.".to_string()))?;

            // Add requested properties to result
            let mut result = HashMap::with_capacity(properties.len());
            for property in properties {
                let value = match property {
                    Property::Id => EmailValue::Id { value: id }.into(),
                    Property::BlobId => EmailValue::Blob {
                        value: JMAPBlob::from(&message_data.raw_message),
                    }
                    .into(),
                    Property::ThreadId => EmailValue::Id {
                        value: id.get_prefix_id().into(),
                    }
                    .into(),
                    Property::MailboxIds => {
                        fields
                            .get_tags(&Property::MailboxIds)
                            .map(|tags| EmailValue::MailboxIds {
                                value: tags
                                    .iter()
                                    .map(|tag| (MaybeIdReference::Value(tag.as_id().into()), true))
                                    .collect(),
                                set: true,
                            })
                    }
                    Property::Keywords => {
                        fields
                            .get_tags(&Property::Keywords)
                            .map(|tags| EmailValue::Keywords {
                                value: tags.iter().map(|tag| (tag.into(), true)).collect(),
                                set: true,
                            })
                    }
                    Property::Size => EmailValue::Size {
                        value: message_data.size,
                    }
                    .into(),
                    Property::ReceivedAt => EmailValue::Date {
                        value: from_timestamp(message_data.received_at),
                    }
                    .into(),
                    Property::MessageId | Property::InReplyTo | Property::References => {
                        message_data.header(
                            &property.as_rfc_header(),
                            &HeaderForm::MessageIds,
                            false,
                        )
                    }
                    Property::Sender
                    | Property::From
                    | Property::To
                    | Property::Cc
                    | Property::Bcc
                    | Property::ReplyTo => message_data.header(
                        &property.as_rfc_header(),
                        &HeaderForm::Addresses,
                        false,
                    ),
                    Property::Subject => {
                        message_data.header(&RfcHeader::Subject, &HeaderForm::Text, false)
                    }
                    Property::SentAt => {
                        message_data.header(&RfcHeader::Date, &HeaderForm::MessageIds, false)
                    }
                    Property::HasAttachment => EmailValue::Bool {
                        value: message_data.has_attachments,
                    }
                    .into(),
                    Property::Header(header) => {
                        match (&header.header, &header.form, &message_outline, &raw_message) {
                            (
                                header_name @ HeaderName::Other(_),
                                header_form,
                                Some(message_outline),
                                Some(raw_message),
                            )
                            | (
                                header_name @ HeaderName::Rfc(_),
                                header_form @ HeaderForm::Raw,
                                Some(message_outline),
                                Some(raw_message),
                            ) => {
                                if let Some(offsets) = message_outline
                                    .headers
                                    .get(0)
                                    .and_then(|h| h.get(header_name))
                                {
                                    header_form
                                        .parse_offsets(offsets, raw_message, header.all)
                                        .into_form(header_form, header.all)
                                } else {
                                    None
                                }
                            }
                            (HeaderName::Rfc(header_name), header_form, _, _) => {
                                message_data.header(header_name, header_form, header.all)
                            }
                            _ => None,
                        }
                    }
                    Property::Preview => {
                        if !message_data.text_body.is_empty() || !message_data.html_body.is_empty()
                        {
                            #[allow(clippy::type_complexity)]
                            let (parts, preview_fnc): (
                                &Vec<usize>,
                                fn(Cow<str>, usize) -> Cow<str>,
                            ) = if !message_data.text_body.is_empty() {
                                (&message_data.text_body, preview_text)
                            } else {
                                (&message_data.html_body, preview_html)
                            };

                            EmailValue::Text {
                                value: preview_fnc(
                                    String::from_utf8(
                                        self.blob_get(
                                            parts
                                                .get(0)
                                                .and_then(|p| message_data.mime_parts.get(p + 1))
                                                .ok_or(StoreError::DataCorruption)?
                                                .blob_id
                                                .as_ref()
                                                .ok_or(StoreError::DataCorruption)?,
                                        )?
                                        .ok_or(StoreError::DataCorruption)?,
                                    )
                                    .map_or_else(
                                        |err| String::from_utf8_lossy(err.as_bytes()).into_owned(),
                                        |s| s,
                                    )
                                    .into(),
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
                        let mut body_values = HashMap::new();
                        for (part_id, mime_part) in
                            message_data.mime_parts.iter().skip(1).enumerate()
                        {
                            if ((MimePartType::Html == mime_part.mime_type
                                && (fetch_all_body_values || fetch_html_body_values))
                                || (MimePartType::Text == mime_part.mime_type
                                    && (fetch_all_body_values || fetch_text_body_values)))
                                && (message_data.text_body.contains(&part_id)
                                    || message_data.html_body.contains(&part_id))
                            {
                                let blob = self
                                    .blob_get(
                                        mime_part
                                            .blob_id
                                            .as_ref()
                                            .ok_or(StoreError::DataCorruption)?,
                                    )?
                                    .ok_or(StoreError::DataCorruption)?;

                                body_values.insert(
                                    part_id.to_string(),
                                    mime_part.as_body_value(
                                        String::from_utf8(blob).map_or_else(
                                            |err| {
                                                String::from_utf8_lossy(err.as_bytes()).into_owned()
                                            },
                                            |s| s,
                                        ),
                                        max_body_value_bytes,
                                    ),
                                );
                            }
                        }
                        if !body_values.is_empty() {
                            EmailValue::BodyValues { value: body_values }.into()
                        } else {
                            None
                        }
                    }
                    Property::TextBody => Some(
                        message_data
                            .mime_parts
                            .as_body_parts(
                                &message_data.text_body,
                                &body_properties,
                                raw_message.as_deref(),
                                message_outline.as_ref(),
                                None,
                            )
                            .into(),
                    ),
                    Property::HtmlBody => Some(
                        message_data
                            .mime_parts
                            .as_body_parts(
                                &message_data.html_body,
                                &body_properties,
                                raw_message.as_deref(),
                                message_outline.as_ref(),
                                None,
                            )
                            .into(),
                    ),
                    Property::Attachments => Some(
                        message_data
                            .mime_parts
                            .as_body_parts(
                                &message_data.attachments,
                                &body_properties,
                                raw_message.as_deref(),
                                message_outline.as_ref(),
                                None,
                            )
                            .into(),
                    ),
                    Property::BodyStructure => message_outline
                        .as_ref()
                        .unwrap()
                        .as_body_structure(
                            &message_data.mime_parts,
                            &body_properties,
                            raw_message.as_deref(),
                            None,
                        )
                        .map(|b| b.into()),
                    Property::Invalid(_) => None,
                };

                if let Some(value) = value {
                    result.insert(property.clone(), value);
                }
            }

            Ok(Some(Email { properties: result }))
        })?;

        Ok(response)
    }
}

impl MimePart {
    pub fn as_body_part(
        &self,
        part_id: Option<usize>,
        properties: &[BodyProperty],
        message_raw: Option<&[u8]>,
        headers_raw: Option<&HashMap<HeaderName, Vec<HeaderOffset>>>,
        base_blob_id: Option<&BlobId>,
    ) -> EmailBodyPart {
        let mut body_part = HashMap::with_capacity(properties.len());

        for property in properties {
            match property {
                BodyProperty::PartId => {
                    if let Some(part_id) = part_id {
                        body_part.insert(
                            BodyProperty::PartId,
                            EmailValue::Text {
                                value: part_id.to_string(),
                            },
                        );
                    }
                }
                BodyProperty::BlobId if part_id.is_some() => {
                    if let Some(blob_id) = &self.blob_id {
                        body_part.insert(
                            BodyProperty::BlobId,
                            EmailValue::Blob {
                                value: if let Some(base_blob_id) = base_blob_id {
                                    JMAPBlob::new_inner(
                                        base_blob_id.clone(),
                                        *part_id.as_ref().unwrap() as u32,
                                    )
                                } else {
                                    JMAPBlob::from(blob_id)
                                },
                            },
                        );
                    }
                }
                BodyProperty::Size => {
                    body_part.insert(
                        BodyProperty::Size,
                        EmailValue::Size {
                            value: self.headers.size,
                        },
                    );
                }
                BodyProperty::Name => {
                    if let Some(name) = &self.headers.name {
                        body_part.insert(
                            BodyProperty::Name,
                            EmailValue::Text {
                                value: name.to_string(),
                            },
                        );
                    }
                }
                BodyProperty::Type => {
                    if let Some(mime_type) = &self.headers.type_ {
                        body_part.insert(
                            BodyProperty::Type,
                            EmailValue::Text {
                                value: mime_type.to_string(),
                            },
                        );
                    }
                }
                BodyProperty::Charset => {
                    if let Some(charset) = &self.headers.charset {
                        body_part.insert(
                            BodyProperty::Charset,
                            EmailValue::Text {
                                value: charset.to_string(),
                            },
                        );
                    }
                }
                BodyProperty::Disposition => {
                    if let Some(disposition) = &self.headers.disposition {
                        body_part.insert(
                            BodyProperty::Disposition,
                            EmailValue::Text {
                                value: disposition.to_string(),
                            },
                        );
                    }
                }
                BodyProperty::Cid => {
                    if let Some(cid) = &self.headers.cid {
                        body_part.insert(
                            BodyProperty::Cid,
                            EmailValue::Text {
                                value: cid.to_string(),
                            },
                        );
                    }
                }
                BodyProperty::Language => {
                    if let Some(language) = &self.headers.language {
                        body_part.insert(
                            BodyProperty::Language,
                            EmailValue::TextList {
                                value: language.to_vec(),
                            },
                        );
                    }
                }
                BodyProperty::Location => {
                    if let Some(location) = &self.headers.location {
                        body_part.insert(
                            BodyProperty::Location,
                            EmailValue::Text {
                                value: location.to_string(),
                            },
                        );
                    }
                }
                BodyProperty::Header(header) if headers_raw.is_some() => {
                    if let Some(offsets) = headers_raw.unwrap().get(&header.header) {
                        if let Some(value) = header
                            .form
                            .parse_offsets(offsets, message_raw.unwrap(), header.all)
                            .into_form(&header.form, header.all)
                        {
                            body_part.insert(BodyProperty::Header(header.clone()), value);
                        }
                    }
                }
                BodyProperty::Headers if headers_raw.is_some() => {
                    let headers_raw = headers_raw.unwrap();
                    let mut headers = Vec::with_capacity(headers_raw.len());
                    for (header, value) in headers_raw {
                        if let HeaderValue::Text(value) =
                            HeaderForm::Raw.parse_offsets(value, message_raw.unwrap(), false)
                        {
                            headers.push(EmailHeader {
                                name: header.as_str().to_string(),
                                value: value.into_owned(),
                            });
                        }
                    }
                    body_part.insert(
                        BodyProperty::Headers,
                        EmailValue::Headers { value: headers },
                    );
                }
                _ => (),
            }
        }

        EmailBodyPart {
            properties: body_part,
        }
    }

    pub fn as_body_value(&self, body_value: String, max_body_value: usize) -> EmailBodyValue {
        EmailBodyValue {
            is_encoding_problem: self.is_encoding_problem.into(),
            is_truncated: (max_body_value > 0 && body_value.len() > max_body_value).into(),
            value: if max_body_value == 0 || body_value.len() <= max_body_value {
                body_value
            } else if matches!(&self.mime_type, MimePartType::Html) {
                truncate_html(body_value.into(), max_body_value).to_string()
            } else {
                truncate_text(body_value.into(), max_body_value).to_string()
            },
        }
    }
}

pub trait AsBodyParts {
    fn as_body_parts(
        &self,
        parts: &[usize],
        properties: &[BodyProperty],
        message_raw: Option<&[u8]>,
        message_outline: Option<&MessageOutline>,
        base_blob_id: Option<&BlobId>,
    ) -> Vec<EmailBodyPart>;
}

impl AsBodyParts for Vec<MimePart> {
    fn as_body_parts(
        &self,
        parts: &[usize],
        properties: &[BodyProperty],
        message_raw: Option<&[u8]>,
        message_outline: Option<&MessageOutline>,
        base_blob_id: Option<&BlobId>,
    ) -> Vec<EmailBodyPart> {
        parts
            .iter()
            .filter_map(|part_id| {
                Some(self.get(part_id + 1)?.as_body_part(
                    (*part_id).into(),
                    properties,
                    message_raw,
                    message_outline.and_then(|m| m.headers.get(part_id + 1)),
                    base_blob_id,
                ))
            })
            .collect::<Vec<_>>()
    }
}

impl MessageOutline {
    pub fn as_body_structure(
        &self,
        mime_parts: &[MimePart],
        properties: &[BodyProperty],
        message_raw: Option<&[u8]>,
        base_blob_id: Option<&BlobId>,
    ) -> Option<EmailBodyPart> {
        let mut parts_stack = Vec::with_capacity(5);
        let mut stack = Vec::new();
        let part_list = match &self.body_structure {
            MessageStructure::Part(part_id) => {
                return mime_parts
                    .get(part_id + 1)?
                    .as_body_part(
                        (*part_id).into(),
                        properties,
                        message_raw,
                        self.headers.get(0),
                        base_blob_id,
                    )
                    .into();
            }
            MessageStructure::List(part_list) => {
                parts_stack.push(mime_parts.get(0)?.as_body_part(
                    None,
                    properties,
                    message_raw,
                    self.headers.get(0),
                    base_blob_id,
                ));
                part_list
            }
            MessageStructure::MultiPart((part_id, part_list)) => {
                parts_stack.push(mime_parts.get(0)?.as_body_part(
                    None,
                    properties,
                    message_raw,
                    self.headers.get(0),
                    base_blob_id,
                ));
                parts_stack.push(mime_parts.get(part_id + 1)?.as_body_part(
                    None,
                    properties,
                    message_raw,
                    self.headers.get(part_id + 1),
                    base_blob_id,
                ));
                stack.push(([].iter(), vec![]));
                part_list
            }
        };

        let mut subparts = Vec::with_capacity(part_list.len());
        let mut part_list_iter = part_list.iter();

        loop {
            while let Some(part) = part_list_iter.next() {
                match part {
                    MessageStructure::Part(part_id) => {
                        subparts.push(mime_parts.get(part_id + 1)?.as_body_part(
                            (*part_id).into(),
                            properties,
                            message_raw,
                            self.headers.get(part_id + 1),
                            base_blob_id,
                        ))
                    }
                    MessageStructure::MultiPart((part_id, next_part_list)) => {
                        parts_stack.push(mime_parts.get(part_id + 1)?.as_body_part(
                            None,
                            properties,
                            message_raw,
                            self.headers.get(part_id + 1),
                            base_blob_id,
                        ));
                        stack.push((part_list_iter, subparts));
                        part_list_iter = next_part_list.iter();
                        subparts = Vec::with_capacity(part_list.len());
                    }
                    MessageStructure::List(_) => (),
                }
            }

            if let Some((prev_part_list_iter, mut prev_subparts)) = stack.pop() {
                let mut prev_part = parts_stack.pop().unwrap();
                prev_part.properties.insert(
                    BodyProperty::Subparts,
                    EmailValue::BodyPartList { value: subparts },
                );
                prev_subparts.push(prev_part);
                part_list_iter = prev_part_list_iter;
                subparts = prev_subparts;
            } else {
                break;
            }
        }

        let mut root_part = parts_stack.pop().unwrap();
        root_part.properties.insert(
            BodyProperty::Subparts,
            EmailValue::BodyPartList { value: subparts },
        );

        root_part.into()
    }
}
