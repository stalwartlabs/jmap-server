use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
};

use crate::mail::{HeaderName, MessageData, MessageField, MessageOutline, MimePart, MimePartType};
use jmap::{
    error::method::MethodError,
    id::{blob::JMAPBlob, jmap::JMAPId, JMAPIdSerialize},
    jmap_store::{
        get::{GetHelper, GetObject},
        orm::JMAPOrm,
    },
    protocol::json::JSONValue,
    request::get::{GetRequest, GetResponse},
};
use mail_parser::{
    parsers::{
        fields::{
            address::parse_address, date::parse_date, id::parse_id,
            unstructured::parse_unstructured,
        },
        message::MessageStream,
        preview::{preview_html, preview_text, truncate_html, truncate_text},
    },
    HeaderOffset, HeaderValue, MessageStructure, RfcHeader,
};
use store::serialize::leb128::Leb128;
use store::{blob::BlobId, core::JMAPIdPrefix, AccountId, JMAPStore};
use store::{
    core::{collection::Collection, error::StoreError},
    serialize::StoreDeserialize,
};
use store::{DocumentId, Store};

use super::{
    conv::{from_timestamp, IntoForm},
    schema::{BodyProperty, Email, EmailValue, HeaderForm, HeaderProperty, Property},
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
            .unwrap_or_else(|| {
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
            });
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
            let (raw_message, mut message_outline) = match &fetch_raw {
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
                    Property::MailboxIds => todo!(),
                    Property::Keywords => todo!(),
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
                    Property::Preview => todo!(),
                    Property::BodyValues => todo!(),
                    Property::TextBody => todo!(),
                    Property::HtmlBody => todo!(),
                    Property::Attachments => todo!(),
                    Property::BodyStructure => todo!(),
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

/*


impl<'y, T> GetObject<'y, T> for GetMail<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    type Property = Property;

    fn new(
        store: &'y JMAPStore<T>,
        request: &mut GetRequest,
        properties: &[Self::Property],
    ) -> jmap::Result<Self> {
        let arguments = MailGetArguments::parse_arguments(std::mem::take(&mut request.arguments))?;

        Ok(GetMail {
            store,
            account_id: request.account_id,
            fetch_raw: ,
            arguments,
        })
    }

    fn get_item(
        &self,
        jmap_id: JMAPId,
        properties: &[Self::Property],
    ) -> jmap::Result<Option<JSONValue>> {

        let mut result: HashMap<String, JSONValue> = HashMap::new();

        for property in properties {
            if let Entry::Vacant(entry) = result.entry(property.to_string()) {
                let value = match property {
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
                            .as_mut()
                            .unwrap()
                            .headers
                            .get_mut(0)
                            .and_then(|l| l.remove(header))
                        {
                            add_raw_header(&offsets, message_raw.as_ref().unwrap(), *form, *all)
                        } else {
                            JSONValue::Null
                        }
                    }
                    Property::MessageId | Property::InReplyTo | Property::References => {
                        get_rfc_header(
                            &mut message_data.properties,
                            property.as_rfc_header(),
                            HeaderForm::MessageIds,
                            false,
                        )?
                    }
                    Property::Sender
                    | Property::From
                    | Property::To
                    | Property::Cc
                    | Property::Bcc
                    | Property::ReplyTo => get_rfc_header(
                        &mut message_data.properties,
                        property.as_rfc_header(),
                        HeaderForm::Addresses,
                        false,
                    )?,
                    Property::Subject => get_rfc_header(
                        &mut message_data.properties,
                        RfcHeader::Subject,
                        HeaderForm::Text,
                        false,
                    )?,
                    Property::SentAt => get_rfc_header(
                        &mut message_data.properties,
                        RfcHeader::Date,
                        HeaderForm::Date,
                        false,
                    )?,
                    Property::Header(HeaderProperty {
                        form,
                        header: HeaderName::Rfc(header),
                        all,
                    }) => get_rfc_header(&mut message_data.properties, *header, *form, *all)?,
                    Property::Id => jmap_id.to_jmap_string().into(),
                    Property::BlobId => JMAPBlob::from(&message_data.raw_message)
                        .to_jmap_string()
                        .into(),
                    Property::ThreadId => {
                        (jmap_id.get_prefix_id() as JMAPId).to_jmap_string().into()
                    }
                    Property::MailboxIds => fields
                        .get_tags(&MessageField::Mailbox)
                        .map(|tags| {
                            tags.iter()
                                .map(|tag| ((tag.as_id() as JMAPId).to_jmap_string(), true.into()))
                                .collect::<HashMap<String, JSONValue>>()
                        })
                        .into(),
                    Property::Keywords => fields
                        .get_tags(&MessageField::Keyword)
                        .map(|tags| {
                            tags.iter()
                                .filter_map(|tag| {
                                    Some((Keyword::to_jmap(tag.clone()).ok()?, true.into()))
                                })
                                .collect::<HashMap<String, JSONValue>>()
                        })
                        .into(),
                    Property::Size | Property::HasAttachment => {
                        message_data.properties.remove(property).unwrap_or_default()
                    }
                    Property::ReceivedAt => message_data
                        .properties
                        .remove(property)
                        .map(|date| date.into_utc_date())
                        .unwrap_or_default(),
                    Property::TextBody => add_body_parts(
                        &message_data.text_body,
                        &message_data.mime_parts,
                        &self.arguments.body_properties,
                        message_raw_ref,
                        message_outline.as_ref(),
                        None,
                    ),

                    Property::HtmlBody => add_body_parts(
                        &message_data.html_body,
                        &message_data.mime_parts,
                        &self.arguments.body_properties,
                        message_raw_ref,
                        message_outline.as_ref(),
                        None,
                    ),

                    Property::Attachments => add_body_parts(
                        &message_data.attachments,
                        &message_data.mime_parts,
                        &self.arguments.body_properties,
                        message_raw_ref,
                        message_outline.as_ref(),
                        None,
                    ),

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

                            preview_fnc(
                                String::from_utf8(
                                    self.store
                                        .blob_get(
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
                            .to_string()
                            .into()
                        } else {
                            JSONValue::Null
                        }
                    }
                    Property::BodyValues => {
                        let mut body_values = HashMap::new();
                        for (part_id, mime_part) in
                            message_data.mime_parts.iter().skip(1).enumerate()
                        {
                            if ((MimePartType::Html == mime_part.mime_type
                                && (self.arguments.fetch_all_body_values
                                    || self.arguments.fetch_html_body_values))
                                || (MimePartType::Text == mime_part.mime_type
                                    && (self.arguments.fetch_all_body_values
                                        || self.arguments.fetch_text_body_values)))
                                && (message_data.text_body.contains(&part_id)
                                    || message_data.html_body.contains(&part_id))
                            {
                                let blob = self
                                    .store
                                    .blob_get(
                                        mime_part
                                            .blob_id
                                            .as_ref()
                                            .ok_or(StoreError::DataCorruption)?,
                                    )?
                                    .ok_or(StoreError::DataCorruption)?;

                                body_values.insert(
                                    part_id.to_string(),
                                    add_body_value(
                                        mime_part,
                                        String::from_utf8(blob).map_or_else(
                                            |err| {
                                                String::from_utf8_lossy(err.as_bytes()).into_owned()
                                            },
                                            |s| s,
                                        ),
                                        &self.arguments,
                                    ),
                                );
                            }
                        }
                        if !body_values.is_empty() {
                            body_values.into()
                        } else {
                            JSONValue::Null
                        }
                    }
                    Property::BodyStructure => {
                        if let Some(body_structure) = add_body_structure(
                            message_outline.as_ref().unwrap(),
                            &message_data.mime_parts,
                            &self.arguments.body_properties,
                            message_raw_ref,
                            None,
                        ) {
                            body_structure
                        } else {
                            JSONValue::Null
                        }
                    }
                };

                if !value.is_null() {
                    entry.insert(value);
                }
            }
        }

        Ok(Some(result.into()))
    }

}

pub fn add_body_value(
    mime_part: &MimePart,
    body_text: String,
    arguments: &MailGetArguments,
) -> JSONValue {
    let mut body_value = HashMap::with_capacity(3);
    body_value.insert(
        "isEncodingProblem".into(),
        JSONValue::Bool(mime_part.is_encoding_problem),
    );
    body_value.insert(
        "isTruncated".into(),
        JSONValue::Bool(
            arguments.max_body_value_bytes > 0 && body_text.len() > arguments.max_body_value_bytes,
        ),
    );
    body_value.insert(
        "value".into(),
        if arguments.max_body_value_bytes == 0 || body_text.len() <= arguments.max_body_value_bytes
        {
            JSONValue::String(body_text)
        } else {
            JSONValue::String(if let MimePartType::Html = mime_part.mime_type {
                truncate_html(body_text.into(), arguments.max_body_value_bytes).to_string()
            } else {
                truncate_text(body_text.into(), arguments.max_body_value_bytes).to_string()
            })
        },
    );
    body_value.into()
}

pub fn add_body_structure(
    message_outline: &MessageOutline,
    mime_parts: &[MimePart],
    properties: &[BodyProperty],
    message_raw: Option<&[u8]>,
    base_blob_id: Option<&BlobId>,
) -> Option<JSONValue> {
    let mut parts_stack = Vec::with_capacity(5);
    let mut stack = Vec::new();

    let part_list = match &message_outline.body_structure {
        MessageStructure::Part(part_id) => {
            return Some(JSONValue::Object(add_body_part(
                (*part_id).into(),
                mime_parts.get(part_id + 1)?,
                properties,
                message_raw,
                message_outline.headers.get(0),
                base_blob_id,
            )))
        }
        MessageStructure::List(part_list) => {
            parts_stack.push(add_body_part(
                None,
                mime_parts.get(0)?,
                properties,
                message_raw,
                message_outline.headers.get(0),
                base_blob_id,
            ));
            part_list
        }
        MessageStructure::MultiPart((part_id, part_list)) => {
            parts_stack.push(add_body_part(
                None,
                mime_parts.get(0)?,
                properties,
                message_raw,
                message_outline.headers.get(0),
                base_blob_id,
            ));
            parts_stack.push(add_body_part(
                None,
                mime_parts.get(part_id + 1)?,
                properties,
                message_raw,
                message_outline.headers.get(part_id + 1),
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
                MessageStructure::Part(part_id) => subparts.push(JSONValue::Object(add_body_part(
                    (*part_id).into(),
                    mime_parts.get(part_id + 1)?,
                    properties,
                    message_raw,
                    message_outline.headers.get(part_id + 1),
                    base_blob_id,
                ))),
                MessageStructure::MultiPart((part_id, next_part_list)) => {
                    parts_stack.push(add_body_part(
                        None,
                        mime_parts.get(part_id + 1)?,
                        properties,
                        message_raw,
                        message_outline.headers.get(part_id + 1),
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
            prev_part.insert("subParts".into(), JSONValue::Array(subparts));
            prev_subparts.push(JSONValue::Object(prev_part));
            part_list_iter = prev_part_list_iter;
            subparts = prev_subparts;
        } else {
            break;
        }
    }

    let mut root_part = parts_stack.pop().unwrap();
    root_part.insert("subParts".into(), JSONValue::Array(subparts));
    Some(JSONValue::Object(root_part))
}

pub fn add_body_parts(
    parts: &[usize],
    mime_parts: &[MimePart],
    properties: &[BodyProperty],
    message_raw: Option<&[u8]>,
    message_outline: Option<&MessageOutline>,
    base_blob_id: Option<&BlobId>,
) -> JSONValue {
    parts
        .iter()
        .filter_map(|part_id| {
            Some(JSONValue::Object(add_body_part(
                (*part_id).into(),
                mime_parts.get(part_id + 1)?,
                properties,
                message_raw,
                message_outline.and_then(|m| m.headers.get(part_id + 1)),
                base_blob_id,
            )))
        })
        .collect::<Vec<_>>()
        .into()
}

fn add_body_part(
    part_id: Option<usize>,
    mime_part: &MimePart,
    properties: &[BodyProperty],
    message_raw: Option<&[u8]>,
    headers_raw: Option<&HashMap<HeaderName, Vec<HeaderOffset>>>,
    base_blob_id: Option<&BlobId>,
) -> HashMap<String, JSONValue> {
    let mut body_part = HashMap::with_capacity(properties.len());
    let mut headers_result: HashMap<String, Vec<JSONValue>> = HashMap::new();
    let has_raw_headers = headers_raw.is_some();

    let get_raw_header = |value: &Vec<HeaderOffset>| -> Vec<_> {
        value
            .iter()
            .filter_map(|offset| {
                Some(JSONValue::String(
                    std::str::from_utf8(message_raw.unwrap().get(offset.start..offset.end)?)
                        .map_or_else(
                            |_| {
                                String::from_utf8_lossy(
                                    message_raw.unwrap().get(offset.start..offset.end).unwrap(),
                                )
                                .trim()
                                .to_string()
                            },
                            |str| str.trim().to_string(),
                        ),
                ))
            })
            .collect::<Vec<JSONValue>>()
    };

    for property in properties {
        match property {
            BodyProperty::Size
            | BodyProperty::Name
            | BodyProperty::Type
            | BodyProperty::Charset
            | BodyProperty::Disposition
            | BodyProperty::Cid
            | BodyProperty::Language
            | BodyProperty::Location => {
                if let Some(value) = mime_part.headers.get(property) {
                    body_part.insert(property.to_string(), value.clone());
                }
            }

            BodyProperty::BlobId if part_id.is_some() => {
                body_part.insert(
                    "blobId".into(),
                    if let Some(base_blob_id) = base_blob_id {
                        JMAPBlob::new_inner(base_blob_id.clone(), *part_id.as_ref().unwrap() as u32)
                            .to_jmap_string()
                            .into()
                    } else {
                        mime_part
                            .blob_id
                            .as_ref()
                            .map(|id| JMAPBlob::from(id).to_jmap_string())
                            .into()
                    },
                );
            }
            BodyProperty::Header(header) if has_raw_headers => {
                if let Some(offsets) = headers_raw.unwrap().get(&header.header) {
                    body_part.insert(
                        header.to_string(),
                        add_raw_header(
                            offsets,
                            message_raw.as_ref().unwrap(),
                            header.form,
                            header.all,
                        ),
                    );
                }
            }
            BodyProperty::Headers if has_raw_headers => {
                for (header, value) in headers_raw.unwrap() {
                    if let Entry::Vacant(entry) = headers_result.entry(header.as_str().to_string())
                    {
                        entry.insert(get_raw_header(value));
                    }
                }
            }
            BodyProperty::PartId => {
                if let Some(part_id) = part_id {
                    body_part.insert("partId".into(), part_id.to_string().into());
                }
            }
            _ => (),
        }
    }

    if !headers_result.is_empty() {
        body_part.insert(
            "headers".into(),
            JSONValue::Array(
                headers_result
                    .into_iter()
                    .flat_map(|(header, values)| {
                        values.into_iter().map(move |value| {
                            let mut result: HashMap<String, JSONValue> = HashMap::with_capacity(2);
                            result.insert("name".into(), JSONValue::String((&header).clone()));
                            result.insert("value".into(), value);
                            JSONValue::Object(result)
                        })
                    })
                    .collect(),
            ),
        );
    }

    body_part
}

pub fn get_rfc_header(
    message_headers: &mut Headers,
    header: RfcHeader,
    form: HeaderForm,
    all: bool,
) -> jmap::Result<JSONValue> {
    let (value, is_collection, is_grouped) =
        match &form {
            HeaderForm::Addresses | HeaderForm::GroupedAddresses => {
                if let Some(value) = message_headers.remove(&Property::Header(
                    HeaderProperty::new_rfc(header, HeaderForm::Addresses, false),
                )) {
                    (value, false, false)
                } else if let Some(value) = message_headers.remove(&Property::Header(
                    HeaderProperty::new_rfc(header, HeaderForm::Addresses, true),
                )) {
                    (value, true, false)
                } else if let Some(value) = message_headers.remove(&Property::Header(
                    HeaderProperty::new_rfc(header, HeaderForm::GroupedAddresses, false),
                )) {
                    (value, false, true)
                } else if let Some(value) = message_headers.remove(&Property::Header(
                    HeaderProperty::new_rfc(header, HeaderForm::GroupedAddresses, true),
                )) {
                    (value, true, true)
                } else {
                    (JSONValue::Null, false, false)
                }
            }
            _ => {
                if let Some(value) = message_headers.remove(&Property::Header(
                    HeaderProperty::new_rfc(header, form, all),
                )) {
                    (value, all, false)
                } else if let Some(value) = message_headers.remove(&Property::Header(
                    HeaderProperty::new_rfc(header, form, !all),
                )) {
                    (value, !all, false)
                } else {
                    (JSONValue::Null, false, false)
                }
            }
        };

    transform_rfc_header(header, value, form, is_collection, is_grouped, all)
}

pub fn transform_rfc_header(
    header: RfcHeader,
    value: JSONValue,
    form: HeaderForm,
    is_collection: bool,
    is_grouped: bool,
    as_collection: bool,
) -> jmap::Result<JSONValue> {
    Ok(match (header, form) {
        (
            RfcHeader::Subject | RfcHeader::Comments | RfcHeader::Keywords | RfcHeader::ListId,
            HeaderForm::Text,
        ) => transform_json_string(value, as_collection),
        (RfcHeader::Date | RfcHeader::ResentDate, HeaderForm::Date) => {
            transform_json_date(value, as_collection)
        }
        (
            RfcHeader::MessageId
            | RfcHeader::References
            | RfcHeader::ResentMessageId
            | RfcHeader::InReplyTo,
            HeaderForm::MessageIds,
        )
        | (
            RfcHeader::ListArchive
            | RfcHeader::ListHelp
            | RfcHeader::ListOwner
            | RfcHeader::ListPost
            | RfcHeader::ListSubscribe
            | RfcHeader::ListUnsubscribe,
            HeaderForm::URLs,
        ) => transform_json_stringlist(value, is_collection, as_collection),
        (
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
            | RfcHeader::ResentSender,
            HeaderForm::Addresses | HeaderForm::GroupedAddresses,
        ) => transform_json_emailaddress(
            value,
            is_grouped,
            is_collection,
            matches!(form, HeaderForm::GroupedAddresses),
            as_collection,
        ),
        _ => {
            return Err(MethodError::InvalidArguments(
                "Invalid header property.".to_string(),
            ))
        }
    })
}

pub fn add_raw_header(
    offsets: &[HeaderOffset],
    message_raw: &[u8],
    form: HeaderForm,
    all: bool,
) -> JSONValue {
    let mut header_values: Vec<HeaderValue> = offsets
        .iter()
        .skip(if !all && offsets.len() > 1 {
            offsets.len() - 1
        } else {
            0
        })
        .map(|offset| {
            (message_raw.get(offset.start..offset.end).map_or(
                HeaderValue::Empty,
                |bytes| match form {
                    HeaderForm::Raw => HeaderValue::Text(std::str::from_utf8(bytes).map_or_else(
                        |_| String::from_utf8_lossy(bytes).trim().to_string().into(),
                        |str| str.trim().to_string().into(),
                    )),
                    HeaderForm::Text => parse_unstructured(&mut MessageStream::new(bytes)),
                    HeaderForm::Addresses => parse_address(&mut MessageStream::new(bytes)),
                    HeaderForm::GroupedAddresses => parse_address(&mut MessageStream::new(bytes)),
                    HeaderForm::MessageIds => parse_id(&mut MessageStream::new(bytes)),
                    HeaderForm::Date => parse_date(&mut MessageStream::new(bytes)),
                    HeaderForm::URLs => parse_address(&mut MessageStream::new(bytes)),
                },
            ))
            .into_owned()
        })
        .collect();
    let header_values = if all {
        HeaderValue::Collection(header_values)
    } else {
        header_values.pop().unwrap_or_default()
    };
    match form {
        HeaderForm::Raw | HeaderForm::Text => header_to_jmap_text(header_values).0,
        HeaderForm::Addresses | HeaderForm::GroupedAddresses => {
            let (value, is_grouped, is_collection) = header_to_jmap_address(header_values, false);
            transform_json_emailaddress(
                value,
                is_grouped,
                is_collection,
                matches!(form, HeaderForm::GroupedAddresses),
                all,
            )
        }
        HeaderForm::MessageIds => header_to_jmap_id(header_values).0,
        HeaderForm::Date => header_to_jmap_date(header_values).0.into_utc_date(),
        HeaderForm::URLs => header_to_jmap_url(header_values).0,
    }
}

pub fn transform_json_emailaddress(
    value: JSONValue,
    is_grouped: bool,
    is_collection: bool,
    as_grouped: bool,
    as_collection: bool,
) -> JSONValue {
    if let JSONValue::Array(mut list) = value {
        if ((as_grouped && is_grouped) || (!as_grouped && !is_grouped))
            && ((is_collection && as_collection) || (!is_collection && !as_collection))
        {
            list.into()
        } else if (as_grouped && is_grouped) || (!as_grouped && !is_grouped) {
            if as_collection && !is_collection {
                vec![list.into()].into()
            } else {
                // !as_collection && is_collection
                list.pop().unwrap_or_default()
            }
        } else {
            let mut list = if as_collection && !is_collection {
                vec![list.into()]
            } else if !as_collection && is_collection {
                if let JSONValue::Array(list) = list.pop().unwrap_or_default() {
                    list
                } else {
                    vec![]
                }
            } else {
                list
            };

            if as_grouped && !is_grouped {
                let list_to_group = |list: Vec<JSONValue>| -> JSONValue {
                    let mut group = HashMap::new();
                    group.insert("name".to_string(), JSONValue::Null);
                    group.insert("addresses".to_string(), list.into());
                    JSONValue::Object(group)
                };
                JSONValue::Array(if !as_collection {
                    vec![list_to_group(list)]
                } else {
                    list.iter_mut().for_each(|field| {
                        if let JSONValue::Array(list) = field {
                            *field = JSONValue::Array(vec![list_to_group(std::mem::take(list))]);
                        }
                    });
                    list
                })
            } else {
                // !as_grouped && is_grouped
                let flatten_group = |list: Vec<JSONValue>| -> Vec<JSONValue> {
                    let mut addresses = Vec::with_capacity(list.len() * 2);
                    list.into_iter().for_each(|group| {
                        if let JSONValue::Object(mut group) = group {
                            if let Some(JSONValue::Array(mut group_addresses)) =
                                group.remove("addresses")
                            {
                                addresses.append(&mut group_addresses);
                            }
                        }
                    });
                    addresses
                };
                JSONValue::Array(if !as_collection {
                    flatten_group(list)
                } else {
                    list.into_iter()
                        .map(|field| {
                            if let JSONValue::Array(list) = field {
                                JSONValue::Array(flatten_group(list))
                            } else {
                                field
                            }
                        })
                        .collect()
                })
            }
        }
    } else {
        JSONValue::Null
    }
}

pub fn transform_json_stringlist(
    value: JSONValue,
    is_collection: bool,
    as_collection: bool,
) -> JSONValue {
    if let JSONValue::Array(mut list) = value {
        if !as_collection {
            if !is_collection {
                list.into()
            } else {
                list.pop().unwrap_or_default()
            }
        } else if is_collection {
            list.into()
        } else {
            vec![list.into()].into()
        }
    } else {
        JSONValue::Null
    }
}

pub fn transform_json_string(value: JSONValue, as_collection: bool) -> JSONValue {
    match value {
        JSONValue::Array(mut list) => {
            if !as_collection {
                list.pop().unwrap_or_default()
            } else {
                list.into()
            }
        }
        value @ JSONValue::String(_) => {
            if !as_collection {
                value
            } else {
                vec![value].into()
            }
        }
        _ => JSONValue::Null,
    }
}

pub fn transform_json_date(value: JSONValue, as_collection: bool) -> JSONValue {
    match value {
        JSONValue::Array(mut list) => {
            if !as_collection {
                list.pop()
                    .map(|value| value.into_utc_date())
                    .unwrap_or_default()
            } else {
                list.into_iter()
                    .map(|value| value.into_utc_date())
                    .collect::<Vec<_>>()
                    .into()
            }
        }
        value @ JSONValue::Number(_) => {
            if !as_collection {
                value.into_utc_date()
            } else {
                vec![value.into_utc_date()].into()
            }
        }
        _ => JSONValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use jmap::protocol::json::JSONValue;

    #[test]
    fn test_json_transform() {
        for (value, expected_result, expected_result_all) in [
            (
                JSONValue::String("hello".into()),
                JSONValue::String("hello".into()),
                JSONValue::Array(vec![JSONValue::String("hello".into())]),
            ),
            (
                JSONValue::Array(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ]),
                JSONValue::String("world".into()),
                JSONValue::Array(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ]),
            ),
        ] {
            assert_eq!(
                super::transform_json_string(value.clone(), false),
                expected_result
            );
            assert_eq!(
                super::transform_json_string(value, true),
                expected_result_all
            );
        }

        for (value, is_collection, expected_result, expected_result_all) in [
            (
                JSONValue::Array(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ]),
                false,
                JSONValue::Array(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ]),
                JSONValue::Array(vec![JSONValue::Array(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ])]),
            ),
            (
                JSONValue::Array(vec![
                    JSONValue::Array(vec![
                        JSONValue::String("hello".into()),
                        JSONValue::String("world".into()),
                    ]),
                    JSONValue::Array(vec![
                        JSONValue::String("hola".into()),
                        JSONValue::String("mundo".into()),
                    ]),
                ]),
                true,
                JSONValue::Array(vec![
                    JSONValue::String("hola".into()),
                    JSONValue::String("mundo".into()),
                ]),
                JSONValue::Array(vec![
                    JSONValue::Array(vec![
                        JSONValue::String("hello".into()),
                        JSONValue::String("world".into()),
                    ]),
                    JSONValue::Array(vec![
                        JSONValue::String("hola".into()),
                        JSONValue::String("mundo".into()),
                    ]),
                ]),
            ),
        ] {
            assert_eq!(
                super::transform_json_stringlist(value.clone(), is_collection, false),
                expected_result
            );
            assert_eq!(
                super::transform_json_stringlist(value, is_collection, true),
                expected_result_all
            );
        }

        fn make_email(name: &str, addr: &str) -> JSONValue {
            let mut email = HashMap::new();
            email.insert("name".to_string(), JSONValue::String(name.to_string()));
            email.insert("email".to_string(), JSONValue::String(addr.to_string()));
            JSONValue::Object(email)
        }

        fn make_group(name: Option<&str>, addresses: JSONValue) -> JSONValue {
            let mut email = HashMap::new();
            email.insert(
                "name".to_string(),
                name.map_or(JSONValue::Null, |name| JSONValue::String(name.to_string())),
            );
            email.insert("addresses".to_string(), addresses);
            JSONValue::Object(email)
        }

        fn make_list(value1: JSONValue, value2: JSONValue) -> JSONValue {
            JSONValue::Array(vec![value1, value2])
        }

        fn make_list_many(
            value1: JSONValue,
            value2: JSONValue,
            value3: JSONValue,
            value4: JSONValue,
        ) -> JSONValue {
            JSONValue::Array(vec![value1, value2, value3, value4])
        }

        fn make_list_single(value: JSONValue) -> JSONValue {
            JSONValue::Array(vec![value])
        }

        for (
            value,
            is_grouped,
            is_collection,
            expected_result_single_addr,
            expected_result_all_addr,
            expected_result_single_group,
            expected_result_all_group,
        ) in [
            (
                make_list(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                ),
                false,
                false,
                make_list(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                ),
                make_list_single(make_list(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                )),
                make_list_single(make_group(
                    None,
                    make_list(
                        make_email("John Doe", "jdoe@domain.com"),
                        make_email("Jane Smith", "jsmith@test.com"),
                    ),
                )),
                make_list_single(make_list_single(make_group(
                    None,
                    make_list(
                        make_email("John Doe", "jdoe@domain.com"),
                        make_email("Jane Smith", "jsmith@test.com"),
                    ),
                ))),
            ),
            (
                make_list(
                    make_list(
                        make_email("John Doe", "jdoe@domain.com"),
                        make_email("Jane Smith", "jsmith@test.com"),
                    ),
                    make_list(
                        make_email("Juan Gomez", "jgomez@dominio.com"),
                        make_email("Juanita Perez", "jperez@prueba.com"),
                    ),
                ),
                false,
                true,
                make_list(
                    make_email("Juan Gomez", "jgomez@dominio.com"),
                    make_email("Juanita Perez", "jperez@prueba.com"),
                ),
                make_list(
                    make_list(
                        make_email("John Doe", "jdoe@domain.com"),
                        make_email("Jane Smith", "jsmith@test.com"),
                    ),
                    make_list(
                        make_email("Juan Gomez", "jgomez@dominio.com"),
                        make_email("Juanita Perez", "jperez@prueba.com"),
                    ),
                ),
                make_list_single(make_group(
                    None,
                    make_list(
                        make_email("Juan Gomez", "jgomez@dominio.com"),
                        make_email("Juanita Perez", "jperez@prueba.com"),
                    ),
                )),
                make_list(
                    make_list_single(make_group(
                        None,
                        make_list(
                            make_email("John Doe", "jdoe@domain.com"),
                            make_email("Jane Smith", "jsmith@test.com"),
                        ),
                    )),
                    make_list_single(make_group(
                        None,
                        make_list(
                            make_email("Juan Gomez", "jgomez@dominio.com"),
                            make_email("Juanita Perez", "jperez@prueba.com"),
                        ),
                    )),
                ),
            ),
            (
                make_list(
                    make_group(
                        "Group 1".into(),
                        make_list(
                            make_email("John Doe", "jdoe@domain.com"),
                            make_email("Jane Smith", "jsmith@test.com"),
                        ),
                    ),
                    make_group(
                        "Group 2".into(),
                        make_list(
                            make_email("Juan Gomez", "jgomez@dominio.com"),
                            make_email("Juanita Perez", "jperez@prueba.com"),
                        ),
                    ),
                ),
                true,
                false,
                make_list_many(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                    make_email("Juan Gomez", "jgomez@dominio.com"),
                    make_email("Juanita Perez", "jperez@prueba.com"),
                ),
                make_list_single(make_list_many(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                    make_email("Juan Gomez", "jgomez@dominio.com"),
                    make_email("Juanita Perez", "jperez@prueba.com"),
                )),
                make_list(
                    make_group(
                        "Group 1".into(),
                        make_list(
                            make_email("John Doe", "jdoe@domain.com"),
                            make_email("Jane Smith", "jsmith@test.com"),
                        ),
                    ),
                    make_group(
                        "Group 2".into(),
                        make_list(
                            make_email("Juan Gomez", "jgomez@dominio.com"),
                            make_email("Juanita Perez", "jperez@prueba.com"),
                        ),
                    ),
                ),
                make_list_single(make_list(
                    make_group(
                        "Group 1".into(),
                        make_list(
                            make_email("John Doe", "jdoe@domain.com"),
                            make_email("Jane Smith", "jsmith@test.com"),
                        ),
                    ),
                    make_group(
                        "Group 2".into(),
                        make_list(
                            make_email("Juan Gomez", "jgomez@dominio.com"),
                            make_email("Juanita Perez", "jperez@prueba.com"),
                        ),
                    ),
                )),
            ),
            (
                make_list(
                    make_list(
                        make_group(
                            "Group 1".into(),
                            make_list(
                                make_email("Tim Hortons", "tim@hortos.com"),
                                make_email("Ronald McDowell", "ronnie@mac.com"),
                            ),
                        ),
                        make_group(
                            "Group 2".into(),
                            make_list(
                                make_email("Wendy D", "wendy@d.com"),
                                make_email("Kentucky Frango", "kentucky@frango.com"),
                            ),
                        ),
                    ),
                    make_list(
                        make_group(
                            "Group 3".into(),
                            make_list(
                                make_email("John Doe", "jdoe@domain.com"),
                                make_email("Jane Smith", "jsmith@test.com"),
                            ),
                        ),
                        make_group(
                            "Group 4".into(),
                            make_list(
                                make_email("Juan Gomez", "jgomez@dominio.com"),
                                make_email("Juanita Perez", "jperez@prueba.com"),
                            ),
                        ),
                    ),
                ),
                true,
                true,
                make_list_many(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                    make_email("Juan Gomez", "jgomez@dominio.com"),
                    make_email("Juanita Perez", "jperez@prueba.com"),
                ),
                make_list(
                    make_list_many(
                        make_email("Tim Hortons", "tim@hortos.com"),
                        make_email("Ronald McDowell", "ronnie@mac.com"),
                        make_email("Wendy D", "wendy@d.com"),
                        make_email("Kentucky Frango", "kentucky@frango.com"),
                    ),
                    make_list_many(
                        make_email("John Doe", "jdoe@domain.com"),
                        make_email("Jane Smith", "jsmith@test.com"),
                        make_email("Juan Gomez", "jgomez@dominio.com"),
                        make_email("Juanita Perez", "jperez@prueba.com"),
                    ),
                ),
                make_list(
                    make_group(
                        "Group 3".into(),
                        make_list(
                            make_email("John Doe", "jdoe@domain.com"),
                            make_email("Jane Smith", "jsmith@test.com"),
                        ),
                    ),
                    make_group(
                        "Group 4".into(),
                        make_list(
                            make_email("Juan Gomez", "jgomez@dominio.com"),
                            make_email("Juanita Perez", "jperez@prueba.com"),
                        ),
                    ),
                ),
                make_list(
                    make_list(
                        make_group(
                            "Group 1".into(),
                            make_list(
                                make_email("Tim Hortons", "tim@hortos.com"),
                                make_email("Ronald McDowell", "ronnie@mac.com"),
                            ),
                        ),
                        make_group(
                            "Group 2".into(),
                            make_list(
                                make_email("Wendy D", "wendy@d.com"),
                                make_email("Kentucky Frango", "kentucky@frango.com"),
                            ),
                        ),
                    ),
                    make_list(
                        make_group(
                            "Group 3".into(),
                            make_list(
                                make_email("John Doe", "jdoe@domain.com"),
                                make_email("Jane Smith", "jsmith@test.com"),
                            ),
                        ),
                        make_group(
                            "Group 4".into(),
                            make_list(
                                make_email("Juan Gomez", "jgomez@dominio.com"),
                                make_email("Juanita Perez", "jperez@prueba.com"),
                            ),
                        ),
                    ),
                ),
            ),
        ] {
            assert_eq!(
                super::transform_json_emailaddress(
                    value.clone(),
                    is_grouped,
                    is_collection,
                    false,
                    false
                ),
                expected_result_single_addr,
                "single+address"
            );
            assert_eq!(
                super::transform_json_emailaddress(
                    value.clone(),
                    is_grouped,
                    is_collection,
                    false,
                    true
                ),
                expected_result_all_addr,
                "all+address"
            );
            assert_eq!(
                super::transform_json_emailaddress(
                    value.clone(),
                    is_grouped,
                    is_collection,
                    true,
                    false
                ),
                expected_result_single_group,
                "single+group"
            );
            assert_eq!(
                super::transform_json_emailaddress(
                    value.clone(),
                    is_grouped,
                    is_collection,
                    true,
                    true
                ),
                expected_result_all_group,
                "all+group"
            );
        }
    }
}

*/
