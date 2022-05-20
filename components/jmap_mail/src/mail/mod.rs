//pub mod changes;
//pub mod get;
pub mod import;
//pub mod parse;
//pub mod query;
//pub mod raft;
pub mod conv;
pub mod schema;
pub mod set;

use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::HashMap, fmt::Display};

use mail_parser::{HeaderOffset, MessagePartId, MessageStructure, RfcHeader};

use store::{
    bincode,
    blob::BlobId,
    serialize::{StoreDeserialize, StoreSerialize},
    FieldId,
};

use self::schema::{EmailAddress, EmailAddressGroup};

pub const MAX_MESSAGE_PARTS: usize = 1000;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageData {
    pub headers: HashMap<RfcHeader, Vec<HeaderValue>>,
    pub mime_parts: Vec<MimePart>,
    pub html_body: Vec<MessagePartId>,
    pub text_body: Vec<MessagePartId>,
    pub attachments: Vec<MessagePartId>,
    pub raw_message: BlobId,
    pub size: usize,
    pub received_at: i64,
    pub has_attachments: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MimeHeaders {
    type_: Option<String>,
    charset: Option<String>,
    name: Option<String>,
    disposition: Option<String>,
    location: Option<String>,
    language: Option<Vec<String>>,
    cid: Option<String>,
    size: usize,
}

impl StoreSerialize for MessageData {
    fn serialize(&self) -> Option<Vec<u8>> {
        bincode::serialize(self).ok()
    }
}

impl StoreDeserialize for MessageData {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize_from(bytes).ok()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageOutline {
    pub body_offset: usize,
    pub body_structure: MessageStructure,
    pub headers: Vec<HashMap<HeaderName, Vec<HeaderOffset>>>,
}

impl StoreSerialize for MessageOutline {
    fn serialize(&self) -> Option<Vec<u8>> {
        bincode::serialize(self).ok()
    }
}

impl StoreDeserialize for MessageOutline {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize_from(bytes).ok()
    }
}

impl From<mail_parser::HeaderName<'_>> for HeaderName {
    fn from(header: mail_parser::HeaderName) -> Self {
        match header {
            mail_parser::HeaderName::Rfc(rfc) => HeaderName::Rfc(rfc),
            mail_parser::HeaderName::Other(other) => HeaderName::Other(other.into_owned()),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum HeaderName {
    Rfc(RfcHeader),
    Other(String),
}

impl HeaderName {
    pub fn as_str(&self) -> &str {
        match self {
            HeaderName::Rfc(rfc) => rfc.as_str(),
            HeaderName::Other(other) => other,
        }
    }

    pub fn unwrap(self) -> String {
        match self {
            HeaderName::Rfc(rfc) => rfc.as_str().to_owned(),
            HeaderName::Other(other) => other,
        }
    }
}

impl From<HeaderName> for Cow<'_, str> {
    fn from(header: HeaderName) -> Self {
        header.to_string().into()
    }
}

impl Display for HeaderName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum MimePartType {
    Text,
    Html,
    Other,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MimePart {
    pub headers: MimeHeaders,
    pub blob_id: Option<BlobId>,
    pub is_encoding_problem: bool,
    pub mime_type: MimePartType,
}

impl MimePart {
    pub fn new_html(headers: MimeHeaders, blob_id: BlobId, is_encoding_problem: bool) -> Self {
        MimePart {
            headers,
            blob_id: blob_id.into(),
            is_encoding_problem,
            mime_type: MimePartType::Html,
        }
    }

    pub fn new_text(headers: MimeHeaders, blob_id: BlobId, is_encoding_problem: bool) -> Self {
        MimePart {
            headers,
            blob_id: blob_id.into(),
            is_encoding_problem,
            mime_type: MimePartType::Text,
        }
    }

    pub fn new_binary(headers: MimeHeaders, blob_id: BlobId, is_encoding_problem: bool) -> Self {
        MimePart {
            headers,
            blob_id: blob_id.into(),
            is_encoding_problem,
            mime_type: MimePartType::Other,
        }
    }

    pub fn new_part(headers: MimeHeaders) -> Self {
        MimePart {
            headers,
            blob_id: None,
            is_encoding_problem: false,
            mime_type: MimePartType::Other,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum MessageField {
    Metadata = 127,
    Body = 128,
    Attachment = 129,
    ReceivedAt = 130,
    Size = 131,
    Keyword = 132,
    Thread = 133,
    ThreadName = 134,
    MessageIdRef = 135,
    ThreadId = 136,
    Mailbox = 137,
    HasHeader = 138,
}

impl From<MessageField> for FieldId {
    fn from(field: MessageField) -> Self {
        field as FieldId
    }
}

impl Display for MessageField {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl MessageData {
    pub fn from_metadata(bytes: &[u8]) -> Option<Self> {
        use store::serialize::leb128::Leb128;

        let (message_data_len, read_bytes) = usize::from_leb128_bytes(bytes)?;

        <MessageData as StoreDeserialize>::deserialize(
            bytes.get(read_bytes..read_bytes + message_data_len)?,
        )
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub enum HeaderValue {
    Timestamp(i64),
    Text(String),
    Keywords(Vec<String>),
    Urls(Vec<String>),
    Addresses(Vec<EmailAddress>),
    GroupedAddresses(Vec<EmailAddressGroup>),
}

impl HeaderValue {
    pub fn unwrap_timestamp(self) -> Option<i64> {
        match self {
            HeaderValue::Timestamp(timestamp) => Some(timestamp),
            _ => None,
        }
    }

    pub fn unwrap_text(self) -> Option<String> {
        match self {
            HeaderValue::Text(text) => Some(text),
            HeaderValue::Keywords(mut textlist) | HeaderValue::Urls(mut textlist) => textlist.pop(),
            _ => None,
        }
    }

    pub fn unwrap_textlist(self) -> Option<Vec<String>> {
        match self {
            HeaderValue::Text(text) => Some(vec![text]),
            HeaderValue::Keywords(mut textlist) | HeaderValue::Urls(mut textlist) => Some(textlist),
            _ => None,
        }
    }

    pub fn visit_addresses(self, mut visitor: impl FnMut(String, bool) -> bool) {
        match self {
            HeaderValue::Addresses(addresses) => {
                for address in addresses {
                    if let Some(name) = address.name {
                        if !visitor(name, false) {
                            return;
                        }
                    }
                    if !visitor(address.email, true) {
                        return;
                    }
                }
            }
            HeaderValue::GroupedAddresses(grouplist) => {
                for group in grouplist {
                    if let Some(name) = group.name {
                        if !visitor(name, false) {
                            return;
                        }
                    }

                    for address in group.addresses {
                        if let Some(name) = address.name {
                            if !visitor(name, false) {
                                return;
                            }
                        }
                        if !visitor(address.email, true) {
                            return;
                        }
                    }
                }
            }
            _ => (),
        }
    }
}
