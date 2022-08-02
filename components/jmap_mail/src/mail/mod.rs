pub mod changes;
pub mod conv;
pub mod copy;
pub mod get;
pub mod import;
pub mod parse;
pub mod query;
pub mod raft;
pub mod schema;
pub mod search_snippet;
pub mod serialize;
pub mod set;
pub mod sharing;

use jmap::{jmap_store::Object, types::jmap::JMAPId};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, fmt::Display};

use mail_parser::{
    decoders::{
        base64::decode_base64, charsets::map::get_charset_decoder,
        quoted_printable::decode_quoted_printable,
    },
    parsers::message::MessageStream,
    Encoding, Header, MessagePartId, RfcHeader,
};

use store::{
    bincode,
    blob::BlobId,
    core::{collection::Collection, vec_map::VecMap},
    serialize::{StoreDeserialize, StoreSerialize},
    FieldId,
};

use self::schema::{Email, EmailAddress, EmailAddressGroup, Property, Value};

pub const MAX_MESSAGE_PARTS: usize = 1000;

impl Object for Email {
    type Property = Property;

    type Value = ();

    fn id(&self) -> Option<&JMAPId> {
        self.properties.get(&Property::Id).and_then(|id| match id {
            Value::Id { value } => Some(value),
            _ => None,
        })
    }

    fn required() -> &'static [Self::Property] {
        &[]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[]
    }

    fn collection() -> Collection {
        Collection::Mail
    }

    fn new(id: JMAPId) -> Self {
        let mut email = Email::default();
        email
            .properties
            .append(Property::Id, Value::Id { value: id });
        email
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageData {
    pub headers: VecMap<RfcHeader, Vec<HeaderValue>>,
    pub mime_parts: Vec<MimePart>,
    pub html_body: Vec<MessagePartId>,
    pub text_body: Vec<MessagePartId>,
    pub attachments: Vec<MessagePartId>,
    pub raw_message: BlobId,
    pub size: usize,
    pub received_at: i64,
    pub has_attachments: bool,
    pub body_offset: usize,
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

pub trait GetRawHeader {
    fn get_raw_header(&self, name: &HeaderName) -> Option<Vec<(usize, usize)>>;
}

impl GetRawHeader for Vec<(HeaderName, usize, usize)> {
    fn get_raw_header(&self, name: &HeaderName) -> Option<Vec<(usize, usize)>> {
        let name = name.as_str();
        let offsets = self
            .iter()
            .filter_map(|(k, start, end)| {
                if k.as_str().eq_ignore_ascii_case(name) {
                    Some((*start, *end))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if !offsets.is_empty() {
            Some(offsets)
        } else {
            None
        }
    }
}

impl GetRawHeader for Vec<Header<'_>> {
    fn get_raw_header(&self, name: &HeaderName) -> Option<Vec<(usize, usize)>> {
        let name = name.as_str();
        let offsets = self
            .iter()
            .filter_map(|h| {
                if h.name.as_str().eq_ignore_ascii_case(name) {
                    Some((h.offset_start, h.offset_end))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if !offsets.is_empty() {
            Some(offsets)
        } else {
            None
        }
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

impl From<RfcHeader> for HeaderName {
    fn from(header: RfcHeader) -> Self {
        HeaderName::Rfc(header)
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
    Text { part: MessagePart },
    Html { part: MessagePart },
    Other { part: MessagePart },
    MultiPart { subparts: Vec<MessagePartId> },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct MessagePart {
    pub offset_start: usize,
    pub offset_end: usize,
    pub encoding: Encoding,
}

impl MessagePart {
    pub fn decode(&self, raw_message: &[u8]) -> Option<Vec<u8>> {
        let data = raw_message.get(self.offset_start..self.offset_end)?;
        let decode_fnc = match self.encoding {
            Encoding::Base64 => decode_base64,
            Encoding::QuotedPrintable => decode_quoted_printable,
            Encoding::None => {
                return Some(data.to_vec());
            }
        };

        match decode_fnc(&MessageStream { data, pos: 0 }, 0, &[][..], false).1 {
            mail_parser::decoders::DecodeResult::Owned(bytes) => Some(bytes),
            mail_parser::decoders::DecodeResult::Borrowed((start, end)) => {
                data.get(start..end).map(|b| b.to_vec())
            }
            mail_parser::decoders::DecodeResult::Empty => None,
        }
    }

    pub fn decode_text(
        &self,
        raw_message: &[u8],
        charset: Option<&str>,
        remove_cr: bool,
    ) -> Option<String> {
        let mut bytes = self.decode(raw_message)?;

        if remove_cr {
            bytes = bytes.into_iter().filter(|&b| b != b'\r').collect();
        }

        if let Some(charset) = charset {
            if let Some(decoder) = get_charset_decoder(charset.as_bytes()) {
                return decoder(&bytes).into();
            }
        }

        String::from_utf8(bytes)
            .map_or_else(
                |err| String::from_utf8_lossy(err.as_bytes()).into_owned(),
                |s| s,
            )
            .into()
    }
}

impl Default for MimePartType {
    fn default() -> Self {
        MimePartType::MultiPart {
            subparts: Vec::new(),
        }
    }
}

impl MimePartType {
    pub fn is_html(&self) -> bool {
        matches!(self, MimePartType::Html { .. })
    }

    pub fn is_text(&self) -> bool {
        matches!(self, MimePartType::Text { .. })
    }

    pub fn part(&self) -> Option<&MessagePart> {
        match self {
            MimePartType::Text { part } => Some(part),
            MimePartType::Html { part } => Some(part),
            MimePartType::Other { part } => Some(part),
            MimePartType::MultiPart { .. } => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MimePart {
    pub mime_type: MimePartType,
    pub is_encoding_problem: bool,
    pub raw_headers: Vec<(HeaderName, usize, usize)>,
    // Headers
    pub type_: Option<String>,
    pub charset: Option<String>,
    pub name: Option<String>,
    pub disposition: Option<String>,
    pub location: Option<String>,
    pub language: Option<Vec<String>>,
    pub cid: Option<String>,
    pub size: usize,
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub enum HeaderValue {
    Timestamp(i64),
    Text(String),
    TextList(Vec<String>),
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
            HeaderValue::TextList(mut textlist) => textlist.pop(),
            _ => None,
        }
    }

    pub fn unwrap_textlist(self) -> Option<Vec<String>> {
        match self {
            HeaderValue::Text(text) => Some(vec![text]),
            HeaderValue::TextList(textlist) => Some(textlist),
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
