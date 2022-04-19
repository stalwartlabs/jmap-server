pub mod changes;
pub mod get;
pub mod import;
pub mod parse;
pub mod query;
pub mod query_changes;
pub mod set;

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};

use jmap::{error::method::MethodError, protocol::json::JSONValue, request::JSONArgumentParser};
use mail_parser::{
    parsers::header::{parse_header_name, HeaderParserResult},
    HeaderOffset, MessagePartId, MessageStructure, RfcHeader,
};

use store::{
    bincode,
    blob::BlobIndex,
    serialize::{StoreDeserialize, StoreSerialize},
    FieldId, StoreError, Tag,
};

pub const MESSAGE_RAW: BlobIndex = 0;
pub const MESSAGE_DATA: BlobIndex = 1;
pub const MESSAGE_PARTS: BlobIndex = 2;

pub type JMAPMailHeaders = HashMap<MailProperties, JSONValue>;
pub type JMAPMailMimeHeaders = HashMap<MailBodyProperties, JSONValue>;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageData {
    pub properties: HashMap<MailProperties, JSONValue>,
    pub mime_parts: Vec<MimePart>,
    pub html_body: Vec<MessagePartId>,
    pub text_body: Vec<MessagePartId>,
    pub attachments: Vec<MessagePartId>,
}

impl StoreSerialize for MessageData {
    fn serialize(&self) -> Option<Vec<u8>> {
        rmp_serde::encode::to_vec(self).ok()
    }
}

impl StoreDeserialize for MessageData {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        rmp_serde::decode::from_slice(bytes).ok()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageOutline {
    pub body_offset: usize,
    pub body_structure: MessageStructure,
    pub headers: Vec<HashMap<HeaderName, Vec<HeaderOffset>>>,
    pub received_at: i64,
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum MimePartType {
    Text,
    Html,
    Other,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MimePart {
    pub headers: JMAPMailMimeHeaders,
    pub blob_index: BlobIndex,
    pub is_encoding_problem: bool,
    pub mime_type: MimePartType,
}

impl MimePart {
    pub fn new_html(
        headers: JMAPMailMimeHeaders,
        blob_index: BlobIndex,
        is_encoding_problem: bool,
    ) -> Self {
        MimePart {
            headers,
            blob_index,
            is_encoding_problem,
            mime_type: MimePartType::Html,
        }
    }

    pub fn new_text(
        headers: JMAPMailMimeHeaders,
        blob_index: BlobIndex,
        is_encoding_problem: bool,
    ) -> Self {
        MimePart {
            headers,
            blob_index,
            is_encoding_problem,
            mime_type: MimePartType::Text,
        }
    }

    pub fn new_other(
        headers: JMAPMailMimeHeaders,
        blob_index: BlobIndex,
        is_encoding_problem: bool,
    ) -> Self {
        MimePart {
            headers,
            blob_index,
            is_encoding_problem,
            mime_type: MimePartType::Other,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageField {
    Internal = 127,
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
}

impl From<MessageField> for FieldId {
    fn from(field: MessageField) -> Self {
        field as FieldId
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum MailHeaderForm {
    Raw,
    Text,
    Addresses,
    GroupedAddresses,
    MessageIds,
    Date,
    URLs,
}

impl MailHeaderForm {
    pub fn parse(value: &str) -> Option<MailHeaderForm> {
        match value {
            "asText" => Some(MailHeaderForm::Text),
            "asAddresses" => Some(MailHeaderForm::Addresses),
            "asGroupedAddresses" => Some(MailHeaderForm::GroupedAddresses),
            "asMessageIds" => Some(MailHeaderForm::MessageIds),
            "asDate" => Some(MailHeaderForm::Date),
            "asURLs" => Some(MailHeaderForm::URLs),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct MailHeaderProperty {
    pub form: MailHeaderForm,
    pub header: HeaderName,
    pub all: bool,
}

impl MailHeaderProperty {
    pub fn new_rfc(header: RfcHeader, form: MailHeaderForm, all: bool) -> Self {
        MailHeaderProperty {
            form,
            header: HeaderName::Rfc(header),
            all,
        }
    }
    pub fn new_other(header: String, form: MailHeaderForm, all: bool) -> Self {
        MailHeaderProperty {
            form,
            header: HeaderName::Other(header),
            all,
        }
    }

    pub fn parse(value: &str) -> Option<MailHeaderProperty> {
        let mut all = false;
        let mut form = MailHeaderForm::Raw;
        let mut header = None;
        for (pos, part) in value.split(':').enumerate() {
            match pos {
                0 if part == "header" => (),
                1 => match parse_header_name(part.as_bytes()) {
                    (_, HeaderParserResult::Rfc(rfc_header)) => {
                        header = Some(HeaderName::Rfc(rfc_header));
                    }
                    (_, HeaderParserResult::Other(other_header)) => {
                        header = Some(HeaderName::Other(other_header.as_ref().to_owned()));
                    }
                    _ => return None,
                },
                2 | 3 if part == "all" => all = true,
                2 => {
                    form = MailHeaderForm::parse(part)?;
                }
                _ => return None,
            }
        }
        Some(MailHeaderProperty {
            form,
            header: header?,
            all,
        })
    }
}

impl JSONArgumentParser for MailHeaderProperty {
    fn parse_argument(argument: JSONValue) -> jmap::Result<Self> {
        let argument = argument.unwrap_string().ok_or_else(|| {
            MethodError::InvalidArguments("Expected string argument.".to_string())
        })?;
        MailHeaderProperty::parse(&argument).ok_or_else(|| {
            MethodError::InvalidArguments(format!("Unknown property: '{}'.", argument))
        })
    }
}

impl Display for MailHeaderProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "header:")?;
        match &self.header {
            HeaderName::Rfc(rfc) => rfc.fmt(f)?,
            HeaderName::Other(name) => name.fmt(f)?,
        }
        match self.form {
            MailHeaderForm::Raw => (),
            MailHeaderForm::Text => write!(f, ":asText")?,
            MailHeaderForm::Addresses => write!(f, ":asAddresses")?,
            MailHeaderForm::GroupedAddresses => write!(f, ":asGroupedAddresses")?,
            MailHeaderForm::MessageIds => write!(f, ":asMessageIds")?,
            MailHeaderForm::Date => write!(f, ":asDate")?,
            MailHeaderForm::URLs => write!(f, ":asURLs")?,
        }
        if self.all {
            write!(f, ":all")
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub enum MailProperties {
    Id,
    BlobId,
    ThreadId,
    MailboxIds,
    Keywords,
    Size,
    ReceivedAt,
    MessageId,
    InReplyTo,
    References,
    Sender,
    From,
    To,
    Cc,
    Bcc,
    ReplyTo,
    Subject,
    SentAt,
    HasAttachment,
    Preview,
    BodyValues,
    TextBody,
    HtmlBody,
    Attachments,
    BodyStructure,
    Header(MailHeaderProperty),
}

impl Display for MailProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MailProperties::Id => write!(f, "id"),
            MailProperties::BlobId => write!(f, "blobId"),
            MailProperties::ThreadId => write!(f, "threadId"),
            MailProperties::MailboxIds => write!(f, "mailboxIds"),
            MailProperties::Keywords => write!(f, "keywords"),
            MailProperties::Size => write!(f, "size"),
            MailProperties::ReceivedAt => write!(f, "receivedAt"),
            MailProperties::MessageId => write!(f, "messageId"),
            MailProperties::InReplyTo => write!(f, "inReplyTo"),
            MailProperties::References => write!(f, "references"),
            MailProperties::Sender => write!(f, "sender"),
            MailProperties::From => write!(f, "from"),
            MailProperties::To => write!(f, "to"),
            MailProperties::Cc => write!(f, "cc"),
            MailProperties::Bcc => write!(f, "bcc"),
            MailProperties::ReplyTo => write!(f, "replyTo"),
            MailProperties::Subject => write!(f, "subject"),
            MailProperties::SentAt => write!(f, "sentAt"),
            MailProperties::HasAttachment => write!(f, "hasAttachment"),
            MailProperties::Preview => write!(f, "preview"),
            MailProperties::BodyValues => write!(f, "bodyValues"),
            MailProperties::TextBody => write!(f, "textBody"),
            MailProperties::HtmlBody => write!(f, "htmlBody"),
            MailProperties::Attachments => write!(f, "attachments"),
            MailProperties::BodyStructure => write!(f, "bodyStructure"),
            MailProperties::Header(header) => header.fmt(f),
        }
    }
}

impl MailProperties {
    pub fn parse(value: &str) -> Option<MailProperties> {
        match value {
            "id" => Some(MailProperties::Id),
            "blobId" => Some(MailProperties::BlobId),
            "threadId" => Some(MailProperties::ThreadId),
            "mailboxIds" => Some(MailProperties::MailboxIds),
            "keywords" => Some(MailProperties::Keywords),
            "size" => Some(MailProperties::Size),
            "receivedAt" => Some(MailProperties::ReceivedAt),
            "messageId" => Some(MailProperties::MessageId),
            "inReplyTo" => Some(MailProperties::InReplyTo),
            "references" => Some(MailProperties::References),
            "sender" => Some(MailProperties::Sender),
            "from" => Some(MailProperties::From),
            "to" => Some(MailProperties::To),
            "cc" => Some(MailProperties::Cc),
            "bcc" => Some(MailProperties::Bcc),
            "replyTo" => Some(MailProperties::ReplyTo),
            "subject" => Some(MailProperties::Subject),
            "sentAt" => Some(MailProperties::SentAt),
            "hasAttachment" => Some(MailProperties::HasAttachment),
            "preview" => Some(MailProperties::Preview),
            "bodyValues" => Some(MailProperties::BodyValues),
            "textBody" => Some(MailProperties::TextBody),
            "htmlBody" => Some(MailProperties::HtmlBody),
            "attachments" => Some(MailProperties::Attachments),
            "bodyStructure" => Some(MailProperties::BodyStructure),
            _ if value.starts_with("header:") => {
                Some(MailProperties::Header(MailHeaderProperty::parse(value)?))
            }
            _ => None,
        }
    }

    pub fn as_rfc_header(&self) -> RfcHeader {
        match self {
            MailProperties::MessageId => RfcHeader::MessageId,
            MailProperties::InReplyTo => RfcHeader::InReplyTo,
            MailProperties::References => RfcHeader::References,
            MailProperties::Sender => RfcHeader::Sender,
            MailProperties::From => RfcHeader::From,
            MailProperties::To => RfcHeader::To,
            MailProperties::Cc => RfcHeader::Cc,
            MailProperties::Bcc => RfcHeader::Bcc,
            MailProperties::ReplyTo => RfcHeader::ReplyTo,
            MailProperties::Subject => RfcHeader::Subject,
            MailProperties::SentAt => RfcHeader::Date,
            MailProperties::Header(MailHeaderProperty {
                header: HeaderName::Rfc(rfc),
                ..
            }) => *rfc,
            _ => unreachable!(),
        }
    }
}

impl Default for MailProperties {
    fn default() -> Self {
        MailProperties::Id
    }
}

impl JSONArgumentParser for MailProperties {
    fn parse_argument(argument: JSONValue) -> jmap::Result<Self> {
        let argument = argument.unwrap_string().ok_or_else(|| {
            MethodError::InvalidArguments("Expected string argument.".to_string())
        })?;
        MailProperties::parse(&argument).ok_or_else(|| {
            MethodError::InvalidArguments(format!("Unknown property: '{}'.", argument))
        })
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum MailBodyProperties {
    PartId,
    BlobId,
    Size,
    Name,
    Type,
    Charset,
    Header(MailHeaderProperty),
    Headers,
    Disposition,
    Cid,
    Language,
    Location,
    Subparts,
}

impl MailBodyProperties {
    pub fn parse(value: &str) -> Option<MailBodyProperties> {
        match value {
            "partId" => Some(MailBodyProperties::PartId),
            "blobId" => Some(MailBodyProperties::BlobId),
            "size" => Some(MailBodyProperties::Size),
            "name" => Some(MailBodyProperties::Name),
            "type" => Some(MailBodyProperties::Type),
            "charset" => Some(MailBodyProperties::Charset),
            "headers" => Some(MailBodyProperties::Headers),
            "disposition" => Some(MailBodyProperties::Disposition),
            "cid" => Some(MailBodyProperties::Cid),
            "language" => Some(MailBodyProperties::Language),
            "location" => Some(MailBodyProperties::Location),
            "subParts" => Some(MailBodyProperties::Subparts),
            _ if value.starts_with("header:") => Some(MailBodyProperties::Header(
                MailHeaderProperty::parse(value)?,
            )),
            _ => None,
        }
    }
}

impl JSONArgumentParser for MailBodyProperties {
    fn parse_argument(argument: JSONValue) -> jmap::Result<Self> {
        let argument = argument.unwrap_string().ok_or_else(|| {
            MethodError::InvalidArguments("Expected string argument.".to_string())
        })?;
        MailBodyProperties::parse(&argument).ok_or_else(|| {
            MethodError::InvalidArguments(format!("Unknown property: '{}'.", argument))
        })
    }
}

impl Display for MailBodyProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MailBodyProperties::PartId => write!(f, "partId"),
            MailBodyProperties::BlobId => write!(f, "blobId"),
            MailBodyProperties::Size => write!(f, "size"),
            MailBodyProperties::Name => write!(f, "name"),
            MailBodyProperties::Type => write!(f, "type"),
            MailBodyProperties::Charset => write!(f, "charset"),
            MailBodyProperties::Header(header) => header.fmt(f),
            MailBodyProperties::Headers => write!(f, "headers"),
            MailBodyProperties::Disposition => write!(f, "disposition"),
            MailBodyProperties::Cid => write!(f, "cid"),
            MailBodyProperties::Language => write!(f, "language"),
            MailBodyProperties::Location => write!(f, "location"),
            MailBodyProperties::Subparts => write!(f, "subParts"),
        }
    }
}

pub struct Keyword {
    pub tag: Tag,
}

impl Keyword {
    pub const SEEN: u8 = 0;
    pub const DRAFT: u8 = 1;
    pub const FLAGGED: u8 = 2;
    pub const ANSWERED: u8 = 3;
    pub const RECENT: u8 = 4;
    pub const IMPORTANT: u8 = 5;
    pub const PHISHING: u8 = 6;
    pub const JUNK: u8 = 7;
    pub const NOTJUNK: u8 = 8;

    pub fn from_jmap(value: String) -> Tag {
        if value.starts_with('$') {
            match value.as_str() {
                "$seen" => Tag::Static(Self::SEEN),
                "$draft" => Tag::Static(Self::DRAFT),
                "$flagged" => Tag::Static(Self::FLAGGED),
                "$answered" => Tag::Static(Self::ANSWERED),
                "$recent" => Tag::Static(Self::RECENT),
                "$important" => Tag::Static(Self::IMPORTANT),
                "$phishing" => Tag::Static(Self::PHISHING),
                "$junk" => Tag::Static(Self::JUNK),
                "$notjunk" => Tag::Static(Self::NOTJUNK),
                _ => Tag::Text(value),
            }
        } else {
            Tag::Text(value)
        }
    }

    pub fn to_jmap(keyword: Tag) -> store::Result<String> {
        match keyword {
            Tag::Static(keyword) => match keyword {
                Self::SEEN => Ok("$seen".to_string()),
                Self::DRAFT => Ok("$draft".to_string()),
                Self::FLAGGED => Ok("$flagged".to_string()),
                Self::ANSWERED => Ok("$answered".to_string()),
                Self::RECENT => Ok("$recent".to_string()),
                Self::IMPORTANT => Ok("$important".to_string()),
                Self::PHISHING => Ok("$phishing".to_string()),
                Self::JUNK => Ok("$junk".to_string()),
                Self::NOTJUNK => Ok("$notjunk".to_string()),
                9..=u8::MAX => Err(StoreError::InternalError(format!(
                    "Invalid keyword id {}",
                    keyword
                ))),
            },
            Tag::Text(value) => Ok(value),
            _ => Err(StoreError::InternalError(format!(
                "Invalid keyword tag {:?}",
                keyword
            ))),
        }
    }
}

impl JSONArgumentParser for Keyword {
    fn parse_argument(argument: JSONValue) -> jmap::Result<Self> {
        Ok(Keyword {
            tag: Keyword::from_jmap(argument.parse_string()?),
        })
    }
}
