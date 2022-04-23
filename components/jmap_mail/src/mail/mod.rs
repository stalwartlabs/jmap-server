pub mod changes;
pub mod get;
pub mod import;
pub mod parse;
pub mod query;
pub mod set;

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};

use jmap::{
    error::method::MethodError, protocol::json::JSONValue, request::JSONArgumentParser, Property,
};
use mail_parser::{
    parsers::header::{parse_header_name, HeaderParserResult},
    HeaderOffset, MessagePartId, MessageStructure, RfcHeader,
};

use store::{
    bincode,
    blob::BlobIndex,
    serialize::{StoreDeserialize, StoreSerialize},
    Collection, FieldId, StoreError, Tag,
};

pub const MESSAGE_RAW: BlobIndex = 0;
pub const MESSAGE_DATA: BlobIndex = 1;
pub const MESSAGE_PARTS: BlobIndex = 2;

pub type JMAPMailHeaders = HashMap<MailProperty, JSONValue>;
pub type JMAPMailMimeHeaders = HashMap<MailBodyProperty, JSONValue>;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageData {
    pub properties: HashMap<MailProperty, JSONValue>,
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
    HasHeader = 138,
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

impl Property for MailHeaderForm {
    fn parse(value: &str) -> Option<MailHeaderForm> {
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

    fn collection() -> Collection {
        Collection::Mail
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
}

impl Property for MailHeaderProperty {
    fn parse(value: &str) -> Option<MailHeaderProperty> {
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

    fn collection() -> Collection {
        Collection::Mail
    }
}

impl Display for MailHeaderProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "header:")?;
        match &self.header {
            HeaderName::Rfc(rfc) => rfc.fmt(f)?,
            HeaderName::Other(name) => name.fmt(f)?,
        }
        self.form.fmt(f)?;
        if self.all {
            write!(f, ":all")
        } else {
            Ok(())
        }
    }
}

impl Display for MailHeaderForm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MailHeaderForm::Raw => Ok(()),
            MailHeaderForm::Text => write!(f, ":asText"),
            MailHeaderForm::Addresses => write!(f, ":asAddresses"),
            MailHeaderForm::GroupedAddresses => write!(f, ":asGroupedAddresses"),
            MailHeaderForm::MessageIds => write!(f, ":asMessageIds"),
            MailHeaderForm::Date => write!(f, ":asDate"),
            MailHeaderForm::URLs => write!(f, ":asURLs"),
        }
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

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub enum MailProperty {
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

impl Property for MailProperty {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "id" => Some(MailProperty::Id),
            "blobId" => Some(MailProperty::BlobId),
            "threadId" => Some(MailProperty::ThreadId),
            "mailboxIds" => Some(MailProperty::MailboxIds),
            "keywords" => Some(MailProperty::Keywords),
            "size" => Some(MailProperty::Size),
            "receivedAt" => Some(MailProperty::ReceivedAt),
            "messageId" => Some(MailProperty::MessageId),
            "inReplyTo" => Some(MailProperty::InReplyTo),
            "references" => Some(MailProperty::References),
            "sender" => Some(MailProperty::Sender),
            "from" => Some(MailProperty::From),
            "to" => Some(MailProperty::To),
            "cc" => Some(MailProperty::Cc),
            "bcc" => Some(MailProperty::Bcc),
            "replyTo" => Some(MailProperty::ReplyTo),
            "subject" => Some(MailProperty::Subject),
            "sentAt" => Some(MailProperty::SentAt),
            "hasAttachment" => Some(MailProperty::HasAttachment),
            "preview" => Some(MailProperty::Preview),
            "bodyValues" => Some(MailProperty::BodyValues),
            "textBody" => Some(MailProperty::TextBody),
            "htmlBody" => Some(MailProperty::HtmlBody),
            "attachments" => Some(MailProperty::Attachments),
            "bodyStructure" => Some(MailProperty::BodyStructure),
            _ if value.starts_with("header:") => {
                Some(MailProperty::Header(MailHeaderProperty::parse(value)?))
            }
            _ => None,
        }
    }

    fn collection() -> Collection {
        Collection::Mail
    }
}

impl Display for MailProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MailProperty::Id => write!(f, "id"),
            MailProperty::BlobId => write!(f, "blobId"),
            MailProperty::ThreadId => write!(f, "threadId"),
            MailProperty::MailboxIds => write!(f, "mailboxIds"),
            MailProperty::Keywords => write!(f, "keywords"),
            MailProperty::Size => write!(f, "size"),
            MailProperty::ReceivedAt => write!(f, "receivedAt"),
            MailProperty::MessageId => write!(f, "messageId"),
            MailProperty::InReplyTo => write!(f, "inReplyTo"),
            MailProperty::References => write!(f, "references"),
            MailProperty::Sender => write!(f, "sender"),
            MailProperty::From => write!(f, "from"),
            MailProperty::To => write!(f, "to"),
            MailProperty::Cc => write!(f, "cc"),
            MailProperty::Bcc => write!(f, "bcc"),
            MailProperty::ReplyTo => write!(f, "replyTo"),
            MailProperty::Subject => write!(f, "subject"),
            MailProperty::SentAt => write!(f, "sentAt"),
            MailProperty::HasAttachment => write!(f, "hasAttachment"),
            MailProperty::Preview => write!(f, "preview"),
            MailProperty::BodyValues => write!(f, "bodyValues"),
            MailProperty::TextBody => write!(f, "textBody"),
            MailProperty::HtmlBody => write!(f, "htmlBody"),
            MailProperty::Attachments => write!(f, "attachments"),
            MailProperty::BodyStructure => write!(f, "bodyStructure"),
            MailProperty::Header(header) => header.fmt(f),
        }
    }
}

impl MailProperty {
    pub fn as_rfc_header(&self) -> RfcHeader {
        match self {
            MailProperty::MessageId => RfcHeader::MessageId,
            MailProperty::InReplyTo => RfcHeader::InReplyTo,
            MailProperty::References => RfcHeader::References,
            MailProperty::Sender => RfcHeader::Sender,
            MailProperty::From => RfcHeader::From,
            MailProperty::To => RfcHeader::To,
            MailProperty::Cc => RfcHeader::Cc,
            MailProperty::Bcc => RfcHeader::Bcc,
            MailProperty::ReplyTo => RfcHeader::ReplyTo,
            MailProperty::Subject => RfcHeader::Subject,
            MailProperty::SentAt => RfcHeader::Date,
            MailProperty::Header(MailHeaderProperty {
                header: HeaderName::Rfc(rfc),
                ..
            }) => *rfc,
            _ => unreachable!(),
        }
    }
}

impl Default for MailProperty {
    fn default() -> Self {
        MailProperty::Id
    }
}

impl JSONArgumentParser for MailProperty {
    fn parse_argument(argument: JSONValue) -> jmap::Result<Self> {
        let argument = argument.unwrap_string().ok_or_else(|| {
            MethodError::InvalidArguments("Expected string argument.".to_string())
        })?;
        MailProperty::parse(&argument).ok_or_else(|| {
            MethodError::InvalidArguments(format!("Unknown property: '{}'.", argument))
        })
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum MailBodyProperty {
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

impl Property for MailBodyProperty {
    fn parse(value: &str) -> Option<MailBodyProperty> {
        match value {
            "partId" => Some(MailBodyProperty::PartId),
            "blobId" => Some(MailBodyProperty::BlobId),
            "size" => Some(MailBodyProperty::Size),
            "name" => Some(MailBodyProperty::Name),
            "type" => Some(MailBodyProperty::Type),
            "charset" => Some(MailBodyProperty::Charset),
            "headers" => Some(MailBodyProperty::Headers),
            "disposition" => Some(MailBodyProperty::Disposition),
            "cid" => Some(MailBodyProperty::Cid),
            "language" => Some(MailBodyProperty::Language),
            "location" => Some(MailBodyProperty::Location),
            "subParts" => Some(MailBodyProperty::Subparts),
            _ if value.starts_with("header:") => {
                Some(MailBodyProperty::Header(MailHeaderProperty::parse(value)?))
            }
            _ => None,
        }
    }

    fn collection() -> Collection {
        Collection::Mail
    }
}

impl Display for MailBodyProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MailBodyProperty::PartId => write!(f, "partId"),
            MailBodyProperty::BlobId => write!(f, "blobId"),
            MailBodyProperty::Size => write!(f, "size"),
            MailBodyProperty::Name => write!(f, "name"),
            MailBodyProperty::Type => write!(f, "type"),
            MailBodyProperty::Charset => write!(f, "charset"),
            MailBodyProperty::Header(header) => header.fmt(f),
            MailBodyProperty::Headers => write!(f, "headers"),
            MailBodyProperty::Disposition => write!(f, "disposition"),
            MailBodyProperty::Cid => write!(f, "cid"),
            MailBodyProperty::Language => write!(f, "language"),
            MailBodyProperty::Location => write!(f, "location"),
            MailBodyProperty::Subparts => write!(f, "subParts"),
        }
    }
}

impl JSONArgumentParser for MailBodyProperty {
    fn parse_argument(argument: JSONValue) -> jmap::Result<Self> {
        let argument = argument.unwrap_string().ok_or_else(|| {
            MethodError::InvalidArguments("Expected string argument.".to_string())
        })?;
        MailBodyProperty::parse(&argument).ok_or_else(|| {
            MethodError::InvalidArguments(format!("Unknown property: '{}'.", argument))
        })
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
