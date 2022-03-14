pub mod changes;
pub mod get;
pub mod import;
pub mod mailbox;
pub mod parse;
pub mod query;
pub mod set;
pub mod thread;

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};

use jmap::json::JSONValue;
use mail_parser::{
    parsers::header::{parse_header_name, HeaderParserResult},
    HeaderOffset, MessagePartId, MessageStructure, RfcHeader,
};

use store::{
    bincode,
    blob::BlobIndex,
    serialize::{StoreDeserialize, StoreSerialize},
    FieldId,
};

pub const MESSAGE_RAW: BlobIndex = 0;
pub const MESSAGE_DATA: BlobIndex = 1;
pub const MESSAGE_PARTS: BlobIndex = 2;

pub type JMAPMailHeaders = HashMap<JMAPMailProperties, JSONValue>;
pub type JMAPMailMimeHeaders = HashMap<JMAPMailBodyProperties, JSONValue>;

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageData {
    pub properties: HashMap<JMAPMailProperties, JSONValue>,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum MimePartType {
    Text,
    Html,
    Other,
}

#[derive(Debug, Serialize, Deserialize)]
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
pub enum JMAPMailHeaderForm {
    Raw,
    Text,
    Addresses,
    GroupedAddresses,
    MessageIds,
    Date,
    URLs,
}

impl JMAPMailHeaderForm {
    pub fn parse(value: &str) -> Option<JMAPMailHeaderForm> {
        match value {
            "asText" => Some(JMAPMailHeaderForm::Text),
            "asAddresses" => Some(JMAPMailHeaderForm::Addresses),
            "asGroupedAddresses" => Some(JMAPMailHeaderForm::GroupedAddresses),
            "asMessageIds" => Some(JMAPMailHeaderForm::MessageIds),
            "asDate" => Some(JMAPMailHeaderForm::Date),
            "asURLs" => Some(JMAPMailHeaderForm::URLs),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct JMAPMailHeaderProperty {
    pub form: JMAPMailHeaderForm,
    pub header: HeaderName,
    pub all: bool,
}

impl JMAPMailHeaderProperty {
    pub fn new_rfc(header: RfcHeader, form: JMAPMailHeaderForm, all: bool) -> Self {
        JMAPMailHeaderProperty {
            form,
            header: HeaderName::Rfc(header),
            all,
        }
    }
    pub fn new_other(header: String, form: JMAPMailHeaderForm, all: bool) -> Self {
        JMAPMailHeaderProperty {
            form,
            header: HeaderName::Other(header),
            all,
        }
    }

    pub fn parse(value: &str) -> Option<JMAPMailHeaderProperty> {
        let mut all = false;
        let mut form = JMAPMailHeaderForm::Raw;
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
                    form = JMAPMailHeaderForm::parse(part)?;
                }
                _ => return None,
            }
        }
        Some(JMAPMailHeaderProperty {
            form,
            header: header?,
            all,
        })
    }
}

impl Display for JMAPMailHeaderProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "header:")?;
        match &self.header {
            HeaderName::Rfc(rfc) => rfc.fmt(f)?,
            HeaderName::Other(name) => name.fmt(f)?,
        }
        match self.form {
            JMAPMailHeaderForm::Raw => (),
            JMAPMailHeaderForm::Text => write!(f, ":asText")?,
            JMAPMailHeaderForm::Addresses => write!(f, ":asAddresses")?,
            JMAPMailHeaderForm::GroupedAddresses => write!(f, ":asGroupedAddresses")?,
            JMAPMailHeaderForm::MessageIds => write!(f, ":asMessageIds")?,
            JMAPMailHeaderForm::Date => write!(f, ":asDate")?,
            JMAPMailHeaderForm::URLs => write!(f, ":asURLs")?,
        }
        if self.all {
            write!(f, ":all")
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub enum JMAPMailProperties {
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
    Header(JMAPMailHeaderProperty),
}

impl Display for JMAPMailProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JMAPMailProperties::Id => write!(f, "id"),
            JMAPMailProperties::BlobId => write!(f, "blobId"),
            JMAPMailProperties::ThreadId => write!(f, "threadId"),
            JMAPMailProperties::MailboxIds => write!(f, "mailboxIds"),
            JMAPMailProperties::Keywords => write!(f, "keywords"),
            JMAPMailProperties::Size => write!(f, "size"),
            JMAPMailProperties::ReceivedAt => write!(f, "receivedAt"),
            JMAPMailProperties::MessageId => write!(f, "messageId"),
            JMAPMailProperties::InReplyTo => write!(f, "inReplyTo"),
            JMAPMailProperties::References => write!(f, "references"),
            JMAPMailProperties::Sender => write!(f, "sender"),
            JMAPMailProperties::From => write!(f, "from"),
            JMAPMailProperties::To => write!(f, "to"),
            JMAPMailProperties::Cc => write!(f, "cc"),
            JMAPMailProperties::Bcc => write!(f, "bcc"),
            JMAPMailProperties::ReplyTo => write!(f, "replyTo"),
            JMAPMailProperties::Subject => write!(f, "subject"),
            JMAPMailProperties::SentAt => write!(f, "sentAt"),
            JMAPMailProperties::HasAttachment => write!(f, "hasAttachment"),
            JMAPMailProperties::Preview => write!(f, "preview"),
            JMAPMailProperties::BodyValues => write!(f, "bodyValues"),
            JMAPMailProperties::TextBody => write!(f, "textBody"),
            JMAPMailProperties::HtmlBody => write!(f, "htmlBody"),
            JMAPMailProperties::Attachments => write!(f, "attachments"),
            JMAPMailProperties::BodyStructure => write!(f, "bodyStructure"),
            JMAPMailProperties::Header(header) => header.fmt(f),
        }
    }
}

impl JMAPMailProperties {
    pub fn parse(value: &str) -> Option<JMAPMailProperties> {
        match value {
            "id" => Some(JMAPMailProperties::Id),
            "blobId" => Some(JMAPMailProperties::BlobId),
            "threadId" => Some(JMAPMailProperties::ThreadId),
            "mailboxIds" => Some(JMAPMailProperties::MailboxIds),
            "keywords" => Some(JMAPMailProperties::Keywords),
            "size" => Some(JMAPMailProperties::Size),
            "receivedAt" => Some(JMAPMailProperties::ReceivedAt),
            "messageId" => Some(JMAPMailProperties::MessageId),
            "inReplyTo" => Some(JMAPMailProperties::InReplyTo),
            "references" => Some(JMAPMailProperties::References),
            "sender" => Some(JMAPMailProperties::Sender),
            "from" => Some(JMAPMailProperties::From),
            "to" => Some(JMAPMailProperties::To),
            "cc" => Some(JMAPMailProperties::Cc),
            "bcc" => Some(JMAPMailProperties::Bcc),
            "replyTo" => Some(JMAPMailProperties::ReplyTo),
            "subject" => Some(JMAPMailProperties::Subject),
            "sentAt" => Some(JMAPMailProperties::SentAt),
            "hasAttachment" => Some(JMAPMailProperties::HasAttachment),
            "preview" => Some(JMAPMailProperties::Preview),
            "bodyValues" => Some(JMAPMailProperties::BodyValues),
            "textBody" => Some(JMAPMailProperties::TextBody),
            "htmlBody" => Some(JMAPMailProperties::HtmlBody),
            "attachments" => Some(JMAPMailProperties::Attachments),
            "bodyStructure" => Some(JMAPMailProperties::BodyStructure),
            _ if value.starts_with("header:") => Some(JMAPMailProperties::Header(
                JMAPMailHeaderProperty::parse(value)?,
            )),
            _ => None,
        }
    }

    pub fn as_rfc_header(&self) -> RfcHeader {
        match self {
            JMAPMailProperties::MessageId => RfcHeader::MessageId,
            JMAPMailProperties::InReplyTo => RfcHeader::InReplyTo,
            JMAPMailProperties::References => RfcHeader::References,
            JMAPMailProperties::Sender => RfcHeader::Sender,
            JMAPMailProperties::From => RfcHeader::From,
            JMAPMailProperties::To => RfcHeader::To,
            JMAPMailProperties::Cc => RfcHeader::Cc,
            JMAPMailProperties::Bcc => RfcHeader::Bcc,
            JMAPMailProperties::ReplyTo => RfcHeader::ReplyTo,
            JMAPMailProperties::Subject => RfcHeader::Subject,
            JMAPMailProperties::SentAt => RfcHeader::Date,
            JMAPMailProperties::Header(JMAPMailHeaderProperty {
                header: HeaderName::Rfc(rfc),
                ..
            }) => *rfc,
            _ => unreachable!(),
        }
    }
}

impl Default for JMAPMailProperties {
    fn default() -> Self {
        JMAPMailProperties::Id
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum JMAPMailBodyProperties {
    PartId,
    BlobId,
    Size,
    Name,
    Type,
    Charset,
    Header(JMAPMailHeaderProperty),
    Headers,
    Disposition,
    Cid,
    Language,
    Location,
    Subparts,
}

impl Display for JMAPMailBodyProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JMAPMailBodyProperties::PartId => write!(f, "partId"),
            JMAPMailBodyProperties::BlobId => write!(f, "blobId"),
            JMAPMailBodyProperties::Size => write!(f, "size"),
            JMAPMailBodyProperties::Name => write!(f, "name"),
            JMAPMailBodyProperties::Type => write!(f, "type"),
            JMAPMailBodyProperties::Charset => write!(f, "charset"),
            JMAPMailBodyProperties::Header(header) => header.fmt(f),
            JMAPMailBodyProperties::Headers => write!(f, "headers"),
            JMAPMailBodyProperties::Disposition => write!(f, "disposition"),
            JMAPMailBodyProperties::Cid => write!(f, "cid"),
            JMAPMailBodyProperties::Language => write!(f, "language"),
            JMAPMailBodyProperties::Location => write!(f, "location"),
            JMAPMailBodyProperties::Subparts => write!(f, "subParts"),
        }
    }
}
