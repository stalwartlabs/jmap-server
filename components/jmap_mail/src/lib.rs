pub mod changes;
pub mod get;
pub mod import;
pub mod parse;
pub mod query;
pub mod set;

use std::{borrow::Cow, collections::HashMap, fmt::Display};

use changes::JMAPMailLocalStoreChanges;
use get::JMAPMailLocalStoreGet;
use import::{JMAPMailImportItem, JMAPMailLocalStoreImport};
use jmap_store::{
    changes::{JMAPLocalChanges, JMAPState},
    json::JSONValue,
    JMAPChangesResponse, JMAPGet, JMAPGetResponse, JMAPId, JMAPQuery, JMAPQueryChanges,
    JMAPQueryChangesResponse, JMAPQueryResponse, JMAPSet, JMAPSetResponse,
};
use mail_parser::{HeaderName, MessagePartId, RawHeaders, RfcHeader};
use query::{JMAPMailComparator, JMAPMailFilterCondition, JMAPMailLocalStoreQuery};
use serde::{Deserialize, Serialize};
use set::JMAPMailLocalStoreSet;
use store::{AccountId, BlobIndex, DocumentId, ThreadId};

pub const MESSAGE_RAW: BlobIndex = 0;
pub const MESSAGE_HEADERS: BlobIndex = 1;
pub const MESSAGE_HEADERS_RAW: BlobIndex = 2;
pub const MESSAGE_BODY: BlobIndex = 3;
pub const MESSAGE_BODY_STRUCTURE: BlobIndex = 4;
pub const MESSAGE_PARTS: BlobIndex = 5;

pub type JMAPMailHeaders<'x> = HashMap<RfcHeader, JSONValue<'x, JMAPMailProperties<'x>>>;
pub type JMAPMailMimeHeaders<'x> =
    HashMap<JMAPMailProperties<'x>, JSONValue<'x, JMAPMailProperties<'x>>>;

pub trait JMAPMailIdImpl {
    fn from_email(thread_id: ThreadId, doc_id: DocumentId) -> Self;
    fn get_document_id(&self) -> DocumentId;
    fn get_thread_id(&self) -> ThreadId;
}

impl JMAPMailIdImpl for JMAPId {
    fn from_email(thread_id: ThreadId, doc_id: DocumentId) -> JMAPId {
        (thread_id as JMAPId) << 32 | doc_id as JMAPId
    }

    fn get_document_id(&self) -> DocumentId {
        (self & 0xFFFFFFFF) as DocumentId
    }

    fn get_thread_id(&self) -> ThreadId {
        (self >> 32) as ThreadId
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageBody<'x> {
    pub mime_parts: Vec<MimePart<'x>>,
    pub html_body: Vec<MessagePartId>,
    pub text_body: Vec<MessagePartId>,
    pub attachments: Vec<MessagePartId>,
    pub size: usize,
    pub received_at: i64,
    pub has_attachments: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum MimePartType {
    Text,
    Html,
    Other,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MimePart<'x> {
    pub headers: JMAPMailMimeHeaders<'x>,
    pub is_encoding_problem: bool,
    pub mime_type: MimePartType,
}

impl<'x> MimePart<'x> {
    pub fn new_html(headers: JMAPMailMimeHeaders<'x>, is_encoding_problem: bool) -> Self {
        MimePart {
            headers,
            is_encoding_problem,
            mime_type: MimePartType::Html,
        }
    }

    pub fn new_text(headers: JMAPMailMimeHeaders<'x>, is_encoding_problem: bool) -> Self {
        MimePart {
            headers,
            is_encoding_problem,
            mime_type: MimePartType::Text,
        }
    }

    pub fn new_other(headers: JMAPMailMimeHeaders<'x>, is_encoding_problem: bool) -> Self {
        MimePart {
            headers,
            is_encoding_problem,
            mime_type: MimePartType::Other,
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct MessageRawHeaders<'x> {
    pub size: usize,
    pub headers: RawHeaders<'x>,
    pub parts_headers: Vec<RawHeaders<'x>>,
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

impl From<MessageField> for u8 {
    fn from(field: MessageField) -> Self {
        field as u8
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

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct JMAPMailHeaderProperty<'x> {
    pub form: JMAPMailHeaderForm,
    pub header: HeaderName<'x>,
    pub all: bool,
}

impl<'x> Display for JMAPMailHeaderProperty<'x> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "header:")?;
        match &self.header {
            HeaderName::Rfc(rfc) => rfc.fmt(f)?,
            HeaderName::Other(name) => name.fmt(f)?,
        }
        match self.form {
            JMAPMailHeaderForm::Raw => (),
            JMAPMailHeaderForm::Text => write!(f, "asText")?,
            JMAPMailHeaderForm::Addresses => write!(f, "asAddresses")?,
            JMAPMailHeaderForm::GroupedAddresses => write!(f, "asGroupedAddresses")?,
            JMAPMailHeaderForm::MessageIds => write!(f, "asMessageIds")?,
            JMAPMailHeaderForm::Date => write!(f, "asDate")?,
            JMAPMailHeaderForm::URLs => write!(f, "asURLs")?,
        }
        if self.all {
            write!(f, ":all")
        } else {
            Ok(())
        }
    }
}

impl<'x> JMAPMailHeaderProperty<'x> {
    pub fn into_owned<'y>(&self) -> JMAPMailHeaderProperty<'y> {
        JMAPMailHeaderProperty {
            form: self.form.clone(),
            header: self.header.into_owned(),
            all: self.all,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub enum JMAPMailProperties<'x> {
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
    Header(JMAPMailHeaderProperty<'x>),

    // Sub-properties
    Name,
    Email,
    Addresses,
    Type,
    Charset,
    Headers,
    Disposition,
    Cid,
    Language,
    Location,
}

impl<'x> Display for JMAPMailProperties<'x> {
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
            JMAPMailProperties::Name => write!(f, "name"),
            JMAPMailProperties::Email => write!(f, "email"),
            JMAPMailProperties::Addresses => write!(f, "addresses"),
            JMAPMailProperties::Type => write!(f, "type"),
            JMAPMailProperties::Charset => write!(f, "charset"),
            JMAPMailProperties::Headers => write!(f, "headers"),
            JMAPMailProperties::Disposition => write!(f, "disposition"),
            JMAPMailProperties::Cid => write!(f, "cid"),
            JMAPMailProperties::Language => write!(f, "language"),
            JMAPMailProperties::Location => write!(f, "location"),
        }
    }
}

impl<'x, 'y> From<JMAPMailProperties<'x>> for Cow<'y, str> {
    fn from(prop: JMAPMailProperties<'x>) -> Self {
        prop.to_string().into()
    }
}

impl<'x> JMAPMailProperties<'x> {
    pub fn into_owned<'y>(&self) -> JMAPMailProperties<'y> {
        match self {
            JMAPMailProperties::Id => JMAPMailProperties::Id,
            JMAPMailProperties::BlobId => JMAPMailProperties::BlobId,
            JMAPMailProperties::ThreadId => JMAPMailProperties::ThreadId,
            JMAPMailProperties::MailboxIds => JMAPMailProperties::MailboxIds,
            JMAPMailProperties::Keywords => JMAPMailProperties::Keywords,
            JMAPMailProperties::Size => JMAPMailProperties::Size,
            JMAPMailProperties::ReceivedAt => JMAPMailProperties::ReceivedAt,
            JMAPMailProperties::MessageId => JMAPMailProperties::MessageId,
            JMAPMailProperties::InReplyTo => JMAPMailProperties::InReplyTo,
            JMAPMailProperties::References => JMAPMailProperties::References,
            JMAPMailProperties::Sender => JMAPMailProperties::Sender,
            JMAPMailProperties::From => JMAPMailProperties::From,
            JMAPMailProperties::To => JMAPMailProperties::To,
            JMAPMailProperties::Cc => JMAPMailProperties::Cc,
            JMAPMailProperties::Bcc => JMAPMailProperties::Bcc,
            JMAPMailProperties::ReplyTo => JMAPMailProperties::ReplyTo,
            JMAPMailProperties::Subject => JMAPMailProperties::Subject,
            JMAPMailProperties::SentAt => JMAPMailProperties::SentAt,
            JMAPMailProperties::HasAttachment => JMAPMailProperties::HasAttachment,
            JMAPMailProperties::Preview => JMAPMailProperties::Preview,
            JMAPMailProperties::BodyValues => JMAPMailProperties::BodyValues,
            JMAPMailProperties::TextBody => JMAPMailProperties::TextBody,
            JMAPMailProperties::HtmlBody => JMAPMailProperties::HtmlBody,
            JMAPMailProperties::Attachments => JMAPMailProperties::Attachments,
            JMAPMailProperties::BodyStructure => JMAPMailProperties::BodyStructure,
            JMAPMailProperties::Header(header) => JMAPMailProperties::Header(header.into_owned()),
            JMAPMailProperties::Name => JMAPMailProperties::Name,
            JMAPMailProperties::Email => JMAPMailProperties::Email,
            JMAPMailProperties::Addresses => JMAPMailProperties::Addresses,
            JMAPMailProperties::Type => JMAPMailProperties::Type,
            JMAPMailProperties::Charset => JMAPMailProperties::Charset,
            JMAPMailProperties::Headers => JMAPMailProperties::Headers,
            JMAPMailProperties::Disposition => JMAPMailProperties::Disposition,
            JMAPMailProperties::Cid => JMAPMailProperties::Cid,
            JMAPMailProperties::Language => JMAPMailProperties::Language,
            JMAPMailProperties::Location => JMAPMailProperties::Location,
        }
    }
}

impl<'x> Default for JMAPMailProperties<'x> {
    fn default() -> Self {
        JMAPMailProperties::Id
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub enum JMAPMailBodyProperties<'x> {
    PartId,
    BlobId,
    Size,
    Name,
    Type,
    Charset,
    Header(JMAPMailHeaderProperty<'x>),
    Headers,
    Disposition,
    Cid,
    Language,
    Location,
    Subparts,
}

impl<'x> Display for JMAPMailBodyProperties<'x> {
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
            JMAPMailBodyProperties::Subparts => write!(f, "subparts"),
        }
    }
}

impl<'x, 'y> From<&JMAPMailBodyProperties<'x>> for JMAPMailProperties<'y> {
    fn from(body_property: &JMAPMailBodyProperties<'x>) -> Self {
        match body_property {
            JMAPMailBodyProperties::Size => JMAPMailProperties::Size,
            JMAPMailBodyProperties::Name => JMAPMailProperties::Name,
            JMAPMailBodyProperties::Type => JMAPMailProperties::Type,
            JMAPMailBodyProperties::Charset => JMAPMailProperties::Charset,
            JMAPMailBodyProperties::Header(header) => {
                JMAPMailProperties::Header(header.into_owned())
            }
            JMAPMailBodyProperties::Headers => JMAPMailProperties::Headers,
            JMAPMailBodyProperties::Disposition => JMAPMailProperties::Disposition,
            JMAPMailBodyProperties::Cid => JMAPMailProperties::Cid,
            JMAPMailBodyProperties::Language => JMAPMailProperties::Language,
            JMAPMailBodyProperties::Location => JMAPMailProperties::Location,
            _ => JMAPMailProperties::BlobId,
        }
    }
}

pub trait JMAPMailStoreImport<'x> {
    fn mail_import_single(
        &'x self,
        account: AccountId,
        message: JMAPMailImportItem<'x>,
    ) -> jmap_store::Result<JMAPId>;
}

pub trait JMAPMailStoreSet<'x> {
    fn mail_set(
        &self,
        request: JMAPSet<'x, JMAPMailProperties<'x>>,
    ) -> jmap_store::Result<JMAPSetResponse<'x, JMAPMailProperties<'x>>>;
}

pub trait JMAPMailStoreQuery<'x> {
    fn mail_query(
        &'x self,
        query: JMAPQuery<JMAPMailFilterCondition<'x>, JMAPMailComparator<'x>>,
        collapse_threads: bool,
    ) -> jmap_store::Result<JMAPQueryResponse>;
}

pub trait JMAPMailStoreChanges<'x>: JMAPLocalChanges<'x> {
    fn mail_changes(
        &'x self,
        account: AccountId,
        since_state: JMAPState,
        max_changes: usize,
    ) -> jmap_store::Result<JMAPChangesResponse>;

    fn mail_query_changes(
        &'x self,
        query: JMAPQueryChanges<JMAPMailFilterCondition<'x>, JMAPMailComparator<'x>>,
        collapse_threads: bool,
    ) -> jmap_store::Result<JMAPQueryChangesResponse>;
}

#[derive(Debug, Default)]
pub struct JMAPMailStoreGetArguments<'x> {
    pub body_properties: Vec<JMAPMailBodyProperties<'x>>,
    pub fetch_text_body_values: bool,
    pub fetch_html_body_values: bool,
    pub fetch_all_body_values: bool,
    pub max_body_value_bytes: usize,
}

pub trait JMAPMailStoreGet<'x> {
    fn mail_get(
        &self,
        request: JMAPGet<JMAPMailProperties<'x>>,
        arguments: JMAPMailStoreGetArguments,
    ) -> jmap_store::Result<JMAPGetResponse<'x>>;
}

pub trait JMAPMailStore<'x>:
    JMAPMailStoreImport<'x>
    + JMAPMailStoreSet<'x>
    + JMAPMailStoreQuery<'x>
    + JMAPMailStoreGet<'x>
    + JMAPMailStoreChanges<'x>
{
}

pub trait JMAPMailLocalStore<'x>:
    JMAPMailLocalStoreGet<'x>
    + JMAPMailLocalStoreQuery<'x>
    + JMAPMailLocalStoreImport<'x>
    + JMAPMailLocalStoreSet<'x>
    + JMAPMailLocalStoreChanges<'x>
{
}
