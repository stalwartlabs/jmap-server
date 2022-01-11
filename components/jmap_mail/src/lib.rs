pub mod changes;
pub mod get;
pub mod import;
pub mod parse;
pub mod query;
pub mod set;

use std::{borrow::Cow, collections::HashMap};

use changes::JMAPMailLocalStoreChanges;
use get::JMAPMailLocalStoreGet;
use import::{JMAPMailImportItem, JMAPMailLocalStoreImport};
use jmap_store::{
    changes::{JMAPLocalChanges, JMAPState},
    json::JSONValue,
    JMAPChangesResponse, JMAPGet, JMAPGetResponse, JMAPId, JMAPQuery, JMAPQueryChanges,
    JMAPQueryChangesResponse, JMAPQueryResponse, JMAPSet, JMAPSetResponse,
};
use mail_parser::{HeaderName, MessagePartId, RawHeaders};
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

pub type JMAPMailHeaders<'x> = HashMap<HeaderName, JSONValue<'x, JMAPMailProperties<'x>>>;
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
    pub parts_headers: Vec<JMAPMailMimeHeaders<'x>>,
    pub html_body: Vec<MessagePartId>,
    pub text_body: Vec<MessagePartId>,
    pub attachments: Vec<MessagePartId>,
    pub size: usize,
    pub received_at: i64,
    pub has_attachments: bool,
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
pub struct JMAPMailHeaderProperty<T> {
    pub form: JMAPMailHeaderForm,
    pub header: T,
    pub all: bool,
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
    RfcHeader(JMAPMailHeaderProperty<HeaderName>),
    OtherHeader(JMAPMailHeaderProperty<Cow<'x, str>>),

    // Sub-properties
    Name,
    Email,
    Addresses,
    PartId,
    Type,
    Charset,
    Headers,
    Disposition,
    Cid,
    Language,
    Location,
    Subparts,
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
            JMAPMailProperties::RfcHeader(header) => {
                JMAPMailProperties::RfcHeader(JMAPMailHeaderProperty {
                    form: header.form.clone(),
                    header: header.header,
                    all: header.all,
                })
            }
            JMAPMailProperties::OtherHeader(header) => {
                JMAPMailProperties::OtherHeader(JMAPMailHeaderProperty {
                    form: header.form.clone(),
                    header: header.header.clone().into_owned().into(),
                    all: header.all,
                })
            }
            JMAPMailProperties::Name => JMAPMailProperties::Name,
            JMAPMailProperties::Email => JMAPMailProperties::Email,
            JMAPMailProperties::Addresses => JMAPMailProperties::Addresses,
            JMAPMailProperties::PartId => JMAPMailProperties::PartId,
            JMAPMailProperties::Type => JMAPMailProperties::Type,
            JMAPMailProperties::Charset => JMAPMailProperties::Charset,
            JMAPMailProperties::Headers => JMAPMailProperties::Headers,
            JMAPMailProperties::Disposition => JMAPMailProperties::Disposition,
            JMAPMailProperties::Cid => JMAPMailProperties::Cid,
            JMAPMailProperties::Language => JMAPMailProperties::Language,
            JMAPMailProperties::Location => JMAPMailProperties::Location,
            JMAPMailProperties::Subparts => JMAPMailProperties::Subparts,
        }
    }
}

impl<'x> Default for JMAPMailProperties<'x> {
    fn default() -> Self {
        JMAPMailProperties::Id
    }
}

#[derive(Debug)]
pub enum JMAPMailBodyProperties {
    PartId,
    BlobId,
    Size,
    Name,
    Type,
    Charset,
    Headers,
    Disposition,
    Cid,
    Language,
    Location,
    Subparts,
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
pub struct JMAPMailStoreGetArguments {
    pub body_properties: Vec<JMAPMailBodyProperties>,
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
    ) -> jmap_store::Result<JMAPGetResponse<'x, JMAPMailProperties<'x>>>;
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
