pub mod changes;
pub mod get;
pub mod import;
pub mod parse;
pub mod query;

use import::JMAPMailImportItem;
use jmap_store::{
    changes::JMAPState, JMAPChangesResponse, JMAPQuery, JMAPQueryChanges, JMAPQueryChangesResponse,
    JMAPQueryResponse,
};
use mail_parser::{MessagePartId, RfcHeaders};
use query::{JMAPMailComparator, JMAPMailFilterCondition};
use serde::{Deserialize, Serialize};
use store::{AccountId, DocumentId, DocumentSet, FieldNumber, ThreadId};

pub const MESSAGE_RAW: FieldNumber = 0;
pub const MESSAGE_HEADERS: FieldNumber = 1;
pub const MESSAGE_HEADERS_OTHER: FieldNumber = 2;
pub const MESSAGE_HEADERS_OFFSETS: FieldNumber = 3;
pub const MESSAGE_HEADERS_NESTED: FieldNumber = 4;
pub const MESSAGE_HEADERS_PARTS: FieldNumber = 5;
pub const MESSAGE_HEADERS_STRUCTURE: FieldNumber = 6;

pub type JMAPMailId = u64;

pub trait JMAPMailIdImpl {
    fn new(thread_id: ThreadId, doc_id: DocumentId) -> Self;
    fn get_document_id(&self) -> DocumentId;
    fn get_thread_id(&self) -> ThreadId;
}

impl JMAPMailIdImpl for JMAPMailId {
    fn new(thread_id: ThreadId, doc_id: DocumentId) -> JMAPMailId {
        (thread_id as JMAPMailId) << 32 | doc_id as JMAPMailId
    }

    fn get_document_id(&self) -> DocumentId {
        (self & 0xFFFFFFFF) as DocumentId
    }

    fn get_thread_id(&self) -> ThreadId {
        (self >> 32) as ThreadId
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageParts {
    pub html_body: Vec<MessagePartId>,
    pub text_body: Vec<MessagePartId>,
    pub attachments: Vec<MessagePartId>,
    pub offset_body: usize,
    pub size: usize,
    pub received_at: i64,
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

pub trait JMAPMailStoreImport<'x> {
    fn mail_import_single(
        &'x self,
        account: AccountId,
        message: JMAPMailImportItem<'x>,
    ) -> store::Result<JMAPMailId>;
}

pub trait JMAPMailStoreQuery<'x> {
    type Set: DocumentSet;

    fn mail_query(
        &'x self,
        query: JMAPQuery<JMAPMailFilterCondition<'x>, JMAPMailComparator<'x>, JMAPMailId>,
        collapse_threads: bool,
    ) -> store::Result<JMAPQueryResponse<JMAPMailId>>;
}

pub trait JMAPMailStoreChanges<'x> {
    type Set: DocumentSet;

    fn mail_changes(
        &'x self,
        account: AccountId,
        since_state: JMAPState,
        max_changes: usize,
    ) -> store::Result<JMAPChangesResponse>;

    fn mail_query_changes(
        &'x self,
        query: JMAPQueryChanges<JMAPMailFilterCondition<'x>, JMAPMailComparator<'x>, JMAPMailId>,
        collapse_threads: bool,
    ) -> store::Result<JMAPQueryChangesResponse<JMAPMailId>>;
}

pub trait JMAPMailStoreGet<'x> {
    fn get_headers_rfc(
        &'x self,
        account: AccountId,
        document: DocumentId,
    ) -> store::Result<RfcHeaders>;
}

pub trait JMAPMailStore<'x>:
    JMAPMailStoreImport<'x> + JMAPMailStoreQuery<'x> + JMAPMailStoreGet<'x> + JMAPMailStoreChanges<'x>
{
}
