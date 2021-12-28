pub mod get;
pub mod import;
pub mod parse;
pub mod query;

use import::JMAPMailImportItem;
use jmap_store::{JMAPQuery, JMAPQueryResponse};
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

#[derive(Debug)]
pub struct JMAPMailId {
    pub thread_id: ThreadId,
    pub doc_id: DocumentId,
}

impl JMAPMailId {
    pub fn new(thread_id: ThreadId, doc_id: DocumentId) -> Self {
        Self { thread_id, doc_id }
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
        query: JMAPQuery<JMAPMailFilterCondition<'x>, JMAPMailComparator<'x>>,
        collapse_threads: bool,
    ) -> store::Result<JMAPQueryResponse<JMAPMailId>>;
}

pub trait JMAPMailStoreGet<'x> {
    fn get_headers_rfc(
        &'x self,
        account: AccountId,
        document: DocumentId,
    ) -> store::Result<RfcHeaders>;
}

pub trait JMAPMailStore<'x>:
    JMAPMailStoreImport<'x> + JMAPMailStoreQuery<'x> + JMAPMailStoreGet<'x>
{
}

pub trait JMAPMailStore<'x>: JMAPMailStoreImport<'x> + JMAPMailStoreQuery<'x> {}
