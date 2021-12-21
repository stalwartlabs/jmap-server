use mail_parser::MessagePartId;
use serde::{Deserialize, Serialize};
use store::{mutex_map::MutexMap, FieldNumber, Store};

pub mod ingest;
pub mod parse;

pub const MAIL_CID: u8 = 0;
pub const THREAD_CID: u8 = 1;

pub const MESSAGE_RAW: FieldNumber = 0;
pub const MESSAGE_HEADERS: FieldNumber = 1;
pub const MESSAGE_HEADERS_OTHER: FieldNumber = 2;
pub const MESSAGE_HEADERS_OFFSETS: FieldNumber = 3;
pub const MESSAGE_HEADERS_NESTED: FieldNumber = 4;
pub const MESSAGE_HEADERS_PARTS: FieldNumber = 5;
pub const MESSAGE_HEADERS_STRUCTURE: FieldNumber = 6;

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
}

impl From<MessageField> for u8 {
    fn from(field: MessageField) -> Self {
        field as u8
    }
}

pub struct MessageStore<'x, T> {
    pub id_lock: MutexMap,
    pub db: &'x T,
}
impl<'x, T> MessageStore<'x, T>
where
    T: Store<'x>,
{
    pub fn new(db: &T) -> MessageStore<T> {
        MessageStore {
            id_lock: MutexMap::with_capacity(1024),
            db,
        }
    }
}
