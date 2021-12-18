use mail_parser::MessagePartId;
use serde::{Deserialize, Serialize};
use store::FieldNumber;

pub mod ingest;
pub mod parse;

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
}

impl From<MessageField> for u8 {
    fn from(field: MessageField) -> Self {
        field as u8
    }
}

pub type Result<T> = std::result::Result<T, MessageStoreError>;

pub enum MessageStoreError {
    ParseError,
    SerializeError(String),
}

pub const COLLECTION_ID: u8 = 0;

pub const MESSAGE_RAW: FieldNumber = 0;
pub const MESSAGE_HEADERS: FieldNumber = 1;
pub const MESSAGE_HEADERS_OTHER: FieldNumber = 2;
pub const MESSAGE_HEADERS_OFFSETS: FieldNumber = 3;
pub const MESSAGE_HEADERS_NESTED: FieldNumber = 4;
pub const MESSAGE_HEADERS_PARTS: FieldNumber = 5;
pub const MESSAGE_HEADERS_STRUCTURE: FieldNumber = 6;
