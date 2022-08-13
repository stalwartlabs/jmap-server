pub mod email_submission;
pub mod identity;
pub mod mail;
pub mod mailbox;
pub mod thread;
pub mod vacation_response;

pub use mail_parser;
pub use mail_send;

pub const INBOX_ID: store::DocumentId = 0;
pub const TRASH_ID: store::DocumentId = 1;
