pub mod parse;

#[repr(u8)]
pub enum MailField {
    Header = 0,
    Body = 1,
    Attachment = 2,
    ReceivedAt = 3,
    Size = 4,
    Keyword = 5,
    Thread = 6,
    Mailbox = 7,

    HeaderField = 8,
}
