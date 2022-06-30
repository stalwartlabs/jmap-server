use jmap_mail::mail::schema::Keyword;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arguments {
    pub mailbox_name: String,
    pub message: Vec<u8>,
    pub flags: Vec<Keyword>,
    pub received_at: Option<i64>,
}
