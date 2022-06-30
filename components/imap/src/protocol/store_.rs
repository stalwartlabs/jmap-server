use jmap_mail::mail::schema::Keyword;

use super::Sequence;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arguments {
    pub sequence_set: Vec<Sequence>,
    pub operation: Operation,
    pub keywords: Vec<Keyword>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    Set,
    SetSilent,
    Add,
    AddSilent,
    Clear,
    ClearSilent,
}
