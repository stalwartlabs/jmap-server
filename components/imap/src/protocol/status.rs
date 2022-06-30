#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arguments {
    pub name: String,
    pub items: Vec<Status>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Messages,
    UidNext,
    UidValidity,
    Unseen,
    Deleted,
    Size,
}
