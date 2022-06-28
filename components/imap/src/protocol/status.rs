#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Messages,
    UidNext,
    UidValidity,
    Unseen,
    Deleted,
    Size,
}
