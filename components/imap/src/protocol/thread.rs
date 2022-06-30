use super::search::Filter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arguments {
    pub filter: Filter,
    pub algorithm: Algorithm,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Algorithm {
    OrderedSubject,
    References,
}
