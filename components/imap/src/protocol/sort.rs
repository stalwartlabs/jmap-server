use super::search::Filter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arguments {
    pub filter: Filter,
    pub sort: Vec<Comparator>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sort {
    Arrival,
    Cc,
    Date,
    From,
    DisplayFrom,
    Size,
    Subject,
    To,
    DisplayTo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Comparator {
    pub sort: Sort,
    pub ascending: bool,
}
