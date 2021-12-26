use store::AccountId;

pub mod local_store;
pub mod mutex_map;

pub const JMAP_MAIL: u8 = 0;
pub const JMAP_MAILBOX: u8 = 1;

pub struct JMAPQuery<T, U> {
    pub account_id: AccountId,
    pub filter: JMAPFilter<T>,
    pub sort: Vec<JMAPComparator<U>>,
    pub position: usize,
    pub anchor: usize,
    pub anchor_offset: usize,
    pub limit: usize,
    pub calculate_total: bool,
}

pub struct JMAPComparator<T> {
    pub property: T,
    pub is_ascending: bool,
    pub collation: Option<String>,
}

pub enum JMAPLogicalOperator {
    And,
    Or,
    Not,
}

pub struct JMAPFilterOperator<T> {
    pub operator: JMAPLogicalOperator,
    pub conditions: Vec<JMAPFilter<T>>,
}
pub enum JMAPFilter<T> {
    Condition(T),
    Operator(JMAPFilterOperator<T>),
    None,
}
