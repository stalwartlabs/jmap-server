use store::AccountId;

pub mod local_store;

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

#[derive(Debug)]
pub struct JMAPQueryResponse<T> {
    pub query_state: String,
    pub total: usize,
    pub ids: Vec<T>,
}

pub struct JMAPComparator<T> {
    pub property: T,
    pub is_ascending: bool,
    pub collation: Option<String>,
}

impl<T> JMAPComparator<T> {
    pub fn ascending(property: T) -> Self {
        Self {
            property,
            is_ascending: true,
            collation: None,
        }
    }

    pub fn descending(property: T) -> Self {
        Self {
            property,
            is_ascending: false,
            collation: None,
        }
    }
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

impl<T> JMAPFilter<T> {
    pub fn condition(cond: T) -> Self {
        JMAPFilter::Condition(cond)
    }

    pub fn and(conditions: Vec<JMAPFilter<T>>) -> Self {
        JMAPFilter::Operator(JMAPFilterOperator {
            operator: JMAPLogicalOperator::And,
            conditions,
        })
    }

    pub fn or(conditions: Vec<JMAPFilter<T>>) -> Self {
        JMAPFilter::Operator(JMAPFilterOperator {
            operator: JMAPLogicalOperator::Or,
            conditions,
        })
    }

    pub fn not(conditions: Vec<JMAPFilter<T>>) -> Self {
        JMAPFilter::Operator(JMAPFilterOperator {
            operator: JMAPLogicalOperator::Not,
            conditions,
        })
    }
}