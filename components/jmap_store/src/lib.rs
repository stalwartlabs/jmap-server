use std::collections::HashSet;

use changes::JMAPState;
use store::{AccountId, ChangeLogId};

pub mod changes;
pub mod local_store;

pub const JMAP_MAIL: u8 = 0;
pub const JMAP_MAILBOX: u8 = 1;
pub const JMAP_THREAD: u8 = 2;

#[derive(Debug, Clone)]
pub struct JMAPQuery<T, U, V> {
    pub account_id: AccountId,
    pub filter: JMAPFilter<T>,
    pub sort: Vec<JMAPComparator<U>>,
    pub position: i32,
    pub anchor: Option<V>,
    pub anchor_offset: i32,
    pub limit: usize,
    pub calculate_total: bool,
}

#[derive(Debug, Clone)]
pub struct JMAPQueryChanges<T, U, V> {
    pub account_id: AccountId,
    pub filter: JMAPFilter<T>,
    pub sort: Vec<JMAPComparator<U>>,
    pub since_query_state: JMAPState,
    pub max_changes: usize,
    pub up_to_id: Option<V>,
    pub calculate_total: bool,
}

#[derive(Debug)]
pub struct JMAPQueryResponse<T> {
    pub query_state: JMAPState,
    pub is_immutable: bool,
    pub total: usize,
    pub ids: Vec<T>,
}

#[derive(Debug)]
pub struct JMAPQueryChangesResponseItem<T> {
    pub id: T,
    pub index: usize,
}

#[derive(Debug)]
pub struct JMAPQueryChangesResponse<T> {
    pub old_query_state: JMAPState,
    pub new_query_state: JMAPState,
    pub total: usize,
    pub removed: Vec<T>,
    pub added: Vec<JMAPQueryChangesResponseItem<T>>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub enum JMAPLogicalOperator {
    And,
    Or,
    Not,
}

#[derive(Debug, Clone)]
pub struct JMAPFilterOperator<T> {
    pub operator: JMAPLogicalOperator,
    pub conditions: Vec<JMAPFilter<T>>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug)]
pub struct JMAPChangesResponse {
    pub old_state: JMAPState,
    pub new_state: JMAPState,
    pub has_more_changes: bool,
    pub total_changes: usize,
    pub created: HashSet<ChangeLogId>,
    pub updated: HashSet<ChangeLogId>,
    pub destroyed: HashSet<ChangeLogId>,
}

pub trait JMAPIdSerialize {
    fn from_jmap_id(id: &str) -> Option<Self>
    where
        Self: Sized;
    fn to_jmap_id(&self) -> String;
}
