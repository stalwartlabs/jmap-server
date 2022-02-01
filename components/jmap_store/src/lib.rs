pub mod blob;
pub mod changes;
pub mod id;
pub mod json;

use std::collections::{HashMap, HashSet};

use changes::JMAPState;
use json::JSONValue;
use store::{AccountId, ChangeLogId, StoreError};

pub const JMAP_MAIL: u8 = 0;
pub const JMAP_MAILBOX: u8 = 1;
pub const JMAP_THREAD: u8 = 2;
pub const JMAP_BLOB: u8 = 3;

pub type JMAPId = u64;

#[derive(Debug)]
pub enum JMAPError {
    InvalidArguments,
    RequestTooLarge,
    StateMismatch,
    AnchorNotFound,
    UnsupportedFilter,
    UnsupportedSort,
    InternalError(StoreError),
}

impl From<StoreError> for JMAPError {
    fn from(e: StoreError) -> Self {
        JMAPError::InternalError(e)
    }
}

pub type Result<T> = std::result::Result<T, JMAPError>;

#[derive(Debug, Clone)]
pub struct JMAPQuery<T, U> {
    pub account_id: AccountId,
    pub filter: JMAPFilter<T>,
    pub sort: Vec<JMAPComparator<U>>,
    pub position: i32,
    pub anchor: Option<JMAPId>,
    pub anchor_offset: i32,
    pub limit: usize,
    pub calculate_total: bool,
}

#[derive(Debug, Clone)]
pub struct JMAPQueryChanges<T, U> {
    pub account_id: AccountId,
    pub filter: JMAPFilter<T>,
    pub sort: Vec<JMAPComparator<U>>,
    pub since_query_state: JMAPState,
    pub max_changes: usize,
    pub up_to_id: Option<JMAPId>,
    pub calculate_total: bool,
}

#[derive(Debug)]
pub struct JMAPQueryResponse {
    pub query_state: JMAPState,
    pub is_immutable: bool,
    pub total: usize,
    pub ids: Vec<JMAPId>,
}

#[derive(Debug)]
pub struct JMAPQueryChangesResponseItem {
    pub id: JMAPId,
    pub index: usize,
}

#[derive(Debug)]
pub struct JMAPQueryChangesResponse {
    pub old_query_state: JMAPState,
    pub new_query_state: JMAPState,
    pub total: usize,
    pub removed: Vec<JMAPId>,
    pub added: Vec<JMAPQueryChangesResponseItem>,
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

#[derive(Debug)]
pub struct JMAPSet {
    pub account_id: AccountId,
    pub if_in_state: Option<JMAPState>,
    pub create: JSONValue,
    pub update: JSONValue,
    pub destroy: JSONValue,
}

#[derive(Debug)]
pub enum JMAPSetErrorType {
    Forbidden,
    OverQuota,
    TooLarge,
    RateLimit, // TODO implement rate limits
    NotFound,
    InvalidPatch,
    WillDestroy,
    InvalidProperties,
    Singleton,
    BlobNotFound,
}

impl JMAPSetErrorType {
    pub fn as_str(&self) -> &'static str {
        match self {
            JMAPSetErrorType::Forbidden => "forbidden",
            JMAPSetErrorType::OverQuota => "overQuota",
            JMAPSetErrorType::TooLarge => "tooLarge",
            JMAPSetErrorType::RateLimit => "rateLimit",
            JMAPSetErrorType::NotFound => "notFound",
            JMAPSetErrorType::InvalidPatch => "invalidPatch",
            JMAPSetErrorType::WillDestroy => "willDestroy",
            JMAPSetErrorType::InvalidProperties => "invalidProperties",
            JMAPSetErrorType::Singleton => "singleton",
            JMAPSetErrorType::BlobNotFound => "blobNotFound",
        }
    }
}

#[derive(Debug, Default)]
pub struct JMAPSetResponse {
    pub old_state: JMAPState,
    pub new_state: JMAPState,
    pub created: JSONValue,
    pub updated: JSONValue,
    pub destroyed: JSONValue,
    pub not_created: JSONValue,
    pub not_updated: JSONValue,
    pub not_destroyed: JSONValue,
}

impl JSONValue {
    pub fn new_error(error_type: JMAPSetErrorType, description: impl Into<String>) -> Self {
        let mut o = HashMap::with_capacity(2);
        o.insert(
            "error_type".to_string(),
            error_type.as_str().to_string().into(),
        );
        o.insert("description".to_string(), description.into().into());
        o.into()
    }

    pub fn new_invalid_property(
        property: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        let mut o = HashMap::with_capacity(2);
        o.insert(
            "error_type".to_string(),
            JMAPSetErrorType::InvalidProperties
                .as_str()
                .to_string()
                .into(),
        );
        o.insert("description".to_string(), description.into().into());
        o.insert(
            "properties".to_string(),
            vec![property.into().into()].into(),
        );
        o.into()
    }
}

pub struct JMAPGet<T> {
    pub account_id: AccountId,
    pub ids: Option<Vec<JMAPId>>,
    pub properties: Option<Vec<T>>,
}

#[derive(Debug)]
pub struct JMAPGetResponse {
    pub state: JMAPState,
    pub list: JSONValue,
    pub not_found: Option<Vec<JMAPId>>,
}
