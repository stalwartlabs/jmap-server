pub mod blob;
pub mod changes;
pub mod id;
pub mod json;
pub mod query;

use std::collections::HashMap;

pub use async_trait;
use changes::JMAPState;
use id::JMAPIdSerialize;
use json::JSONValue;
use store::{AccountId, JMAPId, StoreError};

pub const JMAP_MAIL: u8 = 0;
pub const JMAP_MAILBOX: u8 = 1;
pub const JMAP_MAILBOX_CHANGES: u8 = 2;
pub const JMAP_THREAD: u8 = 3;
pub const JMAP_BLOB: u8 = 4;

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
        match e {
            StoreError::AnchorNotFound => JMAPError::AnchorNotFound,
            _ => JMAPError::InternalError(e),
        }
    }
}

pub type Result<T> = std::result::Result<T, JMAPError>;

#[derive(Debug, Clone)]
pub struct JMAPQueryRequest<T, U, V> {
    pub account_id: AccountId,
    pub filter: JMAPFilter<T>,
    pub sort: Vec<JMAPComparator<U>>,
    pub position: i32,
    pub anchor: Option<JMAPId>,
    pub anchor_offset: i32,
    pub limit: usize,
    pub calculate_total: bool,
    pub arguments: V,
}

#[derive(Debug, Clone)]
pub struct JMAPQueryChangesRequest<T, U, V> {
    pub account_id: AccountId,
    pub filter: JMAPFilter<T>,
    pub sort: Vec<JMAPComparator<U>>,
    pub since_query_state: JMAPState,
    pub max_changes: usize,
    pub up_to_id: Option<JMAPId>,
    pub calculate_total: bool,
    pub arguments: V,
}

#[derive(Debug)]
pub struct JMAPQueryResponse {
    pub account_id: AccountId,
    pub query_state: JMAPState,
    pub is_immutable: bool,
    pub include_total: bool,
    pub position: usize,
    pub total: usize,
    pub limit: usize,
    pub ids: Vec<JMAPId>,
}

impl From<JMAPQueryResponse> for JSONValue {
    fn from(value: JMAPQueryResponse) -> Self {
        let mut obj = HashMap::new();
        obj.insert(
            "accountId".to_string(),
            (value.account_id as JMAPId).to_jmap_string().into(),
        );
        obj.insert("canCalculateChanges".to_string(), true.into());
        obj.insert(
            "queryState".to_string(),
            value.query_state.to_jmap_string().into(),
        );
        if value.include_total {
            obj.insert("total".to_string(), value.total.into());
        }
        if value.limit > 0 && value.total > value.limit {
            obj.insert("limit".to_string(), value.limit.into());
        }
        obj.insert("position".to_string(), value.position.into());
        obj.insert(
            "ids".to_string(),
            value
                .ids
                .into_iter()
                .map(|id| id.to_jmap_string().into())
                .collect::<Vec<JSONValue>>()
                .into(),
        );
        obj.into()
    }
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
pub struct JMAPSet<U> {
    pub account_id: AccountId,
    pub if_in_state: Option<JMAPState>,
    pub create: JSONValue,
    pub update: JSONValue,
    pub destroy: JSONValue,
    pub arguments: U,
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
    MailboxHasChild, //TODO abstract
    MailboxHasEmail,
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
            JMAPSetErrorType::MailboxHasChild => "mailboxHasChild",
            JMAPSetErrorType::MailboxHasEmail => "mailboxHasEmail",
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

impl From<JMAPSetResponse> for JSONValue {
    fn from(value: JMAPSetResponse) -> Self {
        let mut obj = HashMap::new();
        obj.insert("oldState".to_string(), value.old_state.into());
        obj.insert("newState".to_string(), value.new_state.into());
        obj.insert("created".to_string(), value.created);
        obj.insert("updated".to_string(), value.updated);
        obj.insert("destroyed".to_string(), value.destroyed);
        obj.insert("notCreated".to_string(), value.not_created);
        obj.insert("notUpdated".to_string(), value.not_updated);
        obj.insert("notDestroyed".to_string(), value.not_destroyed);
        obj.into()
    }
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

pub struct JMAPGet<T, U> {
    pub account_id: AccountId,
    pub ids: Option<Vec<JMAPId>>,
    pub properties: Option<Vec<T>>,
    pub arguments: U,
}

#[derive(Debug)]
pub struct JMAPGetResponse {
    pub state: JMAPState,
    pub list: JSONValue,
    pub not_found: Option<Vec<JMAPId>>,
}

impl From<JMAPGetResponse> for JSONValue {
    fn from(value: JMAPGetResponse) -> Self {
        let mut obj = HashMap::new();
        obj.insert("state".to_string(), value.state.into());
        obj.insert("list".to_string(), value.list);
        obj.insert(
            "notFound".to_string(),
            if let Some(not_found) = value.not_found {
                not_found
                    .into_iter()
                    .map(|id| id.to_jmap_string().into())
                    .collect::<Vec<JSONValue>>()
                    .into()
            } else {
                JSONValue::Null
            },
        );
        obj.into()
    }
}
