pub mod blob;
pub mod changes;
pub mod id;
pub mod json;
pub mod query;
pub mod request;

use std::collections::HashMap;

use json::JSONValue;
use store::StoreError;

#[derive(Debug)]
pub enum JMAPError {
    InvalidArguments(String),
    RequestTooLarge,
    StateMismatch,
    AnchorNotFound,
    UnsupportedFilter(String),
    UnsupportedSort(String),
    ServerFail(StoreError),
    UnknownMethod(String),
    ServerUnavailable,
    ServerPartialFail,
    InvalidResultReference(String),
    Forbidden,
    AccountNotFound,
    AccountNotSupportedByMethod,
    AccountReadOnly,
}

impl From<StoreError> for JMAPError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::AnchorNotFound => JMAPError::AnchorNotFound,
            _ => JMAPError::ServerFail(e),
        }
    }
}

pub type Result<T> = std::result::Result<T, JMAPError>;

#[derive(Debug)]
pub enum SetErrorType {
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

impl SetErrorType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SetErrorType::Forbidden => "forbidden",
            SetErrorType::OverQuota => "overQuota",
            SetErrorType::TooLarge => "tooLarge",
            SetErrorType::RateLimit => "rateLimit",
            SetErrorType::NotFound => "notFound",
            SetErrorType::InvalidPatch => "invalidPatch",
            SetErrorType::WillDestroy => "willDestroy",
            SetErrorType::InvalidProperties => "invalidProperties",
            SetErrorType::Singleton => "singleton",
            SetErrorType::BlobNotFound => "blobNotFound",
            SetErrorType::MailboxHasChild => "mailboxHasChild",
            SetErrorType::MailboxHasEmail => "mailboxHasEmail",
        }
    }
}

impl JSONValue {
    pub fn new_error(error_type: SetErrorType, description: impl Into<String>) -> Self {
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
            SetErrorType::InvalidProperties.as_str().to_string().into(),
        );
        o.insert("description".to_string(), description.into().into());
        o.insert(
            "properties".to_string(),
            vec![property.into().into()].into(),
        );
        o.into()
    }
}
