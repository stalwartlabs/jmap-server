use std::collections::HashMap;

use crate::protocol::json::JSONValue;
use store::tracing::error;
use store::StoreError;

pub struct SetError {
    pub error_type: SetErrorType,
    pub description: Option<String>,
    pub properties: Option<Vec<JSONValue>>,
}

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

impl SetError {
    pub fn new_err(error_type: SetErrorType) -> Self {
        SetError {
            error_type,
            description: None,
            properties: None,
        }
    }
    pub fn new(error_type: SetErrorType, description: impl Into<String>) -> Self {
        SetError {
            error_type,
            description: description.into().into(),
            properties: None,
        }
    }
    pub fn invalid_property(property: impl Into<String>, description: impl Into<String>) -> Self {
        SetError {
            error_type: SetErrorType::InvalidProperties,
            description: description.into().into(),
            properties: vec![property.into().into()].into(),
        }
    }
}

impl From<SetError> for JSONValue {
    fn from(err: SetError) -> Self {
        let mut o = HashMap::with_capacity(2);
        o.insert(
            "error_type".to_string(),
            err.error_type.as_str().to_string().into(),
        );
        if let Some(description) = err.description {
            o.insert("description".to_string(), description.into());
        } else {
            o.insert(
                "description".to_string(),
                match err.error_type {
                    SetErrorType::Forbidden => "Forbidden.".to_string().into(),
                    SetErrorType::OverQuota => "Over quota.".to_string().into(),
                    SetErrorType::TooLarge => "Too large.".to_string().into(),
                    SetErrorType::RateLimit => "Rate limit.".to_string().into(),
                    SetErrorType::NotFound => "Not found.".to_string().into(),
                    SetErrorType::InvalidPatch => "Invalid patch.".to_string().into(),
                    SetErrorType::WillDestroy => "Will be destroyed.".to_string().into(),
                    SetErrorType::InvalidProperties => "Invalid properties.".to_string().into(),
                    SetErrorType::Singleton => "Singleton.".to_string().into(),
                    SetErrorType::BlobNotFound => "Blob not found.".to_string().into(),
                    SetErrorType::MailboxHasChild => "Mailbox has child.".to_string().into(),
                    SetErrorType::MailboxHasEmail => "Mailbox has email.".to_string().into(),
                },
            );
        }
        if let Some(properties) = err.properties {
            o.insert("properties".to_string(), properties.into());
        }

        o.into()
    }
}

impl From<StoreError> for SetError {
    fn from(error: StoreError) -> Self {
        error!("Failed store operation: {:?}", error);
        SetError::new(
            SetErrorType::Forbidden,
            "There was a problem while processing your request.".to_string(),
        )
    }
}

pub type Result<T> = std::result::Result<T, SetError>;
