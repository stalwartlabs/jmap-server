use std::collections::HashMap;

use crate::protocol::json::JSONValue;

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

    pub fn store_error() -> Self {
        JSONValue::new_error(
            SetErrorType::Forbidden,
            "There was a problem while processing your request.".to_string(),
        )
    }
}
