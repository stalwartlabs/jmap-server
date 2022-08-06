use std::borrow::Cow;

use store::core::error::StoreError;
use store::tracing::error;

#[derive(Debug, Clone, serde::Serialize)]
pub struct SetError<U> {
    #[serde(rename = "type")]
    pub type_: SetErrorType,
    description: Option<Cow<'static, str>>,
    properties: Option<Vec<U>>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum SetErrorType {
    #[serde(rename = "forbidden")]
    Forbidden,
    #[serde(rename = "overQuota")]
    OverQuota,
    #[serde(rename = "tooLarge")]
    TooLarge,
    #[serde(rename = "rateLimit")]
    RateLimit,
    #[serde(rename = "notFound")]
    NotFound,
    #[serde(rename = "invalidPatch")]
    InvalidPatch,
    #[serde(rename = "willDestroy")]
    WillDestroy,
    #[serde(rename = "invalidProperties")]
    InvalidProperties,
    #[serde(rename = "singleton")]
    Singleton,
    #[serde(rename = "mailboxHasChild")]
    MailboxHasChild,
    #[serde(rename = "mailboxHasEmail")]
    MailboxHasEmail,
    #[serde(rename = "blobNotFound")]
    BlobNotFound,
    #[serde(rename = "tooManyKeywords")]
    TooManyKeywords,
    #[serde(rename = "tooManyMailboxes")]
    TooManyMailboxes,
    #[serde(rename = "forbiddenFrom")]
    ForbiddenFrom,
    #[serde(rename = "invalidEmail")]
    InvalidEmail,
    #[serde(rename = "tooManyRecipients")]
    TooManyRecipients,
    #[serde(rename = "noRecipients")]
    NoRecipients,
    #[serde(rename = "invalidRecipients")]
    InvalidRecipients,
    #[serde(rename = "forbiddenMailFrom")]
    ForbiddenMailFrom,
    #[serde(rename = "forbiddenToSend")]
    ForbiddenToSend,
    #[serde(rename = "cannotUnsend")]
    CannotUnsend,
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
            SetErrorType::TooManyKeywords => "tooManyKeywords",
            SetErrorType::TooManyMailboxes => "tooManyMailboxes",
            SetErrorType::ForbiddenFrom => "forbiddenFrom",
            SetErrorType::InvalidEmail => "invalidEmail",
            SetErrorType::TooManyRecipients => "tooManyRecipients",
            SetErrorType::NoRecipients => "noRecipients",
            SetErrorType::InvalidRecipients => "invalidRecipients",
            SetErrorType::ForbiddenMailFrom => "forbiddenMailFrom",
            SetErrorType::ForbiddenToSend => "forbiddenToSend",
            SetErrorType::CannotUnsend => "cannotUnsend",
        }
    }
}

impl<U> SetError<U> {
    pub fn new_err(type_: SetErrorType) -> Self {
        SetError {
            type_,
            description: None,
            properties: None,
        }
    }

    pub fn new(type_: SetErrorType, description: impl Into<Cow<'static, str>>) -> Self {
        SetError {
            type_,
            description: description.into().into(),
            properties: None,
        }
    }

    pub fn invalid_property(property: U, description: impl Into<Cow<'static, str>>) -> Self {
        SetError {
            type_: SetErrorType::InvalidProperties,
            description: description.into().into(),
            properties: vec![property].into(),
        }
    }

    pub fn invalid_properties(
        properties: impl IntoIterator<Item = U>,
        description: impl Into<Cow<'static, str>>,
    ) -> Self {
        SetError {
            type_: SetErrorType::InvalidProperties,
            description: description.into().into(),
            properties: properties.into_iter().collect::<Vec<_>>().into(),
        }
    }

    pub fn forbidden(description: impl Into<Cow<'static, str>>) -> Self {
        SetError {
            type_: SetErrorType::Forbidden,
            description: description.into().into(),
            properties: None,
        }
    }
}

impl<U> From<StoreError> for SetError<U> {
    fn from(error: StoreError) -> Self {
        error!("Failed store operation: {:?}", error);
        if let StoreError::NotFound(_) = error {
            SetError::new(SetErrorType::NotFound, "Not found.")
        } else {
            SetError::new(
                SetErrorType::Forbidden,
                "There was a problem while processing your request.".to_string(),
            )
        }
    }
}

pub type Result<T, U> = std::result::Result<T, SetError<U>>;
