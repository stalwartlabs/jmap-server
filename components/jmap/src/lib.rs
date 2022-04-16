pub mod blob;
pub mod changes;
pub mod id;
pub mod json;
pub mod query;
pub mod request;

use std::collections::HashMap;

use json::JSONValue;
use store::{tracing::error, StoreError};

#[derive(Debug, Clone, serde::Serialize, Hash, PartialEq, Eq)]
pub enum URI {
    #[serde(rename(serialize = "urn:ietf:params:jmap:core"))]
    Core,
    #[serde(rename(serialize = "urn:ietf:params:jmap:mail"))]
    Mail,
    #[serde(rename(serialize = "urn:ietf:params:jmap:submission"))]
    Submission,
    #[serde(rename(serialize = "urn:ietf:params:jmap:vacationresponse"))]
    VacationResponse,
    #[serde(rename(serialize = "urn:ietf:params:jmap:contacts"))]
    Contacts,
    #[serde(rename(serialize = "urn:ietf:params:jmap:calendars"))]
    Calendars,
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub enum RequestLimitError {
    #[serde(rename(serialize = "maxSizeRequest"))]
    Size,
    #[serde(rename(serialize = "maxCallsInRequest"))]
    CallsIn,
    #[serde(rename(serialize = "maxConcurrentRequests"))]
    Concurrent,
}

#[derive(Debug, serde::Serialize)]
enum RequestErrorType {
    #[serde(rename(serialize = "urn:ietf:params:jmap:error:unknownCapability"))]
    UnknownCapability,
    #[serde(rename(serialize = "urn:ietf:params:jmap:error:notJSON"))]
    NotJSON,
    #[serde(rename(serialize = "urn:ietf:params:jmap:error:notRequest"))]
    NotRequest,
    #[serde(rename(serialize = "urn:ietf:params:jmap:error:limit"))]
    Limit,
}

#[derive(Debug, serde::Serialize)]
pub struct RequestError {
    #[serde(rename(serialize = "type"))]
    error: RequestErrorType,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<RequestLimitError>,
    status: u32,
    detail: String,
}

impl RequestError {
    pub fn unknown_capability(capability: &str) -> RequestError {
        RequestError {
            error: RequestErrorType::UnknownCapability,
            limit: None,
            status: 400,
            detail: format!(
                concat!(
                    "The Request object used capability ",
                    "'{}', which is not supported",
                    "by this server."
                ),
                capability
            ),
        }
    }

    pub fn not_json() -> RequestError {
        RequestError {
            error: RequestErrorType::NotJSON,
            limit: None,
            status: 400,
            detail: "The Request object is not a valid JSON object.".to_string(),
        }
    }

    pub fn not_request() -> RequestError {
        RequestError {
            error: RequestErrorType::NotRequest,
            limit: None,
            status: 400,
            detail: "The Request object is not a valid JMAP request.".to_string(),
        }
    }

    pub fn limit(limit: RequestLimitError) -> RequestError {
        RequestError {
            error: RequestErrorType::Limit,
            limit: Some(limit),
            status: 400,
            detail: match limit {
                RequestLimitError::Size => concat!(
                    "The request is larger than the server ",
                    "is willing to process."
                )
                .to_string(),
                RequestLimitError::CallsIn => concat!(
                    "The request exceeds the maximum number ",
                    "of calls in a single request."
                )
                .to_string(),
                RequestLimitError::Concurrent => concat!(
                    "The request exceeds the maximum number ",
                    "of concurrent requests."
                )
                .to_string(),
            },
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

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
            StoreError::InvalidArguments(err) => JMAPError::InvalidArguments(err),
            _ => JMAPError::ServerFail(e),
        }
    }
}

impl From<JMAPError> for JSONValue {
    fn from(error: JMAPError) -> Self {
        let (error_type, description) = match error {
            JMAPError::InvalidArguments(description) => ("invalidArguments", description),
            JMAPError::RequestTooLarge => (
                "requestTooLarge",
                concat!(
                    "The number of ids requested by the client exceeds the maximum number ",
                    "the server is willing to process in a single method call."
                )
                .to_string(),
            ),
            JMAPError::StateMismatch => (
                "stateMismatch",
                concat!(
                    "An \"ifInState\" argument was supplied, but ",
                    "it does not match the current state."
                )
                .to_string(),
            ),
            JMAPError::AnchorNotFound => (
                "anchorNotFound",
                concat!(
                    "An anchor argument was supplied, but it ",
                    "cannot be found in the results of the query."
                )
                .to_string(),
            ),
            JMAPError::UnsupportedFilter(description) => ("unsupportedFilter", description),
            JMAPError::UnsupportedSort(description) => ("unsupportedSort", description),
            JMAPError::ServerFail(e) => ("serverFail", {
                error!("JMAP request failed: {:?}", e);
                concat!(
                    "An unexpected error occurred while processing ",
                    "this call, please contact the system administrator."
                )
                .to_string()
            }),
            JMAPError::UnknownMethod(description) => ("unknownMethod", description),
            JMAPError::ServerUnavailable => (
                "serverUnavailable",
                concat!(
                    "This server is temporarily unavailable. ",
                    "Attempting this same operation later may succeed."
                )
                .to_string(),
            ),
            JMAPError::ServerPartialFail => (
                "serverPartialFail",
                concat!(
                    "Some, but not all, expected changes described by the method ",
                    "occurred.  Please resynchronise to determine server state."
                )
                .to_string(),
            ),
            JMAPError::InvalidResultReference(description) => {
                ("invalidResultReference", description)
            }
            JMAPError::Forbidden => (
                "forbidden",
                concat!(
                    "The method and arguments are valid, but executing the ",
                    "method would violate an Access Control List (ACL) or ",
                    "other permissions policy."
                )
                .to_string(),
            ),
            JMAPError::AccountNotFound => (
                "accountNotFound",
                "The accountId does not correspond to a valid account".to_string(),
            ),
            JMAPError::AccountNotSupportedByMethod => (
                "accountNotSupportedByMethod",
                concat!(
                    "The accountId given corresponds to a valid account, ",
                    "but the account does not support this method or data type."
                )
                .to_string(),
            ),
            JMAPError::AccountReadOnly => (
                "accountReadOnly",
                "This method modifies state, but the account is read-only.".to_string(),
            ),
        };

        let mut o = HashMap::with_capacity(2);
        o.insert("type".to_string(), error_type.to_string().into());
        o.insert("description".to_string(), description.into());
        o.into()
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

#[derive(Debug, serde::Serialize)]
pub struct ProblemDetails {
    #[serde(rename(serialize = "type"))]
    p_type: String,
    pub status: u16,
    title: String,
    detail: String,
}

impl ProblemDetails {
    pub fn new(status: u16, title: impl Into<String>, detail: impl Into<String>) -> Self {
        ProblemDetails {
            p_type: "about:blank".to_string(),
            status,
            title: title.into(),
            detail: detail.into(),
        }
    }

    pub fn internal_server_error() -> Self {
        ProblemDetails::new(
            500,
            "Internal Server Error",
            concat!(
                "There was a problem while processing your request. ",
                "Please contact the system administrator."
            ),
        )
    }

    pub fn invalid_parameters() -> Self {
        ProblemDetails::new(
            400,
            "Invalid Parameters",
            "One or multiple parameters could not be parsed.",
        )
    }

    pub fn forbidden() -> Self {
        ProblemDetails::new(
            403,
            "Forbidden",
            "You do not have enough permissions to access this resource.",
        )
    }

    pub fn not_found() -> Self {
        ProblemDetails::new(
            404,
            "Not Found",
            "The requested resource does not exist on this server.",
        )
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}
