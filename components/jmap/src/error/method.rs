use std::fmt::Display;

use serde::ser::SerializeMap;
use serde::Serialize;
use store::core::error::StoreError;
use store::tracing::error;

#[derive(Debug)]
pub enum MethodError {
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

impl From<StoreError> for MethodError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::AnchorNotFound => MethodError::AnchorNotFound,
            StoreError::InvalidArguments(err) => MethodError::InvalidArguments(err),
            _ => MethodError::ServerFail(e),
        }
    }
}

impl Display for MethodError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            MethodError::InvalidArguments(err) => write!(f, "Invalid arguments: {}", err),
            MethodError::RequestTooLarge => write!(f, "Request too large"),
            MethodError::StateMismatch => write!(f, "State mismatch"),
            MethodError::AnchorNotFound => write!(f, "Anchor not found"),
            MethodError::UnsupportedFilter(err) => write!(f, "Unsupported filter: {}", err),
            MethodError::UnsupportedSort(err) => write!(f, "Unsupported sort: {}", err),
            MethodError::ServerFail(err) => write!(f, "Server error: {}", err),
            MethodError::UnknownMethod(err) => write!(f, "Unknown method: {}", err),
            MethodError::ServerUnavailable => write!(f, "Server unavailable"),
            MethodError::ServerPartialFail => write!(f, "Server partial fail"),
            MethodError::InvalidResultReference(err) => {
                write!(f, "Invalid result reference: {}", err)
            }
            MethodError::Forbidden => write!(f, "Forbidden"),
            MethodError::AccountNotFound => write!(f, "Account not found"),
            MethodError::AccountNotSupportedByMethod => {
                write!(f, "Account not supported by method")
            }
            MethodError::AccountReadOnly => write!(f, "Account read only"),
        }
    }
}

impl Serialize for MethodError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(2.into())?;

        let (error_type, description) = match self {
            MethodError::InvalidArguments(description) => {
                ("invalidArguments", description.as_str())
            }
            MethodError::RequestTooLarge => (
                "requestTooLarge",
                concat!(
                    "The number of ids requested by the client exceeds the maximum number ",
                    "the server is willing to process in a single method call."
                ),
            ),
            MethodError::StateMismatch => (
                "stateMismatch",
                concat!(
                    "An \"ifInState\" argument was supplied, but ",
                    "it does not match the current state."
                ),
            ),
            MethodError::AnchorNotFound => (
                "anchorNotFound",
                concat!(
                    "An anchor argument was supplied, but it ",
                    "cannot be found in the results of the query."
                ),
            ),
            MethodError::UnsupportedFilter(description) => {
                ("unsupportedFilter", description.as_str())
            }
            MethodError::UnsupportedSort(description) => ("unsupportedSort", description.as_str()),
            MethodError::ServerFail(e) => ("serverFail", {
                error!("JMAP request failed: {:?}", e);
                concat!(
                    "An unexpected error occurred while processing ",
                    "this call, please contact the system administrator."
                )
            }),
            MethodError::UnknownMethod(description) => ("unknownMethod", description.as_str()),
            MethodError::ServerUnavailable => (
                "serverUnavailable",
                concat!(
                    "This server is temporarily unavailable. ",
                    "Attempting this same operation later may succeed."
                ),
            ),
            MethodError::ServerPartialFail => (
                "serverPartialFail",
                concat!(
                    "Some, but not all, expected changes described by the method ",
                    "occurred.  Please resynchronise to determine server state."
                ),
            ),
            MethodError::InvalidResultReference(description) => {
                ("invalidResultReference", description.as_str())
            }
            MethodError::Forbidden => (
                "forbidden",
                concat!(
                    "The method and arguments are valid, but executing the ",
                    "method would violate an Access Control List (ACL) or ",
                    "other permissions policy."
                ),
            ),
            MethodError::AccountNotFound => (
                "accountNotFound",
                "The accountId does not correspond to a valid account",
            ),
            MethodError::AccountNotSupportedByMethod => (
                "accountNotSupportedByMethod",
                concat!(
                    "The accountId given corresponds to a valid account, ",
                    "but the account does not support this method or data type."
                ),
            ),
            MethodError::AccountReadOnly => (
                "accountReadOnly",
                "This method modifies state, but the account is read-only.",
            ),
        };

        map.serialize_entry("type", error_type)?;
        map.serialize_entry("description", description)?;
        map.end()
    }
}
