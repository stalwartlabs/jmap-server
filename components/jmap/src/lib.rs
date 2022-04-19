pub mod error;
pub mod id;
pub mod jmap_store;
pub mod protocol;
pub mod request;

use error::method::MethodError;

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

pub type Result<T> = std::result::Result<T, MethodError>;
