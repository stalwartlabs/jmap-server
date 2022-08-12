pub mod error;
pub mod jmap_store;
pub mod orm;
pub mod principal;
pub mod push_subscription;
pub mod request;
pub mod types;

pub use base64;

use error::method::MethodError;
use store::AccountId;

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
    #[serde(rename(serialize = "urn:ietf:params:jmap:websocket"))]
    WebSocket,
}

pub type Result<T> = std::result::Result<T, MethodError>;

pub const SUPERUSER_ID: AccountId = 0;
pub const INGEST_ID: AccountId = 1;

// Basic email sanitizer
pub fn sanitize_email(email: &str) -> Option<String> {
    let mut result = String::with_capacity(email.len());
    let mut found_local = false;
    let mut found_domain = false;
    let mut last_ch = char::from(0);

    for ch in email.chars() {
        if !ch.is_whitespace() {
            if ch == '@' {
                if !result.is_empty() && !found_local {
                    found_local = true;
                } else {
                    return None;
                }
            } else if ch == '.' {
                if !(last_ch.is_alphanumeric() || last_ch == '-' || last_ch == '_') {
                    return None;
                } else if found_local {
                    found_domain = true;
                }
            }
            last_ch = ch;
            for ch in ch.to_lowercase() {
                result.push(ch);
            }
        }
    }

    if found_domain && last_ch != '.' {
        Some(result)
    } else {
        None
    }
}

// Basic domain sanitizer
pub fn sanitize_domain(domain: &str) -> Option<String> {
    let mut result = String::with_capacity(domain.len());
    let mut found_domain = false;
    let mut last_ch = char::from(0);

    for ch in domain.chars() {
        if !ch.is_whitespace() {
            if ch == '.' {
                if !(last_ch.is_alphanumeric() || last_ch == '-' || last_ch == '_') {
                    return None;
                } else if !found_domain {
                    found_domain = true;
                }
            }
            last_ch = ch;
            for ch in ch.to_lowercase() {
                result.push(ch);
            }
        }
    }

    if found_domain && last_ch != '.' {
        Some(result)
    } else {
        None
    }
}
