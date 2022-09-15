/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

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
