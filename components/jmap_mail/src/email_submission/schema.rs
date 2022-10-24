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

use std::fmt::Display;

use jmap::{
    orm,
    request::ResultReference,
    types::{blob::JMAPBlob, date::JMAPDate, jmap::JMAPId},
};
use serde::{Deserialize, Serialize};
use store::{ahash::AHashMap, core::vec_map::VecMap, FieldId};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EmailSubmission {
    pub properties: VecMap<Property, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Id {
        value: JMAPId,
    },
    Text {
        value: String,
    },
    DateTime {
        value: JMAPDate,
    },
    UndoStatus {
        value: UndoStatus,
    },
    DeliveryStatus {
        value: AHashMap<String, DeliveryStatus>,
    },
    Envelope {
        value: Envelope,
    },
    BlobIds {
        value: Vec<JMAPBlob>,
    },
    IdReference {
        value: String,
    },
    ResultReference {
        value: ResultReference,
    },
    Null,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    #[serde(rename = "mailFrom")]
    pub mail_from: Address,

    #[serde(rename = "rcptTo")]
    pub rcpt_to: Vec<Address>,
}

impl Envelope {
    pub fn new(email: String) -> Self {
        Envelope {
            mail_from: Address {
                email,
                parameters: None,
            },
            rcpt_to: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Address {
    pub email: String,
    pub parameters: Option<AHashMap<String, Option<String>>>,
}

impl Display for Address {
    // SMTP address format
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{}>", self.email)?;
        if let Some(parameters) = &self.parameters {
            for (key, value) in parameters {
                write!(f, " {}", key)?;
                if let Some(value) = value {
                    write!(f, "={}", value)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UndoStatus {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "final")]
    Final,
    #[serde(rename = "canceled")]
    Canceled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryStatus {
    #[serde(rename = "smtpReply")]
    pub smtp_reply: String,

    #[serde(rename = "delivered")]
    pub delivered: Delivered,

    #[serde(rename = "displayed")]
    pub displayed: Displayed,
}

impl DeliveryStatus {
    pub fn new(smtp_reply: String, delivered: Delivered, displayed: Displayed) -> Self {
        DeliveryStatus {
            smtp_reply,
            delivered,
            displayed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Delivered {
    #[serde(rename = "queued")]
    Queued,
    #[serde(rename = "yes")]
    Yes,
    #[serde(rename = "no")]
    No,
    #[serde(rename = "unknown")]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Displayed {
    #[serde(rename = "unknown")]
    Unknown,
    #[serde(rename = "yes")]
    Yes,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
#[repr(u8)]
pub enum Property {
    Id = 0,
    IdentityId = 1,
    EmailId = 2,
    ThreadId = 3,
    Envelope = 4,
    SendAt = 5,
    UndoStatus = 6,
    DeliveryStatus = 7,
    DsnBlobIds = 8,
    MdnBlobIds = 9,
    Invalid = 10,
}

impl Property {
    pub fn parse(value: &str) -> Property {
        match value {
            "id" => Property::Id,
            "identityId" => Property::IdentityId,
            "emailId" => Property::EmailId,
            "threadId" => Property::ThreadId,
            "envelope" => Property::Envelope,
            "sendAt" => Property::SendAt,
            "undoStatus" => Property::UndoStatus,
            "deliveryStatus" => Property::DeliveryStatus,
            "dsnBlobIds" => Property::DsnBlobIds,
            "mdnBlobIds" => Property::MdnBlobIds,
            _ => Property::Invalid,
        }
    }
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::IdentityId => write!(f, "identityId"),
            Property::EmailId => write!(f, "emailId"),
            Property::ThreadId => write!(f, "threadId"),
            Property::Envelope => write!(f, "envelope"),
            Property::SendAt => write!(f, "sendAt"),
            Property::UndoStatus => write!(f, "undoStatus"),
            Property::DeliveryStatus => write!(f, "deliveryStatus"),
            Property::DsnBlobIds => write!(f, "dsnBlobIds"),
            Property::MdnBlobIds => write!(f, "mdnBlobIds"),
            Property::Invalid => Ok(()),
        }
    }
}

impl From<Property> for FieldId {
    fn from(property: Property) -> Self {
        property as FieldId
    }
}

impl From<FieldId> for Property {
    fn from(field: FieldId) -> Self {
        match field {
            0 => Property::Id,
            1 => Property::IdentityId,
            2 => Property::EmailId,
            3 => Property::ThreadId,
            4 => Property::Envelope,
            5 => Property::SendAt,
            6 => Property::UndoStatus,
            7 => Property::DeliveryStatus,
            8 => Property::DsnBlobIds,
            9 => Property::MdnBlobIds,
            _ => Property::Invalid,
        }
    }
}

impl TryFrom<&str> for Property {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match Property::parse(value) {
            Property::Invalid => Err(()),
            property => Ok(property),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Filter {
    IdentityIds { value: Vec<JMAPId> },
    EmailIds { value: Vec<JMAPId> },
    ThreadIds { value: Vec<JMAPId> },
    UndoStatus { value: UndoStatus },
    Before { value: JMAPDate },
    After { value: JMAPDate },
    Unsupported { value: String },
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "property")]
pub enum Comparator {
    #[serde(rename = "emailId")]
    EmailId,
    #[serde(rename = "threadId")]
    ThreadId,
    #[serde(rename = "sentAt")]
    SentAt,
}

impl orm::Value for Value {
    fn index_as(&self) -> orm::Index {
        match self {
            Value::Id { value } => u64::from(value).into(),
            Value::DateTime { value } => (value.timestamp() as u64).into(),
            Value::UndoStatus { value } => match value {
                UndoStatus::Pending => "p".to_string().into(),
                UndoStatus::Final => "f".to_string().into(),
                UndoStatus::Canceled => "c".to_string().into(),
            },
            _ => orm::Index::Null,
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Value::Text { value } => value.is_empty(),
            Value::Null => true,
            _ => false,
        }
    }

    fn len(&self) -> usize {
        match self {
            Value::Id { .. } => std::mem::size_of::<JMAPId>(),
            Value::Text { value } => value.len(),
            Value::DateTime { .. } => std::mem::size_of::<JMAPDate>(),
            Value::UndoStatus { .. } => std::mem::size_of::<UndoStatus>(),
            Value::DeliveryStatus { value } => value.keys().fold(0, |acc, x| {
                acc + x.len() + std::mem::size_of::<DeliveryStatus>()
            }),
            Value::Envelope { value } => value.len(),
            Value::BlobIds { value } => value.len() * std::mem::size_of::<JMAPBlob>(),
            Value::IdReference { value } => value.len(),
            Value::ResultReference { .. } => std::mem::size_of::<ResultReference>(),
            Value::Null => 0,
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

impl Envelope {
    pub fn len(&self) -> usize {
        self.mail_from.len() + self.rcpt_to.iter().fold(0, |acc, item| acc + item.len())
    }

    pub fn is_empty(&self) -> bool {
        self.mail_from.is_empty() && self.rcpt_to.is_empty()
    }
}

impl Address {
    pub fn len(&self) -> usize {
        let mut size = self.email.len();
        if let Some(params) = &self.parameters {
            for (key, value) in params {
                size += key.len() + value.as_ref().map(|v| v.len()).unwrap_or(0);
            }
        }
        size
    }

    pub fn is_empty(&self) -> bool {
        self.email.is_empty() && self.parameters.is_none()
    }
}
