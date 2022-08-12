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
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}
