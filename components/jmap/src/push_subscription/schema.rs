use std::{collections::HashMap, fmt::Display};

use serde::{Deserialize, Serialize};
use store::{
    chrono::{DateTime, Utc},
    FieldId,
};

use crate::{id::jmap::JMAPId, protocol::type_state::TypeState};

#[derive(Debug, Clone, Default)]
pub struct PushSubscription {
    pub properties: HashMap<Property, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Keys {
    pub p256dh: String,
    pub auth: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Value {
    Id { value: JMAPId },
    Text { value: String },
    DateTime { value: DateTime<Utc> },
    Types { value: Vec<TypeState> },
    Keys { value: Keys },
    Null,
}

impl Value {
    pub fn unwrap_text(self) -> Option<String> {
        match self {
            Value::Text { value } => Some(value),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text { value } => Some(value),
            _ => None,
        }
    }

    pub fn as_timestamp(&self) -> Option<i64> {
        match self {
            Value::DateTime { value } => Some(value.timestamp()),
            _ => None,
        }
    }
}

impl crate::jmap_store::orm::Value for Value {
    fn index_as(&self) -> crate::jmap_store::orm::IndexableValue {
        crate::jmap_store::orm::IndexableValue::Null
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

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[repr(u8)]
pub enum Property {
    Id = 0,
    DeviceClientId = 1,
    Url = 2,
    Keys = 3,
    VerificationCode = 4,
    Expires = 5,
    Types = 6,
    VerificationCode_ = 7,
}

impl Property {
    pub fn parse(value: &str) -> Self {
        match value {
            "id" => Property::Id,
            "deviceClientId" => Property::DeviceClientId,
            "url" => Property::Url,
            "keys" => Property::Keys,
            "verificationCode" => Property::VerificationCode,
            "expires" => Property::Expires,
            "types" => Property::Types,
            _ => Property::VerificationCode_,
        }
    }
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::DeviceClientId => write!(f, "deviceClientId"),
            Property::Url => write!(f, "url"),
            Property::Keys => write!(f, "keys"),
            Property::VerificationCode => write!(f, "verificationCode"),
            Property::Expires => write!(f, "expires"),
            Property::Types => write!(f, "types"),
            Property::VerificationCode_ => Ok(()),
        }
    }
}

impl From<Property> for FieldId {
    fn from(field: Property) -> Self {
        field as FieldId
    }
}

impl From<FieldId> for Property {
    fn from(field: FieldId) -> Self {
        match field {
            0 => Property::Id,
            1 => Property::DeviceClientId,
            2 => Property::Url,
            3 => Property::Keys,
            4 => Property::VerificationCode,
            5 => Property::Expires,
            6 => Property::Types,
            7 => Property::VerificationCode_,
            _ => Property::VerificationCode_,
        }
    }
}

impl TryFrom<&str> for Property {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match Property::parse(value) {
            Property::VerificationCode_ => Err(()),
            property => Ok(property),
        }
    }
}
