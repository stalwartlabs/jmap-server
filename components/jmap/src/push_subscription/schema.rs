use std::{collections::HashMap, fmt::Display};

use serde::{Deserialize, Serialize};
use store::{
    chrono::{DateTime, Utc},
    FieldId,
};

use crate::{id::jmap::JMAPId, jmap_store::orm::Indexable, protocol::TypeState};

#[derive(Debug, Clone, Default)]
pub struct PushSubscription {
    pub properties: HashMap<Property, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Keys {
    p256dh: String,
    auth: String,
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

impl Indexable for Value {
    fn index_as(&self) -> crate::jmap_store::orm::Value<Self> {
        crate::jmap_store::orm::Value::Null
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
