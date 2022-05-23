use std::{collections::HashMap, fmt::Display};

use jmap::{id::jmap::JMAPId, jmap_store::orm::Indexable};
use serde::{Deserialize, Serialize};

use crate::mail::schema::EmailAddress;

#[derive(Debug, Clone, Default)]
pub struct Identity {
    pub properties: HashMap<Property, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Value {
    Id { value: JMAPId },
    Text { value: String },
    Bool { value: bool },
    Addresses { value: Vec<EmailAddress> },
    Null,
}

impl Indexable for Value {
    fn index_as(&self) -> jmap::jmap_store::orm::Value<Self> {
        jmap::jmap_store::orm::Value::Null
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
#[repr(u8)]
pub enum Property {
    Id = 0,
    Name = 1,
    Email = 2,
    ReplyTo = 3,
    Bcc = 4,
    TextSignature = 5,
    HtmlSignature = 6,
    MayDelete = 7,
    Invalid = 8,
}

impl Property {
    pub fn parse(value: &str) -> Self {
        match value {
            "id" => Property::Id,
            "name" => Property::Name,
            "email" => Property::Email,
            "replyTo" => Property::ReplyTo,
            "bcc" => Property::Bcc,
            "textSignature" => Property::TextSignature,
            "htmlSignature" => Property::HtmlSignature,
            "mayDelete" => Property::MayDelete,
            _ => Property::Invalid,
        }
    }
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::Name => write!(f, "name"),
            Property::Email => write!(f, "email"),
            Property::ReplyTo => write!(f, "replyTo"),
            Property::Bcc => write!(f, "bcc"),
            Property::TextSignature => write!(f, "textSignature"),
            Property::HtmlSignature => write!(f, "htmlSignature"),
            Property::MayDelete => write!(f, "mayDelete"),
            Property::Invalid => Ok(()),
        }
    }
}

impl From<Property> for u8 {
    fn from(property: Property) -> Self {
        property as u8
    }
}
