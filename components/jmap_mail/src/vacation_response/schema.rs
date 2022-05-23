use std::{collections::HashMap, fmt::Display};

use jmap::{id::jmap::JMAPId, jmap_store::orm::Indexable};
use serde::{Deserialize, Serialize};
use store::chrono::{DateTime, Utc};

#[derive(Debug, Clone, Default)]
pub struct VacationResponse {
    pub properties: HashMap<Property, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Id { value: JMAPId },
    Text { value: String },
    Bool { value: bool },
    DateTime { value: DateTime<Utc> },
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
    IsEnabled = 1,
    FromDate = 2,
    ToDate = 3,
    Subject = 4,
    TextBody = 5,
    HtmlBody = 6,
    Invalid = 7,
}

impl Property {
    pub fn parse(value: &str) -> Self {
        match value {
            "id" => Property::Id,
            "isEnabled" => Property::IsEnabled,
            "fromDate" => Property::FromDate,
            "toDate" => Property::ToDate,
            "subject" => Property::Subject,
            "textBody" => Property::TextBody,
            "htmlBody" => Property::HtmlBody,
            _ => Property::Invalid,
        }
    }
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::IsEnabled => write!(f, "isEnabled"),
            Property::FromDate => write!(f, "fromDate"),
            Property::ToDate => write!(f, "toDate"),
            Property::Subject => write!(f, "subject"),
            Property::TextBody => write!(f, "textBody"),
            Property::HtmlBody => write!(f, "htmlBody"),
            Property::Invalid => Ok(()),
        }
    }
}

impl From<Property> for u8 {
    fn from(property: Property) -> Self {
        property as u8
    }
}
