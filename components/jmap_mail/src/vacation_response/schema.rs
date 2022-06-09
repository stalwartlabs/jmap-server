use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use jmap::{orm, types::jmap::JMAPId};
use serde::{Deserialize, Serialize};
use store::{
    chrono::{DateTime, Utc},
    FieldId,
};

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
    SentResponses { value: HashSet<String> },
    Null,
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

impl orm::Value for Value {
    fn index_as(&self) -> orm::Index {
        orm::Index::Null
    }

    fn is_empty(&self) -> bool {
        match self {
            Value::Text { value } => value.is_empty(),
            Value::Null => true,
            _ => false,
        }
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
    SentResponses_ = 7,
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
            _ => Property::SentResponses_,
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
            Property::SentResponses_ => Ok(()),
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
            1 => Property::IsEnabled,
            2 => Property::FromDate,
            3 => Property::ToDate,
            4 => Property::Subject,
            5 => Property::TextBody,
            6 => Property::HtmlBody,
            _ => Property::SentResponses_,
        }
    }
}

impl TryFrom<&str> for Property {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match Property::parse(value) {
            Property::SentResponses_ => Err(()),
            property => Ok(property),
        }
    }
}
