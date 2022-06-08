use std::{collections::HashMap, fmt::Display};

use serde::{Deserialize, Serialize};
use store::FieldId;

use crate::{
    jmap_store::orm,
    types::{blob::JMAPBlob, jmap::JMAPId},
};

#[derive(Debug, Clone, Default)]
pub struct Principal {
    pub properties: HashMap<Property, Value>,
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
#[repr(u8)]
pub enum Property {
    Id = 0,
    Type = 1,
    Name = 2,
    Description = 3,
    Email = 4,
    Timezone = 5,
    Capabilities = 6,
    Aliases = 7,
    Secret = 8,
    DKIM = 9,
    Quota = 10,
    Picture = 11,
    MemberOf = 12,
    Invalid = 13,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Type {
    #[serde(rename = "individual")]
    Individual,
    #[serde(rename = "group")]
    Group,
    #[serde(rename = "resource")]
    Resource,
    #[serde(rename = "location")]
    Location,
    #[serde(rename = "domain")]
    Domain,
    #[serde(rename = "list")]
    List,
    #[serde(rename = "other")]
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DKIM {
    #[serde(rename = "dkimSelector")]
    pub dkim_selector: Option<String>,
    #[serde(rename = "dkimExpiration")]
    pub dkim_expiration: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Id { value: JMAPId },
    Blob { value: JMAPBlob },
    Text { value: String },
    TextList { value: Vec<String> },
    Number { value: i64 },
    Type { value: Type },
    DKIM { value: DKIM },
    Members { value: Vec<JMAPId> },
    Null,
}

#[derive(Clone, Debug)]
pub enum Filter {
    Email { value: String },
    Name { value: String },
    Text { value: String },
    Type { value: Type },
    Timezone { value: String },
    MemberOf { value: JMAPId },
    QuotaLt { value: u64 },
    QuotaGt { value: u64 },
    Unsupported { value: String },
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "property")]
pub enum Comparator {
    #[serde(rename = "type")]
    Type,
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "email")]
    Email,
}

impl orm::Value for Value {
    fn index_as(&self) -> orm::IndexableValue {
        match self {
            Value::Text { value } => value.to_string().into(),
            Value::TextList { value } => todo!(),
            Value::Number { value } => (*value as u64).into(),
            Value::Type { value } => match value {
                Type::Individual => "i".to_string().into(),
                Type::Group => "g".to_string().into(),
                Type::Resource => "r".to_string().into(),
                Type::Location => "l".to_string().into(),
                Type::Domain => "d".to_string().into(),
                Type::List => "t".to_string().into(),
                Type::Other => "o".to_string().into(),
            },
            Value::Members { value } => todo!(),
            _ => orm::IndexableValue::Null,
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

impl Property {
    pub fn parse(value: &str) -> Self {
        match value {
            "id" => Property::Id,
            "type" => Property::Type,
            "name" => Property::Name,
            "description" => Property::Description,
            "email" => Property::Email,
            "timezone" => Property::Timezone,
            "capabilities" => Property::Capabilities,
            "secret" => Property::Secret,
            "aliases" => Property::Aliases,
            "dkim" => Property::DKIM,
            "quota" => Property::Quota,
            "picture" => Property::Picture,
            "memberOf" => Property::MemberOf,
            _ => Property::Invalid,
        }
    }
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => f.write_str("id"),
            Property::Type => f.write_str("type"),
            Property::Name => f.write_str("name"),
            Property::Description => f.write_str("description"),
            Property::Email => f.write_str("email"),
            Property::Timezone => f.write_str("timezone"),
            Property::Capabilities => f.write_str("capabilities"),
            Property::Secret => f.write_str("secret"),
            Property::DKIM => f.write_str("dkim"),
            Property::Quota => f.write_str("quota"),
            Property::Picture => f.write_str("picture"),
            Property::MemberOf => f.write_str("memberOf"),
            Property::Aliases => f.write_str("aliases"),
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
            1 => Property::Type,
            2 => Property::Name,
            3 => Property::Description,
            4 => Property::Email,
            5 => Property::Timezone,
            6 => Property::Capabilities,
            7 => Property::Aliases,
            8 => Property::Secret,
            9 => Property::DKIM,
            10 => Property::Quota,
            11 => Property::Picture,
            12 => Property::MemberOf,
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
