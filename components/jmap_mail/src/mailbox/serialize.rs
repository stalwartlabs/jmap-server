use std::{collections::HashMap, fmt};

use jmap::request::MaybeIdReference;
use serde::{ser::SerializeMap, Deserialize, Serialize};

use super::schema::{Mailbox, MailboxValue, Property};

// Property de/serialization
impl Serialize for Property {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}
struct PropertyVisitor;

impl<'de> serde::de::Visitor<'de> for PropertyVisitor {
    type Value = Property;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP Mailbox property")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Property::parse(v))
    }
}

impl<'de> Deserialize<'de> for Property {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(PropertyVisitor)
    }
}

// Mailbox de/serialization
impl Serialize for Mailbox {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(self.properties.len().into())?;

        for (name, value) in &self.properties {
            match value {
                MailboxValue::Id { value } => map.serialize_entry(name, value)?,
                MailboxValue::Text { value } => map.serialize_entry(name, value)?,
                MailboxValue::Bool { value } => map.serialize_entry(name, value)?,
                MailboxValue::Number { value } => map.serialize_entry(name, value)?,
                MailboxValue::MailboxRights { value } => map.serialize_entry(name, value)?,
                MailboxValue::Null => map.serialize_entry(name, &None::<&str>)?,
                MailboxValue::ResultReference { value } => map.serialize_entry(name, value)?,
                MailboxValue::IdReference { value } => {
                    map.serialize_entry(name, &format!("#{}", value))?
                }
            }
        }

        map.end()
    }
}

struct MailboxVisitor;

impl<'de> serde::de::Visitor<'de> for MailboxVisitor {
    type Value = Mailbox;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP e-mail object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut properties: HashMap<Property, MailboxValue> = HashMap::new();

        while let Some(key) = map.next_key::<&str>()? {
            match key {
                "name" => {
                    properties.insert(
                        Property::Name,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            MailboxValue::Text { value }
                        } else {
                            MailboxValue::Null
                        },
                    );
                }
                "parentId" => {
                    properties.insert(
                        Property::ParentId,
                        if let Some(value) = map.next_value::<Option<MaybeIdReference>>()? {
                            match value {
                                MaybeIdReference::Value(value) => MailboxValue::Id { value },
                                MaybeIdReference::Reference(value) => {
                                    MailboxValue::IdReference { value }
                                }
                            }
                        } else {
                            MailboxValue::Null
                        },
                    );
                }
                "role" => {
                    properties.insert(
                        Property::Role,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            MailboxValue::Text { value }
                        } else {
                            MailboxValue::Null
                        },
                    );
                }
                "sortOrder" => {
                    properties.insert(
                        Property::SortOrder,
                        if let Some(value) = map.next_value::<Option<u32>>()? {
                            MailboxValue::Number { value }
                        } else {
                            MailboxValue::Null
                        },
                    );
                }
                "#parentId" => {
                    properties.insert(
                        Property::ParentId,
                        MailboxValue::ResultReference {
                            value: map.next_value()?,
                        },
                    );
                }
                _ => (),
            }
        }

        Ok(Mailbox { properties })
    }
}

impl<'de> Deserialize<'de> for Mailbox {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(MailboxVisitor)
    }
}
