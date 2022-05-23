use std::{collections::HashMap, fmt};

use serde::{ser::SerializeMap, Deserialize, Serialize};

use crate::mail::schema::EmailAddress;

use super::schema::{Identity, Property, Value};

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
        formatter.write_str("a valid JMAP Identity property")
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

// Identity de/serialization
impl Serialize for Identity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(self.properties.len().into())?;

        for (name, value) in &self.properties {
            match value {
                Value::Id { value } => map.serialize_entry(name, value)?,
                Value::Text { value } => map.serialize_entry(name, value)?,
                Value::Bool { value } => map.serialize_entry(name, value)?,
                Value::Addresses { value } => map.serialize_entry(name, value)?,
                Value::Null => map.serialize_entry(name, &())?,
            }
        }

        map.end()
    }
}

struct IdentityVisitor;

impl<'de> serde::de::Visitor<'de> for IdentityVisitor {
    type Value = Identity;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP Identity object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut properties: HashMap<Property, Value> = HashMap::new();

        while let Some(key) = map.next_key::<&str>()? {
            match key {
                "name" => {
                    properties.insert(
                        Property::Name,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "email" => {
                    properties.insert(
                        Property::Email,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "textSignature" => {
                    properties.insert(
                        Property::TextSignature,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "htmlSignature" => {
                    properties.insert(
                        Property::HtmlSignature,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "replyTo" => {
                    properties.insert(
                        Property::ReplyTo,
                        if let Some(value) = map.next_value::<Option<Vec<EmailAddress>>>()? {
                            Value::Addresses { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "bcc" => {
                    properties.insert(
                        Property::Bcc,
                        if let Some(value) = map.next_value::<Option<Vec<EmailAddress>>>()? {
                            Value::Addresses { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                _ => (),
            }
        }

        Ok(Identity { properties })
    }
}

impl<'de> Deserialize<'de> for Identity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(IdentityVisitor)
    }
}
