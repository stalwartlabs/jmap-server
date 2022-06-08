use std::{collections::HashMap, fmt};

use serde::{ser::SerializeMap, Deserialize, Serialize};

use crate::types::{blob::JMAPBlob, jmap::JMAPId};

use super::schema::{Filter, Principal, Property, Type, Value, DKIM};

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
        formatter.write_str("a valid Principal property")
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

// Principal de/serialization
impl Serialize for Principal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(self.properties.len().into())?;

        for (name, value) in &self.properties {
            match value {
                Value::Id { value } => map.serialize_entry(name, value)?,
                Value::Text { value } => map.serialize_entry(name, value)?,
                Value::Null => map.serialize_entry(name, &None::<&str>)?,
                Value::TextList { value } => map.serialize_entry(name, value)?,
                Value::Number { value } => map.serialize_entry(name, value)?,
                Value::Type { value } => map.serialize_entry(name, value)?,
                Value::Members { value } => map.serialize_entry(name, value)?,
                Value::Blob { value } => map.serialize_entry(name, value)?,
                Value::DKIM { value } => map.serialize_entry(name, value)?,
            }
        }

        map.end()
    }
}

struct PrincipalVisitor;

impl<'de> serde::de::Visitor<'de> for PrincipalVisitor {
    type Value = Principal;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP Principal object")
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
                "description" => {
                    properties.insert(
                        Property::Description,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "timezone" => {
                    properties.insert(
                        Property::Timezone,
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
                "aliases" => {
                    properties.insert(
                        Property::Aliases,
                        if let Some(value) = map.next_value::<Option<Vec<String>>>()? {
                            Value::TextList { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "capabilities" => {
                    properties.insert(
                        Property::Capabilities,
                        if let Some(value) = map.next_value::<Option<Vec<String>>>()? {
                            Value::TextList { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "type" => {
                    properties.insert(
                        Property::Type,
                        Value::Type {
                            value: map.next_value::<Type>()?,
                        },
                    );
                }
                "secret" => {
                    properties.insert(
                        Property::Secret,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "dkim" => {
                    properties.insert(
                        Property::DKIM,
                        if let Some(value) = map.next_value::<Option<DKIM>>()? {
                            Value::DKIM { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "memberOf" => {
                    properties.insert(
                        Property::MemberOf,
                        if let Some(value) = map.next_value::<Option<Vec<JMAPId>>>()? {
                            Value::Members { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "quota" => {
                    properties.insert(
                        Property::Quota,
                        if let Some(value) = map.next_value::<Option<u64>>()? {
                            Value::Number {
                                value: value as i64,
                            }
                        } else {
                            Value::Null
                        },
                    );
                }
                "picture" => {
                    properties.insert(
                        Property::Picture,
                        if let Some(value) = map.next_value::<Option<JMAPBlob>>()? {
                            Value::Blob { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                _ => (),
            }
        }

        Ok(Principal { properties })
    }
}

impl<'de> Deserialize<'de> for Principal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(PrincipalVisitor)
    }
}

// Filter deserializer
struct FilterVisitor;

impl<'de> serde::de::Visitor<'de> for FilterVisitor {
    type Value = Filter;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid Principal filter")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        Ok(
            match map
                .next_key::<&str>()?
                .ok_or_else(|| serde::de::Error::custom("Missing filter property"))?
            {
                "email" => Filter::Email {
                    value: map.next_value()?,
                },
                "name" => Filter::Name {
                    value: map.next_value()?,
                },
                "text" => Filter::Text {
                    value: map.next_value()?,
                },
                "type" => Filter::Type {
                    value: map.next_value()?,
                },
                "timezone" => Filter::Timezone {
                    value: map.next_value()?,
                },
                "memberOf" => Filter::MemberOf {
                    value: map.next_value()?,
                },
                "quotaLowerThan" => Filter::QuotaLt {
                    value: map.next_value()?,
                },
                "quotaGreaterThan" => Filter::QuotaGt {
                    value: map.next_value()?,
                },
                unsupported => Filter::Unsupported {
                    value: unsupported.to_string(),
                },
            },
        )
    }
}

impl<'de> Deserialize<'de> for Filter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(FilterVisitor)
    }
}
