use std::{collections::HashMap, fmt};

use serde::{ser::SerializeMap, Deserialize, Serialize};
use store::chrono::{DateTime, Utc};

use super::schema::{Property, VacationResponse, Value};

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
        formatter.write_str("a valid JMAP VacationResponse property")
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

// VacationResponse de/serialization
impl Serialize for VacationResponse {
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
                Value::DateTime { value } => map.serialize_entry(name, value)?,
                Value::Bool { value } => map.serialize_entry(name, value)?,
                Value::SentResponses { .. } => (),
            }
        }

        map.end()
    }
}

struct VacationResponseVisitor;

impl<'de> serde::de::Visitor<'de> for VacationResponseVisitor {
    type Value = VacationResponse;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP VacationResponse object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut properties: HashMap<Property, Value> = HashMap::new();

        while let Some(key) = map.next_key::<&str>()? {
            match key {
                "subject" => {
                    properties.insert(
                        Property::Subject,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "textBody" => {
                    properties.insert(
                        Property::TextBody,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "htmlBody" => {
                    properties.insert(
                        Property::HtmlBody,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "isEnabled" => {
                    properties.insert(
                        Property::IsEnabled,
                        Value::Bool {
                            value: map.next_value::<Option<bool>>()?.unwrap_or(false),
                        },
                    );
                }
                "fromDate" => {
                    properties.insert(
                        Property::FromDate,
                        if let Some(value) = map.next_value::<Option<DateTime<Utc>>>()? {
                            Value::DateTime { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "toDate" => {
                    properties.insert(
                        Property::ToDate,
                        if let Some(value) = map.next_value::<Option<DateTime<Utc>>>()? {
                            Value::DateTime { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                _ => (),
            }
        }

        Ok(VacationResponse { properties })
    }
}

impl<'de> Deserialize<'de> for VacationResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(VacationResponseVisitor)
    }
}
