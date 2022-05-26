use std::{collections::HashMap, fmt};

use serde::{ser::SerializeMap, Deserialize, Serialize};
use store::chrono::{DateTime, Utc};

use crate::types::type_state::TypeState;

use super::schema::{Keys, Property, PushSubscription, Value};

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
        formatter.write_str("a valid JMAP PushSubscription property")
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

// PushSubscription de/serialization
impl Serialize for PushSubscription {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(self.properties.len().into())?;

        for (name, value) in &self.properties {
            match value {
                Value::Id { value } => map.serialize_entry(name, value)?,
                Value::Text { value } => map.serialize_entry(name, value)?,
                Value::DateTime { value } => map.serialize_entry(name, value)?,
                Value::Types { value } => map.serialize_entry(name, value)?,
                Value::Keys { value } => map.serialize_entry(name, value)?,
                Value::Null => map.serialize_entry(name, &())?,
            }
        }

        map.end()
    }
}

struct PushSubscriptionVisitor;

impl<'de> serde::de::Visitor<'de> for PushSubscriptionVisitor {
    type Value = PushSubscription;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP PushSubscription object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut properties: HashMap<Property, Value> = HashMap::new();

        while let Some(key) = map.next_key::<&str>()? {
            match key {
                "deviceClientId" => {
                    properties.insert(
                        Property::DeviceClientId,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "url" => {
                    properties.insert(
                        Property::Url,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "verificationCode" => {
                    properties.insert(
                        Property::VerificationCode,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "keys" => {
                    properties.insert(
                        Property::Keys,
                        if let Some(value) = map.next_value::<Option<Keys>>()? {
                            Value::Keys { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "expires" => {
                    properties.insert(
                        Property::Expires,
                        if let Some(value) = map.next_value::<Option<DateTime<Utc>>>()? {
                            Value::DateTime { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "types" => {
                    properties.insert(
                        Property::Types,
                        if let Some(value) = map.next_value::<Option<Vec<TypeState>>>()? {
                            Value::Types { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                _ => (),
            }
        }

        Ok(PushSubscription { properties })
    }
}

impl<'de> Deserialize<'de> for PushSubscription {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(PushSubscriptionVisitor)
    }
}
