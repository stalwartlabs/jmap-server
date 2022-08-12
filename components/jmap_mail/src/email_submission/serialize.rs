use std::{borrow::Cow, fmt};

use jmap::request::{query::FilterDeserializer, ArgumentDeserializer, MaybeIdReference};
use serde::{de::IgnoredAny, ser::SerializeMap, Deserialize, Serialize};
use store::core::vec_map::VecMap;

use super::{
    schema::{EmailSubmission, Envelope, Filter, Property, Value},
    set::SetArguments,
};

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
        formatter.write_str("a valid JMAP EmailSubmission property")
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

// EmailSubmission de/serialization
impl Serialize for EmailSubmission {
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
                Value::ResultReference { value } => map.serialize_entry(name, value)?,
                Value::IdReference { value } => {
                    map.serialize_entry(name, &format!("#{}", value))?
                }
                Value::DateTime { value } => map.serialize_entry(name, value)?,
                Value::UndoStatus { value } => map.serialize_entry(name, value)?,
                Value::DeliveryStatus { value } => map.serialize_entry(name, value)?,
                Value::BlobIds { value } => map.serialize_entry(name, value)?,
                Value::Envelope { value } => map.serialize_entry(name, value)?,
            }
        }

        map.end()
    }
}

struct EmailSubmissionVisitor;

impl<'de> serde::de::Visitor<'de> for EmailSubmissionVisitor {
    type Value = EmailSubmission;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP EmailSubmission object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut properties: VecMap<Property, Value> = VecMap::new();

        while let Some(key) = map.next_key::<Cow<str>>()? {
            match key.as_ref() {
                "emailId" => {
                    properties.append(
                        Property::EmailId,
                        match map.next_value::<MaybeIdReference>()? {
                            MaybeIdReference::Value(value) => Value::Id { value },
                            MaybeIdReference::Reference(value) => Value::IdReference { value },
                        },
                    );
                }
                "identityId" => {
                    properties.append(
                        Property::IdentityId,
                        match map.next_value::<MaybeIdReference>()? {
                            MaybeIdReference::Value(value) => Value::Id { value },
                            MaybeIdReference::Reference(value) => Value::IdReference { value },
                        },
                    );
                }
                "undoStatus" => {
                    properties.append(
                        Property::UndoStatus,
                        Value::UndoStatus {
                            value: map.next_value()?,
                        },
                    );
                }
                "envelope" => {
                    properties.append(
                        Property::Envelope,
                        if let Some(value) = map.next_value::<Option<Envelope>>()? {
                            Value::Envelope { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                _ if key.starts_with('#') => {
                    if let Some(property) = key.get(1..) {
                        properties.append(
                            Property::parse(property),
                            Value::ResultReference {
                                value: map.next_value()?,
                            },
                        );
                    } else {
                        map.next_value::<IgnoredAny>()?;
                    }
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        Ok(EmailSubmission { properties })
    }
}

impl<'de> Deserialize<'de> for EmailSubmission {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(EmailSubmissionVisitor)
    }
}

// Argument serializer
impl ArgumentDeserializer for SetArguments {
    fn deserialize<'x: 'y, 'y, 'z>(
        &'y mut self,
        property: &'z str,
        value: &mut impl serde::de::MapAccess<'x>,
    ) -> Result<(), String> {
        if property == "onSuccessUpdateEmail" {
            self.on_success_update_email = value.next_value().map_err(|err| err.to_string())?;
        } else if property == "onSuccessDestroyEmail" {
            self.on_success_destroy_email = value.next_value().map_err(|err| err.to_string())?;
        } else {
            value
                .next_value::<IgnoredAny>()
                .map_err(|err| err.to_string())?;
        }
        Ok(())
    }
}

// Filter deserializer
impl FilterDeserializer for Filter {
    fn deserialize<'x>(property: &str, map: &mut impl serde::de::MapAccess<'x>) -> Option<Self> {
        match property {
            "identityIds" => Filter::IdentityIds {
                value: map.next_value().ok()?,
            },
            "emailIds" => Filter::EmailIds {
                value: map.next_value().ok()?,
            },
            "threadIds" => Filter::ThreadIds {
                value: map.next_value().ok()?,
            },
            "undoStatus" => Filter::UndoStatus {
                value: map.next_value().ok()?,
            },
            "before" => Filter::Before {
                value: map.next_value().ok()?,
            },
            "after" => Filter::After {
                value: map.next_value().ok()?,
            },
            unsupported => {
                map.next_value::<IgnoredAny>().ok()?;
                Filter::Unsupported {
                    value: unsupported.to_string(),
                }
            }
        }
        .into()
    }
}
