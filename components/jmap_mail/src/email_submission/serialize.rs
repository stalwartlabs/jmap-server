use std::{borrow::Cow, collections::HashMap, fmt};

use jmap::request::{ArgumentSerializer, MaybeIdReference};
use serde::{ser::SerializeMap, Deserialize, Serialize};

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
        let mut properties: HashMap<Property, Value> = HashMap::new();

        while let Some(key) = map.next_key::<Cow<str>>()? {
            match key.as_ref() {
                "emailId" => {
                    properties.insert(
                        Property::EmailId,
                        match map.next_value::<MaybeIdReference>()? {
                            MaybeIdReference::Value(value) => Value::Id { value },
                            MaybeIdReference::Reference(value) => Value::IdReference { value },
                        },
                    );
                }
                "identityId" => {
                    properties.insert(
                        Property::IdentityId,
                        match map.next_value::<MaybeIdReference>()? {
                            MaybeIdReference::Value(value) => Value::Id { value },
                            MaybeIdReference::Reference(value) => Value::IdReference { value },
                        },
                    );
                }
                "undoStatus" => {
                    properties.insert(
                        Property::UndoStatus,
                        Value::UndoStatus {
                            value: map.next_value()?,
                        },
                    );
                }
                "envelope" => {
                    properties.insert(
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
                        properties.insert(
                            Property::parse(property),
                            Value::ResultReference {
                                value: map.next_value()?,
                            },
                        );
                    }
                }
                _ => (),
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
impl ArgumentSerializer for SetArguments {
    fn deserialize<'x: 'y, 'y, 'z>(
        &'y mut self,
        property: &'z str,
        value: &mut impl serde::de::MapAccess<'x>,
    ) -> Result<(), String> {
        if property == "onSuccessUpdateEmail" {
            self.on_success_update_email = value.next_value().map_err(|err| err.to_string())?;
        } else if property == "onSuccessDestroyEmail" {
            self.on_success_destroy_email = value.next_value().map_err(|err| err.to_string())?;
        }
        Ok(())
    }
}

// Filter deserializer
struct FilterVisitor;

impl<'de> serde::de::Visitor<'de> for FilterVisitor {
    type Value = Filter;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP e-mail submission filter")
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
                "identityIds" => Filter::IdentityIds {
                    value: map.next_value()?,
                },
                "emailIds" => Filter::EmailIds {
                    value: map.next_value()?,
                },
                "threadIds" => Filter::ThreadIds {
                    value: map.next_value()?,
                },
                "undoStatus" => Filter::UndoStatus {
                    value: map.next_value()?,
                },
                "before" => Filter::Before {
                    value: map.next_value()?,
                },
                "after" => Filter::After {
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
