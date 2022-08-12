use std::{borrow::Cow, fmt};

use jmap::{
    orm::acl::ACLUpdate,
    request::{query::FilterDeserializer, ArgumentDeserializer, MaybeIdReference},
    types::json_pointer::JSONPointer,
};
use serde::{de::IgnoredAny, ser::SerializeMap, Deserialize, Serialize};
use store::core::{acl::ACL, vec_map::VecMap};

use super::{
    schema::{Filter, Mailbox, Property, Value},
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
                Value::Id { value } => map.serialize_entry(name, value)?,
                Value::Text { value } => map.serialize_entry(name, value)?,
                Value::Bool { value } => map.serialize_entry(name, value)?,
                Value::Number { value } => map.serialize_entry(name, value)?,
                Value::MailboxRights { value } => map.serialize_entry(name, value)?,
                Value::Null => map.serialize_entry(name, &None::<&str>)?,
                Value::ResultReference { value } => map.serialize_entry(name, value)?,
                Value::IdReference { value } => {
                    map.serialize_entry(name, &format!("#{}", value))?
                }
                Value::ACLGet(value) => map.serialize_entry(name, value)?,
                Value::Subscriptions { .. } | Value::ACLSet(_) => (),
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
        let mut properties: VecMap<Property, Value> = VecMap::new();
        let mut acls = Vec::new();

        while let Some(key) = map.next_key::<Cow<str>>()? {
            match key.as_ref() {
                "name" => {
                    properties.append(
                        Property::Name,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "parentId" => {
                    properties.append(
                        Property::ParentId,
                        if let Some(value) = map.next_value::<Option<MaybeIdReference>>()? {
                            match value {
                                MaybeIdReference::Value(value) => Value::Id { value },
                                MaybeIdReference::Reference(value) => Value::IdReference { value },
                            }
                        } else {
                            Value::Null
                        },
                    );
                }
                "role" => {
                    properties.append(
                        Property::Role,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "sortOrder" => {
                    properties.append(
                        Property::SortOrder,
                        if let Some(value) = map.next_value::<Option<u32>>()? {
                            Value::Number { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "isSubscribed" => {
                    properties.append(
                        Property::IsSubscribed,
                        Value::Bool {
                            value: map.next_value::<Option<bool>>()?.unwrap_or(false),
                        },
                    );
                }
                "acl" => {
                    acls.push(ACLUpdate::Replace {
                        acls: map
                            .next_value::<Option<VecMap<String, Vec<ACL>>>>()?
                            .unwrap_or_default(),
                    });
                }
                _ if key.starts_with('#') => {
                    if let Some(property) = key.get(1..) {
                        properties.append(
                            Property::parse(property),
                            Value::ResultReference {
                                value: map.next_value()?,
                            },
                        );
                    }
                }
                key => match JSONPointer::parse(key) {
                    Some(JSONPointer::Path(path))
                        if path.len() >= 2
                            && path
                                .get(0)
                                .and_then(|p| p.to_string())
                                .map(Property::parse)
                                .unwrap_or(Property::Invalid)
                                == Property::ACL =>
                    {
                        if let Some(account_id) = path
                            .get(1)
                            .and_then(|p| p.to_string())
                            .map(|p| p.to_string())
                        {
                            if path.len() > 2 {
                                if let Some(acl) =
                                    path.get(2).and_then(|p| p.to_string()).map(ACL::parse)
                                {
                                    if acl != ACL::None_ {
                                        acls.push(ACLUpdate::Set {
                                            account_id,
                                            acl,
                                            is_set: map
                                                .next_value::<Option<bool>>()?
                                                .unwrap_or(false),
                                        });
                                    }
                                }
                            } else {
                                acls.push(ACLUpdate::Update {
                                    account_id,
                                    acls: map.next_value::<Option<Vec<ACL>>>()?.unwrap_or_default(),
                                });
                            }
                        } else {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                    _ => {
                        map.next_value::<IgnoredAny>()?;
                    }
                },
            }
        }

        if !acls.is_empty() {
            properties.append(Property::ACL, Value::ACLSet(acls));
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

// Argument serializer
impl ArgumentDeserializer for SetArguments {
    fn deserialize<'x: 'y, 'y, 'z>(
        &'y mut self,
        property: &'z str,
        value: &mut impl serde::de::MapAccess<'x>,
    ) -> Result<(), String> {
        if property == "onDestroyRemoveEmails" {
            self.on_destroy_remove_emails = value.next_value().map_err(|err| err.to_string())?;
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
            "parentId" => Filter::ParentId {
                value: map.next_value().ok()?,
            },
            "name" => Filter::Name {
                value: map.next_value().ok()?,
            },
            "role" => Filter::Role {
                value: map.next_value().ok()?,
            },
            "hasAnyRole" => Filter::HasAnyRole {
                value: map.next_value().ok()?,
            },
            "isSubscribed" => Filter::IsSubscribed {
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
