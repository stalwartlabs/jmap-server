use std::{borrow::Cow, fmt};

use serde::{de::IgnoredAny, ser::SerializeMap, Deserialize, Serialize};
use store::core::{acl::ACL, vec_map::VecMap};

use crate::{
    orm::acl::ACLUpdate,
    types::{blob::JMAPBlob, jmap::JMAPId, json_pointer::JSONPointer},
};

use super::schema::{Filter, Patch, Principal, Property, Type, Value, DKIM};

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
                Value::ACL(value) => map.serialize_entry(name, value)?,
                Value::Patch(_) => (),
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
        let mut properties: VecMap<Property, Value> = VecMap::new();
        let mut acls = Vec::new();
        let mut patch_members = VecMap::new();
        let mut patch_aliases = VecMap::new();

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
                "description" => {
                    properties.append(
                        Property::Description,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "timezone" => {
                    properties.append(
                        Property::Timezone,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "email" => {
                    properties.append(
                        Property::Email,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "aliases" => {
                    properties.append(
                        Property::Aliases,
                        if let Some(value) = map.next_value::<Option<Vec<String>>>()? {
                            Value::TextList { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "capabilities" => {
                    properties.append(
                        Property::Capabilities,
                        if let Some(value) = map.next_value::<Option<Vec<String>>>()? {
                            Value::TextList { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "type" => {
                    properties.append(
                        Property::Type,
                        Value::Type {
                            value: map.next_value::<Type>()?,
                        },
                    );
                }
                "secret" => {
                    properties.append(
                        Property::Secret,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "dkim" => {
                    properties.append(
                        Property::DKIM,
                        if let Some(value) = map.next_value::<Option<DKIM>>()? {
                            Value::DKIM { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "members" => {
                    properties.append(
                        Property::Members,
                        if let Some(value) = map.next_value::<Option<Vec<JMAPId>>>()? {
                            Value::Members { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "quota" => {
                    properties.append(
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
                    properties.append(
                        Property::Picture,
                        if let Some(value) = map.next_value::<Option<JMAPBlob>>()? {
                            Value::Blob { value }
                        } else {
                            Value::Null
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
                key => {
                    match JSONPointer::parse(key) {
                        Some(JSONPointer::Path(path)) if path.len() >= 2 => {
                            match (
                                path.get(0)
                                    .and_then(|p| p.to_string())
                                    .map(Property::parse)
                                    .unwrap_or(Property::Invalid),
                                path.get(1).and_then(|p| p.to_string()),
                            ) {
                                (Property::ACL, Some(account_id)) => {
                                    if path.len() > 2 {
                                        if let Some(acl) =
                                            path.get(2).and_then(|p| p.to_string()).map(ACL::parse)
                                        {
                                            if acl != ACL::None_ {
                                                acls.push(ACLUpdate::Set {
                                                    account_id: account_id.to_string(),
                                                    acl,
                                                    is_set: map
                                                        .next_value::<Option<bool>>()?
                                                        .unwrap_or(false),
                                                });
                                            }
                                        }
                                    } else {
                                        acls.push(ACLUpdate::Update {
                                            account_id: account_id.to_string(),
                                            acls: map
                                                .next_value::<Option<Vec<ACL>>>()?
                                                .unwrap_or_default(),
                                        });
                                    }
                                    continue;
                                }
                                (Property::Aliases, Some(alias)) => {
                                    patch_aliases.append(
                                        alias.to_string(),
                                        map.next_value::<Option<bool>>()?.unwrap_or(false),
                                    );
                                    continue;
                                }
                                (Property::Members, Some(account_id)) => {
                                    if let Some(account_id) = JMAPId::parse(account_id) {
                                        patch_members.append(
                                            account_id,
                                            map.next_value::<Option<bool>>()?.unwrap_or(false),
                                        );
                                        continue;
                                    }
                                }
                                _ => (),
                            }
                        }
                        _ => (),
                    }
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        if !acls.is_empty() {
            properties.append(Property::ACL, Value::Patch(Patch::ACL(acls)));
        }

        if !patch_aliases.is_empty() {
            properties.append(
                Property::Aliases,
                Value::Patch(Patch::Aliases(patch_aliases)),
            );
        }

        if !patch_members.is_empty() {
            properties.append(
                Property::Members,
                Value::Patch(Patch::Members(patch_members)),
            );
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
                "domainName" => Filter::DomainName {
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
                "members" => Filter::Members {
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
