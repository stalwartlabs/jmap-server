/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use std::{borrow::Cow, fmt};

use jmap::request::{query::FilterDeserializer, ArgumentDeserializer, MaybeIdReference};
use serde::{de::IgnoredAny, ser::SerializeMap, Deserialize, Serialize};
use store::core::vec_map::VecMap;

use super::{
    schema::{Filter, Property, SieveScript, Value},
    set::{ActivateScript, SetArguments},
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
        formatter.write_str("a valid JMAP SieveScript property")
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

// SieveScript de/serialization
impl Serialize for SieveScript {
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
                Value::BlobId { value } => map.serialize_entry(name, value)?,
                Value::Null => map.serialize_entry(name, &None::<&str>)?,
                Value::CompiledScript { .. } => (),
            }
        }

        map.end()
    }
}

struct SieveScriptVisitor;

impl<'de> serde::de::Visitor<'de> for SieveScriptVisitor {
    type Value = SieveScript;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP SieveScript object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut properties: VecMap<Property, Value> = VecMap::new();

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
                "blobId" => {
                    properties.append(
                        Property::BlobId,
                        Value::BlobId {
                            value: map.next_value()?,
                        },
                    );
                }
                "isActive" => {
                    properties.append(
                        Property::IsActive,
                        Value::Bool {
                            value: map.next_value::<Option<bool>>()?.unwrap_or(false),
                        },
                    );
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        Ok(SieveScript { properties })
    }
}

impl<'de> Deserialize<'de> for SieveScript {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(SieveScriptVisitor)
    }
}

// Argument serializer
impl ArgumentDeserializer for SetArguments {
    fn deserialize<'x: 'y, 'y, 'z>(
        &'y mut self,
        property: &'z str,
        value: &mut impl serde::de::MapAccess<'x>,
    ) -> Result<(), String> {
        if property == "onSuccessActivateScript" {
            self.on_success_activate_script = match value
                .next_value::<Option<MaybeIdReference>>()
                .map_err(|err| err.to_string())?
            {
                Some(id_ref) => ActivateScript::Activate(id_ref),
                None => ActivateScript::None,
            };
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
            "name" => Filter::Name {
                value: map.next_value().ok()?,
            },
            "isActive" => Filter::IsActive {
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
