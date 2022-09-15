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

use jmap::types::date::JMAPDate;
use serde::{ser::SerializeMap, Deserialize, Serialize};
use store::core::vec_map::VecMap;

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
        let mut properties: VecMap<Property, Value> = VecMap::new();

        while let Some(key) = map.next_key::<Cow<str>>()? {
            match key.as_ref() {
                "subject" => {
                    properties.append(
                        Property::Subject,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "textBody" => {
                    properties.append(
                        Property::TextBody,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "htmlBody" => {
                    properties.append(
                        Property::HtmlBody,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "isEnabled" => {
                    properties.append(
                        Property::IsEnabled,
                        Value::Bool {
                            value: map.next_value::<Option<bool>>()?.unwrap_or(false),
                        },
                    );
                }
                "fromDate" => {
                    properties.append(
                        Property::FromDate,
                        if let Some(value) = map.next_value::<Option<JMAPDate>>()? {
                            Value::DateTime { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "toDate" => {
                    properties.append(
                        Property::ToDate,
                        if let Some(value) = map.next_value::<Option<JMAPDate>>()? {
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
