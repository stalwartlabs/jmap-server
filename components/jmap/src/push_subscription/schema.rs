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

use std::fmt::Display;

use serde::{Deserialize, Serialize};
use store::{core::vec_map::VecMap, FieldId};

use crate::{
    orm,
    types::{date::JMAPDate, jmap::JMAPId},
    types::{state::JMAPState, type_state::TypeState},
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PushSubscription {
    pub properties: VecMap<Property, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Keys {
    pub p256dh: String,
    pub auth: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Value {
    Id { value: JMAPId },
    Text { value: String },
    DateTime { value: JMAPDate },
    Types { value: Vec<TypeState> },
    Keys { value: Keys },
    Null,
}

impl Value {
    pub fn unwrap_text(self) -> Option<String> {
        match self {
            Value::Text { value } => Some(value),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text { value } => Some(value),
            _ => None,
        }
    }

    pub fn as_timestamp(&self) -> Option<i64> {
        match self {
            Value::DateTime { value } => Some(value.timestamp()),
            _ => None,
        }
    }
}

impl orm::Value for Value {
    fn index_as(&self) -> orm::Index {
        orm::Index::Null
    }

    fn is_empty(&self) -> bool {
        match self {
            Value::Text { value } => value.is_empty(),
            Value::Null => true,
            _ => false,
        }
    }

    fn len(&self) -> usize {
        match self {
            Value::Id { .. } => std::mem::size_of::<JMAPId>(),
            Value::Text { value } => value.len(),
            Value::DateTime { .. } => std::mem::size_of::<JMAPState>(),
            Value::Types { value } => value.len() * std::mem::size_of::<TypeState>(),
            Value::Keys { value } => value.auth.len() + value.p256dh.len(),
            Value::Null => 0,
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[repr(u8)]
pub enum Property {
    Id = 0,
    DeviceClientId = 1,
    Url = 2,
    Keys = 3,
    VerificationCode = 4,
    Expires = 5,
    Types = 6,
    VerificationCode_ = 7,
}

impl Property {
    pub fn parse(value: &str) -> Self {
        match value {
            "id" => Property::Id,
            "deviceClientId" => Property::DeviceClientId,
            "url" => Property::Url,
            "keys" => Property::Keys,
            "verificationCode" => Property::VerificationCode,
            "expires" => Property::Expires,
            "types" => Property::Types,
            _ => Property::VerificationCode_,
        }
    }
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::DeviceClientId => write!(f, "deviceClientId"),
            Property::Url => write!(f, "url"),
            Property::Keys => write!(f, "keys"),
            Property::VerificationCode => write!(f, "verificationCode"),
            Property::Expires => write!(f, "expires"),
            Property::Types => write!(f, "types"),
            Property::VerificationCode_ => Ok(()),
        }
    }
}

impl From<Property> for FieldId {
    fn from(field: Property) -> Self {
        field as FieldId
    }
}

impl From<FieldId> for Property {
    fn from(field: FieldId) -> Self {
        match field {
            0 => Property::Id,
            1 => Property::DeviceClientId,
            2 => Property::Url,
            3 => Property::Keys,
            4 => Property::VerificationCode,
            5 => Property::Expires,
            6 => Property::Types,
            7 => Property::VerificationCode_,
            _ => Property::VerificationCode_,
        }
    }
}

impl TryFrom<&str> for Property {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match Property::parse(value) {
            Property::VerificationCode_ => Err(()),
            property => Ok(property),
        }
    }
}
