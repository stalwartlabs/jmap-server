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

use jmap::{
    orm,
    types::{blob::JMAPBlob, jmap::JMAPId},
};
use serde::{Deserialize, Serialize};
use store::{core::vec_map::VecMap, sieve::Sieve, FieldId};

use crate::{SeenIdHash, SeenIds};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SieveScript {
    pub properties: VecMap<Property, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
#[repr(u8)]
pub enum Property {
    Id = 0,
    Name = 1,
    BlobId = 2,
    IsActive = 3,
    CompiledScript = 4,
    SeenIds = 5,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Id { value: JMAPId },
    Text { value: String },
    BlobId { value: JMAPBlob },
    Bool { value: bool },
    CompiledScript { value: CompiledScript },
    SeenIds { value: SeenIds },
    Null,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledScript {
    pub version: u32,
    pub script: Option<Sieve>,
}

impl Property {
    pub fn parse(value: &str) -> Property {
        match value {
            "id" => Property::Id,
            "name" => Property::Name,
            "blobId" => Property::BlobId,
            "isActive" => Property::IsActive,
            _ => Property::CompiledScript,
        }
    }
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::Name => write!(f, "name"),
            Property::BlobId => write!(f, "blobId"),
            Property::IsActive => write!(f, "isActive"),
            Property::CompiledScript | Property::SeenIds => Ok(()),
        }
    }
}

impl From<Property> for FieldId {
    fn from(property: Property) -> Self {
        property as FieldId
    }
}

impl From<FieldId> for Property {
    fn from(field: FieldId) -> Self {
        match field {
            0 => Property::Id,
            1 => Property::Name,
            2 => Property::BlobId,
            3 => Property::IsActive,
            _ => Property::CompiledScript,
        }
    }
}

impl TryFrom<&str> for Property {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match Property::parse(value) {
            Property::CompiledScript => Err(()),
            property => Ok(property),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Filter {
    Name { value: String },
    IsActive { value: bool },
    Unsupported { value: String },
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "property")]
pub enum Comparator {
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "isActive")]
    IsActive,
}

impl orm::Value for Value {
    fn index_as(&self) -> orm::Index {
        match self {
            Value::Id { value } => u64::from(value).into(),
            Value::Text { value } => value.to_string().into(),
            Value::Bool { value } => (if *value { "1" } else { "0" }).to_string().into(),
            _ => orm::Index::Null,
        }
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
            Value::BlobId { .. } => std::mem::size_of::<JMAPBlob>(),
            Value::Bool { .. } => std::mem::size_of::<bool>(),
            Value::CompiledScript { .. } => std::mem::size_of::<CompiledScript>(),
            Value::SeenIds { value } => std::mem::size_of::<SeenIdHash>() * value.ids.len(),
            Value::Null => 0,
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}
