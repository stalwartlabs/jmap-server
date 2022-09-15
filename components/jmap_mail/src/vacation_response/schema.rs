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
    types::{date::JMAPDate, jmap::JMAPId},
};
use serde::{Deserialize, Serialize};
use store::{ahash::AHashSet, core::vec_map::VecMap, FieldId};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VacationResponse {
    pub properties: VecMap<Property, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Id { value: JMAPId },
    Text { value: String },
    Bool { value: bool },
    DateTime { value: JMAPDate },
    SentResponses { value: AHashSet<String> },
    Null,
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
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
            Value::Bool { .. } => std::mem::size_of::<bool>(),
            Value::DateTime { .. } => std::mem::size_of::<JMAPDate>(),
            Value::SentResponses { value } => value.iter().fold(0, |acc, x| acc + x.len()),
            Value::Null => 0,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
#[repr(u8)]
pub enum Property {
    Id = 0,
    IsEnabled = 1,
    FromDate = 2,
    ToDate = 3,
    Subject = 4,
    TextBody = 5,
    HtmlBody = 6,
    SentResponses_ = 7,
}

impl Property {
    pub fn parse(value: &str) -> Self {
        match value {
            "id" => Property::Id,
            "isEnabled" => Property::IsEnabled,
            "fromDate" => Property::FromDate,
            "toDate" => Property::ToDate,
            "subject" => Property::Subject,
            "textBody" => Property::TextBody,
            "htmlBody" => Property::HtmlBody,
            _ => Property::SentResponses_,
        }
    }
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::IsEnabled => write!(f, "isEnabled"),
            Property::FromDate => write!(f, "fromDate"),
            Property::ToDate => write!(f, "toDate"),
            Property::Subject => write!(f, "subject"),
            Property::TextBody => write!(f, "textBody"),
            Property::HtmlBody => write!(f, "htmlBody"),
            Property::SentResponses_ => Ok(()),
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
            1 => Property::IsEnabled,
            2 => Property::FromDate,
            3 => Property::ToDate,
            4 => Property::Subject,
            5 => Property::TextBody,
            6 => Property::HtmlBody,
            _ => Property::SentResponses_,
        }
    }
}

impl TryFrom<&str> for Property {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match Property::parse(value) {
            Property::SentResponses_ => Err(()),
            property => Ok(property),
        }
    }
}
