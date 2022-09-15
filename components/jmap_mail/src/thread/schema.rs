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

use jmap::types::jmap::JMAPId;
use serde::{Deserialize, Serialize};
use store::FieldId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Thread {
    pub id: JMAPId,
    #[serde(rename = "emailIds")]
    pub email_ids: Vec<JMAPId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[repr(u8)]
pub enum Property {
    #[serde(rename = "id")]
    Id = 0,
    #[serde(rename = "emailIds")]
    EmailIds = 1,
}

impl Property {
    pub fn parse(value: &str) -> Self {
        match value {
            "id" => Property::Id,
            "emailIds" => Property::EmailIds,
            _ => Property::Id,
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
            _ => Property::EmailIds,
        }
    }
}

impl TryFrom<&str> for Property {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Property::parse(value))
    }
}
