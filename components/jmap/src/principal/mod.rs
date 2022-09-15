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

use ::store::FieldId;

use self::schema::Property;

pub mod orm;
pub mod raft;
pub mod schema;
pub mod serialize;
pub mod store;

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => f.write_str("id"),
            Property::Type => f.write_str("type"),
            Property::Name => f.write_str("name"),
            Property::Description => f.write_str("description"),
            Property::Email => f.write_str("email"),
            Property::Timezone => f.write_str("timezone"),
            Property::Capabilities => f.write_str("capabilities"),
            Property::Secret => f.write_str("secret"),
            Property::DKIM => f.write_str("dkim"),
            Property::Quota => f.write_str("quota"),
            Property::Picture => f.write_str("picture"),
            Property::Members => f.write_str("members"),
            Property::Aliases => f.write_str("aliases"),
            Property::ACL => f.write_str("acl"),
            Property::Invalid => Ok(()),
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
            1 => Property::Type,
            2 => Property::Name,
            3 => Property::Description,
            4 => Property::Email,
            5 => Property::Timezone,
            6 => Property::Capabilities,
            7 => Property::Aliases,
            8 => Property::Secret,
            9 => Property::DKIM,
            10 => Property::Quota,
            11 => Property::Picture,
            12 => Property::Members,
            13 => Property::ACL,
            _ => Property::Invalid,
        }
    }
}

impl TryFrom<&str> for Property {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match Property::parse(value) {
            Property::Invalid => Err(()),
            property => Ok(property),
        }
    }
}

impl Property {
    pub fn parse(value: &str) -> Self {
        match value {
            "id" => Property::Id,
            "type" => Property::Type,
            "name" => Property::Name,
            "description" => Property::Description,
            "email" => Property::Email,
            "timezone" => Property::Timezone,
            "capabilities" => Property::Capabilities,
            "secret" => Property::Secret,
            "aliases" => Property::Aliases,
            "dkim" => Property::DKIM,
            "quota" => Property::Quota,
            "picture" => Property::Picture,
            "members" => Property::Members,
            "acl" => Property::ACL,
            _ => Property::Invalid,
        }
    }
}
