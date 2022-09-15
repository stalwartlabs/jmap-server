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

use serde::{Deserialize, Serialize};
use store::core::{acl::ACL, vec_map::VecMap};

use crate::{
    orm::acl::ACLUpdate,
    types::{blob::JMAPBlob, jmap::JMAPId},
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Principal {
    pub properties: VecMap<Property, Value>,
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
#[repr(u8)]
pub enum Property {
    Id = 0,
    Type = 1,
    Name = 2,
    Description = 3,
    Email = 4,
    Timezone = 5,
    Capabilities = 6,
    Aliases = 7,
    Secret = 8,
    DKIM = 9,
    Quota = 10,
    Picture = 11,
    Members = 12,
    ACL = 13,
    Invalid = 14,
}

pub const ACCOUNTS_TO_DELETE: u8 = u8::MAX;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Type {
    #[serde(rename = "individual")]
    Individual,
    #[serde(rename = "group")]
    Group,
    #[serde(rename = "resource")]
    Resource,
    #[serde(rename = "location")]
    Location,
    #[serde(rename = "domain")]
    Domain,
    #[serde(rename = "list")]
    List,
    #[serde(rename = "other")]
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DKIM {
    #[serde(rename = "dkimSelector")]
    pub dkim_selector: Option<String>,
    #[serde(rename = "dkimExpiration")]
    pub dkim_expiration: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Id { value: JMAPId },
    Blob { value: JMAPBlob },
    Text { value: String },
    TextList { value: Vec<String> },
    Number { value: i64 },
    Type { value: Type },
    DKIM { value: DKIM },
    Members { value: Vec<JMAPId> },
    ACL(VecMap<String, Vec<ACL>>),
    Patch(Patch),
    Null,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Patch {
    ACL(Vec<ACLUpdate>),
    Members(VecMap<JMAPId, bool>),
    Aliases(VecMap<String, bool>),
}

#[derive(Clone, Debug)]
pub enum Filter {
    Email { value: String },
    Name { value: String },
    DomainName { value: String },
    Text { value: String },
    Type { value: Type },
    Timezone { value: String },
    Members { value: JMAPId },
    QuotaLt { value: u64 },
    QuotaGt { value: u64 },
    Unsupported { value: String },
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "property")]
pub enum Comparator {
    #[serde(rename = "type")]
    Type,
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "email")]
    Email,
}
