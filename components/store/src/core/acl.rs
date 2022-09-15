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

use std::fmt::{self, Display};

use crate::AccountId;

use super::{
    bitmap::{Bitmap, BitmapItem},
    collection::Collection,
};

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Clone, Copy)]
#[repr(u8)]
pub enum ACL {
    Read = 0,
    Modify = 1,
    Delete = 2,
    ReadItems = 3,
    AddItems = 4,
    ModifyItems = 5,
    RemoveItems = 6,
    CreateChild = 7,
    Administer = 8,
    Submit = 9,
    None_ = 10,
}

#[derive(
    Debug, Default, Clone, Eq, PartialEq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct Permission {
    pub id: AccountId,
    pub acl: Bitmap<ACL>,
}

#[derive(Debug)]
pub struct ACLToken {
    pub member_of: Vec<AccountId>,
    pub access_to: Vec<(AccountId, Bitmap<Collection>)>,
}

impl ACL {
    pub fn parse(value: &str) -> ACL {
        match value {
            "read" => ACL::Read,
            "modify" => ACL::Modify,
            "delete" => ACL::Delete,
            "readItems" => ACL::ReadItems,
            "addItems" => ACL::AddItems,
            "modifyItems" => ACL::ModifyItems,
            "removeItems" => ACL::RemoveItems,
            "createChild" => ACL::CreateChild,
            "administer" => ACL::Administer,
            "submit" => ACL::Submit,
            _ => ACL::None_,
        }
    }
}

impl BitmapItem for ACL {
    fn max() -> u64 {
        ACL::None_ as u64
    }

    fn is_valid(&self) -> bool {
        !matches!(self, ACL::None_)
    }
}

impl From<ACL> for u64 {
    fn from(acl: ACL) -> u64 {
        acl as u64
    }
}

impl From<u64> for ACL {
    fn from(value: u64) -> Self {
        match value {
            0 => ACL::Read,
            1 => ACL::Modify,
            2 => ACL::Delete,
            3 => ACL::ReadItems,
            4 => ACL::AddItems,
            5 => ACL::ModifyItems,
            6 => ACL::RemoveItems,
            7 => ACL::CreateChild,
            8 => ACL::Administer,
            9 => ACL::Submit,
            _ => {
                debug_assert!(false, "Invalid ACL value: {}", value);
                ACL::None_
            }
        }
    }
}

impl Display for ACL {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ACL::Read => write!(f, "read"),
            ACL::Modify => write!(f, "modify"),
            ACL::Delete => write!(f, "delete"),
            ACL::ReadItems => write!(f, "readItems"),
            ACL::AddItems => write!(f, "addItems"),
            ACL::ModifyItems => write!(f, "modifyItems"),
            ACL::RemoveItems => write!(f, "removeItems"),
            ACL::CreateChild => write!(f, "createChild"),
            ACL::Administer => write!(f, "administer"),
            ACL::Submit => write!(f, "submit"),
            ACL::None_ => Ok(()),
        }
    }
}

// ACL de/serialization
impl serde::Serialize for ACL {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}
struct ACLVisitor;

impl<'de> serde::de::Visitor<'de> for ACLVisitor {
    type Value = ACL;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid ACL value")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(ACL::parse(v))
    }
}

impl<'de> serde::Deserialize<'de> for ACL {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(ACLVisitor)
    }
}
