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
    SetSeen = 9,
    SetKeywords = 10,
    Submit = 11,
    None_ = 12,
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
            "setSeen" => ACL::SetSeen,
            "setKeywords" => ACL::SetKeywords,
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
            9 => ACL::SetSeen,
            10 => ACL::SetKeywords,
            11 => ACL::Submit,
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
            ACL::SetSeen => write!(f, "setSeen"),
            ACL::SetKeywords => write!(f, "setKeywords"),
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
