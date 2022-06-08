use crate::{core::bitmap::Bitmap, AccountId};

pub mod manage;
pub mod permission;
pub mod util;

#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy)]
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
    None_ = 9,
}

#[derive(Debug, Default)]
pub struct Permission {
    pub id: AccountId,
    pub acl: Bitmap<ACL>,
}
