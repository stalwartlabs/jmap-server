use crate::id::jmap::JMAPId;
use core::hash::Hash;
use std::fmt::Debug;
use store::core::collection::Collection;

pub mod blob;
pub mod changes;
pub mod get;
pub mod orm;
pub mod query;
pub mod query_changes;
pub mod raft;
pub mod set;

pub trait Object: Sized + for<'de> serde::Deserialize<'de> + serde::Serialize {
    type Property: for<'de> serde::Deserialize<'de>
        + serde::Serialize
        + Eq
        + PartialEq
        + Debug
        + Hash
        + Clone
        + Into<u8>
        + Sync
        + Send;
    type Value: for<'de> serde::Deserialize<'de>
        + serde::Serialize
        + Sync
        + Send
        + Eq
        + PartialEq
        + Debug;

    fn id(&self) -> Option<&JMAPId>;
    fn required() -> &'static [Self::Property];
    fn indexed() -> &'static [(Self::Property, u64)];
    fn collection() -> Collection;
    fn hide_account() -> bool;
}
