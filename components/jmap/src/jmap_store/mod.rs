use crate::{orm, types::jmap::JMAPId};
use core::hash::Hash;
use std::fmt::Debug;
use store::core::collection::Collection;

pub mod blob;
pub mod changes;
pub mod copy;
pub mod get;
pub mod query;
pub mod query_changes;
pub mod raft;
pub mod set;

pub trait Object: Sized + for<'de> serde::Deserialize<'de> + serde::Serialize {
    type Property: for<'de> serde::Deserialize<'de>
        + serde::Serialize
        + for<'x> TryFrom<&'x str>
        + From<u8>
        + Into<u8>
        + Eq
        + PartialEq
        + Debug
        + Hash
        + Clone
        + Sync
        + Send;
    type Value: orm::Value;

    fn new(id: JMAPId) -> Self;
    fn id(&self) -> Option<&JMAPId>;
    fn required() -> &'static [Self::Property];
    fn indexed() -> &'static [(Self::Property, u64)];
    fn collection() -> Collection;
}
