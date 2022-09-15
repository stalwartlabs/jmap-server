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

pub mod acl;
pub mod merge;
pub mod serialize;
pub mod tags;
pub mod update;

use store::ahash::AHashSet;
use store::core::acl::Permission;
use store::core::tag::Tag;
use store::core::vec_map::VecMap;
use store::{Integer, LongInteger};

use crate::jmap_store::Object;

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct TinyORM<T>
where
    T: Object,
{
    #[serde(bound(
        serialize = "VecMap<T::Property, T::Value>: serde::Serialize",
        deserialize = "VecMap<T::Property, T::Value>: serde::Deserialize<'de>"
    ))]
    properties: VecMap<T::Property, T::Value>,
    #[serde(bound(
        serialize = "VecMap<T::Property, AHashSet<Tag>>: serde::Serialize",
        deserialize = "VecMap<T::Property, AHashSet<Tag>>: serde::Deserialize<'de>"
    ))]
    tags: VecMap<T::Property, AHashSet<Tag>>,
    acls: Vec<Permission>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub enum Index {
    Text(String),
    TextList(Vec<String>),
    Integer(Integer),
    IntegerList(Vec<Integer>),
    LongInteger(LongInteger),
    Null,
}

impl From<String> for Index {
    fn from(value: String) -> Self {
        Index::Text(value)
    }
}

impl From<Vec<String>> for Index {
    fn from(value: Vec<String>) -> Self {
        Index::TextList(value)
    }
}

impl From<Integer> for Index {
    fn from(value: Integer) -> Self {
        Index::Integer(value)
    }
}

impl From<Vec<Integer>> for Index {
    fn from(value: Vec<Integer>) -> Self {
        Index::IntegerList(value)
    }
}

impl From<LongInteger> for Index {
    fn from(value: LongInteger) -> Self {
        Index::LongInteger(value)
    }
}

pub trait Value
where
    Self: Sized
        + Sync
        + Send
        + Eq
        + PartialEq
        + Default
        + std::fmt::Debug
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>,
{
    fn index_as(&self) -> Index;
    fn is_empty(&self) -> bool;
    fn len(&self) -> usize;
}

impl Value for () {
    fn index_as(&self) -> Index {
        Index::Null
    }

    fn is_empty(&self) -> bool {
        true
    }

    fn len(&self) -> usize {
        0
    }
}

impl<T> Default for TinyORM<T>
where
    T: Object,
{
    fn default() -> Self {
        Self {
            properties: VecMap::new(),
            tags: VecMap::new(),
            acls: Vec::new(),
        }
    }
}

impl<T> TinyORM<T>
where
    T: Object + 'static,
{
    pub const FIELD_ID: u8 = u8::MAX;

    pub fn new() -> Self {
        Self::default()
    }

    pub fn track_changes(source: &TinyORM<T>) -> TinyORM<T> {
        TinyORM {
            properties: VecMap::new(),
            tags: source.tags.clone(),
            acls: source.acls.clone(),
        }
    }

    pub fn get(&self, property: &T::Property) -> Option<&T::Value> {
        self.properties.get(property)
    }

    pub fn get_mut(&mut self, property: &T::Property) -> Option<&mut T::Value> {
        self.properties.get_mut(property)
    }

    /*pub fn entry(&mut self, property: T::Property) -> Entry<'_, T::Property, T::Value> {
        self.properties.entry(property)
    }*/

    pub fn set(&mut self, property: T::Property, value: impl Into<T::Value>) {
        self.properties.set(property, value.into());
    }

    pub fn remove(&mut self, property: &T::Property) -> Option<T::Value> {
        self.properties.remove(property)
    }

    pub fn has_property(&self, property: &T::Property) -> bool {
        self.properties.contains_key(property)
    }
}
