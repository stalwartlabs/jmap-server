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

use store::{ahash::AHashSet, core::tag::Tag};

use super::{Object, TinyORM};

impl<T> TinyORM<T>
where
    T: Object + 'static,
{
    pub fn tag(&mut self, property: T::Property, tag: Tag) {
        self.tags.get_mut_or_insert(property).insert(tag);
    }

    pub fn untag(&mut self, property: &T::Property, tag: &Tag) {
        self.tags.get_mut(property).map(|set| set.remove(tag));
    }

    pub fn untag_all(&mut self, property: &T::Property) {
        if let Some(set) = self.tags.get_mut(property) {
            set.clear()
        }
    }

    pub fn get_tags(&self, property: &T::Property) -> Option<&AHashSet<Tag>> {
        self.tags.get(property)
    }

    pub fn has_tags(&self, property: &T::Property) -> bool {
        self.tags
            .get(property)
            .map(|set| !set.is_empty())
            .unwrap_or(false)
    }

    pub fn get_changed_tags(&self, changes: &Self, property: &T::Property) -> AHashSet<Tag> {
        match (self.tags.get(property), changes.tags.get(property)) {
            (Some(this), Some(changes)) if this != changes => {
                let mut tag_diff = AHashSet::default();
                for tag in this {
                    if !changes.contains(tag) {
                        tag_diff.insert(tag.clone());
                    }
                }
                for tag in changes {
                    if !this.contains(tag) {
                        tag_diff.insert(tag.clone());
                    }
                }
                tag_diff
            }
            (Some(this), None) => this.clone(),
            (None, Some(changes)) => changes.clone(),
            _ => AHashSet::default(),
        }
    }

    pub fn get_added_tags(&self, changes: &Self, property: &T::Property) -> AHashSet<Tag> {
        match (self.tags.get(property), changes.tags.get(property)) {
            (Some(this), Some(changes)) if this != changes => {
                let mut tag_diff = AHashSet::default();
                for tag in changes {
                    if !this.contains(tag) {
                        tag_diff.insert(tag.clone());
                    }
                }
                tag_diff
            }
            (None, Some(changes)) => changes.clone(),
            _ => AHashSet::default(),
        }
    }

    pub fn get_removed_tags(&self, changes: &Self, property: &T::Property) -> AHashSet<Tag> {
        match (self.tags.get(property), changes.tags.get(property)) {
            (Some(this), Some(changes)) if this != changes => {
                let mut tag_diff = AHashSet::default();
                for tag in this {
                    if !changes.contains(tag) {
                        tag_diff.insert(tag.clone());
                    }
                }
                tag_diff
            }
            (Some(this), None) => this.clone(),
            _ => AHashSet::default(),
        }
    }
}
