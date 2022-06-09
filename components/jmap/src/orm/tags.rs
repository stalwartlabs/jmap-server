use store::core::tag::Tag;

use std::collections::HashSet;

use super::{Object, TinyORM};

impl<T> TinyORM<T>
where
    T: Object + 'static,
{
    pub fn tag(&mut self, property: T::Property, tag: Tag) {
        self.tags
            .entry(property)
            .or_insert_with(HashSet::new)
            .insert(tag);
    }

    pub fn untag(&mut self, property: &T::Property, tag: &Tag) {
        self.tags.get_mut(property).map(|set| set.remove(tag));
    }

    pub fn untag_all(&mut self, property: &T::Property) {
        if let Some(set) = self.tags.get_mut(property) {
            set.clear()
        }
    }

    pub fn get_tags(&self, property: &T::Property) -> Option<&HashSet<Tag>> {
        self.tags.get(property)
    }

    pub fn has_tags(&self, property: &T::Property) -> bool {
        self.tags
            .get(property)
            .map(|set| !set.is_empty())
            .unwrap_or(false)
    }

    pub fn get_changed_tags(&self, changes: &Self, property: &T::Property) -> HashSet<Tag> {
        match (self.tags.get(property), changes.tags.get(property)) {
            (Some(this), Some(changes)) if this != changes => {
                let mut tag_diff = HashSet::new();
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
            _ => HashSet::with_capacity(0),
        }
    }
}
