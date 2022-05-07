use std::ops::Deref;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum Collection {
    Account = 0,
    PushSubscription = 1,
    Mail = 2,
    Mailbox = 3,
    Thread = 4,
    Identity = 5,
    EmailSubmission = 6,
    VacationResponse = 7,
    None = 8,
}

impl Default for Collection {
    fn default() -> Self {
        Collection::None
    }
}

impl From<u8> for Collection {
    fn from(value: u8) -> Self {
        match value {
            0 => Collection::Account,
            1 => Collection::PushSubscription,
            2 => Collection::Mail,
            3 => Collection::Mailbox,
            4 => Collection::Thread,
            5 => Collection::Identity,
            6 => Collection::EmailSubmission,
            7 => Collection::VacationResponse,
            _ => {
                debug_assert!(false, "Invalid collection value: {}", value);
                Collection::None
            }
        }
    }
}

impl From<Collection> for u8 {
    fn from(collection: Collection) -> u8 {
        collection as u8
    }
}

impl From<Collection> for u64 {
    fn from(collection: Collection) -> u64 {
        collection as u64
    }
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct Collections {
    pub collections: u64,
}

impl Collections {
    pub fn all() -> Self {
        Self {
            collections: u64::MAX >> (64 - (Collection::None as u64)),
        }
    }

    pub fn union(&mut self, items: &Collections) {
        self.collections |= items.collections;
    }

    pub fn insert(&mut self, item: Collection) {
        debug_assert_ne!(item, Collection::None);
        self.collections |= 1 << item as u64;
    }

    pub fn pop(&mut self) -> Option<Collection> {
        if self.collections != 0 {
            let collection_id = 63 - self.collections.leading_zeros();
            self.collections ^= 1 << collection_id;
            Some(Collection::from(collection_id as u8))
        } else {
            None
        }
    }

    pub fn contains(&self, item: Collection) -> bool {
        self.collections & (1 << item as u64) != 0
    }

    pub fn is_empty(&self) -> bool {
        self.collections == 0
    }

    pub fn clear(&mut self) -> Self {
        let collections = self.collections;
        self.collections = 0;
        Collections { collections }
    }
}

impl From<u64> for Collections {
    fn from(value: u64) -> Self {
        Self { collections: value }
    }
}

impl AsRef<u64> for Collections {
    fn as_ref(&self) -> &u64 {
        &self.collections
    }
}

impl Deref for Collections {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.collections
    }
}

impl Iterator for Collections {
    type Item = Collection;

    fn next(&mut self) -> Option<Self::Item> {
        if self.collections != 0 {
            let collection_id = 63 - self.collections.leading_zeros();
            self.collections ^= 1 << collection_id;
            Some(Collection::from(collection_id as u8))
        } else {
            None
        }
    }
}
