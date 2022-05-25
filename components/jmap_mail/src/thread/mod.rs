use jmap::{id::jmap::JMAPId, jmap_store::Object};
use store::core::collection::Collection;

use self::schema::{Property, Thread};

pub mod changes;
pub mod get;
pub mod schema;

impl Object for Thread {
    type Property = Property;

    type Value = ();

    fn new(id: JMAPId) -> Self {
        Thread {
            id,
            email_ids: Vec::new(),
        }
    }

    fn id(&self) -> Option<&JMAPId> {
        Some(&self.id)
    }

    fn required() -> &'static [Self::Property] {
        &[]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[]
    }

    fn collection() -> store::core::collection::Collection {
        Collection::Thread
    }
}
