pub mod changes;
pub mod get;
pub mod query;
pub mod raft;
pub mod schema;
pub mod serialize;
pub mod set;

use jmap::types::jmap::JMAPId;
use jmap::{jmap_store::Object, orm::TinyORM};

use store::core::collection::Collection;
use store::core::tag::Tag;
use store::write::options::Options;

use self::schema::{Mailbox, Property, Value};

impl Object for Mailbox {
    type Property = Property;

    type Value = Value;

    fn id(&self) -> Option<&JMAPId> {
        self.properties.get(&Property::Id).and_then(|id| match id {
            Value::Id { value } => Some(value),
            _ => None,
        })
    }

    fn required() -> &'static [Self::Property] {
        &[Property::Name]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[
            (
                Property::Name,
                <u64 as Options>::F_TOKENIZE | <u64 as Options>::F_INDEX,
            ),
            (Property::Role, <u64 as Options>::F_KEYWORD),
            (Property::ParentId, <u64 as Options>::F_INDEX),
            (Property::SortOrder, <u64 as Options>::F_INDEX),
        ]
    }

    fn max_len() -> &'static [(Self::Property, usize)] {
        // Validated from set.rs
        &[]
    }

    fn collection() -> Collection {
        Collection::Mailbox
    }

    fn new(id: JMAPId) -> Self {
        let mut item = Mailbox::default();
        item.properties
            .append(Property::Id, Value::Id { value: id });
        item
    }
}

pub trait CreateMailbox: Sized {
    fn new_mailbox(name: &str, role: &str) -> Self;
}

impl CreateMailbox for TinyORM<Mailbox> {
    fn new_mailbox(name: &str, role: &str) -> Self {
        let mut mailbox = TinyORM::<Mailbox>::new();
        mailbox.set(
            Property::Name,
            Value::Text {
                value: name.to_string(),
            },
        );
        mailbox.set(
            Property::Role,
            Value::Text {
                value: role.to_string(),
            },
        );
        mailbox.tag(Property::Role, Tag::Default);
        mailbox.set(Property::ParentId, Value::Id { value: 0u64.into() });
        mailbox
    }
}
