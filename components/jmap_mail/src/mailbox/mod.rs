//pub mod changes;
//pub mod get;
//pub mod query;
//pub mod raft;
pub mod schema;
pub mod serialize;
pub mod set;

use jmap::id::jmap::JMAPId;
use jmap::jmap_store::Object;

use store::core::collection::Collection;
use store::write::options::Options;
use store::FieldId;

use self::schema::{Mailbox, MailboxValue, Property};

impl Object for Mailbox {
    type Property = Property;

    type Value = ();

    fn id(&self) -> Option<&JMAPId> {
        todo!()
    }

    fn required() -> &'static [Self::Property] {
        &[Property::Name]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[
            (
                Property::Name,
                <u64 as Options>::F_TOKENIZE | <u64 as Options>::F_SORT,
            ),
            (Property::Role, <u64 as Options>::F_KEYWORD),
            (Property::ParentId, <u64 as Options>::F_SORT),
            (Property::SortOrder, <u64 as Options>::F_SORT),
        ]
    }

    fn collection() -> Collection {
        Collection::Mailbox
    }

    fn hide_account() -> bool {
        false
    }

    fn new(id: JMAPId) -> Self {
        let mut email = Mailbox::default();
        email
            .properties
            .insert(Property::Id, MailboxValue::Id { value: id });
        email
    }
}

impl From<Property> for FieldId {
    fn from(field: Property) -> Self {
        field as FieldId
    }
}
