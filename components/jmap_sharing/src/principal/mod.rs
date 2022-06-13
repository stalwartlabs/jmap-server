use scrypt::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
    Scrypt,
};
use store::{core::collection::Collection, write::options::Options};

use jmap::{jmap_store::Object, orm::TinyORM};

use self::schema::{Principal, Property, Type, Value};

pub mod account;
pub mod get;
pub mod query;
pub mod schema;
pub mod serialize;
pub mod set;

impl Object for Principal {
    type Property = Property;

    type Value = Value;

    fn new(id: jmap::types::jmap::JMAPId) -> Self {
        let mut item = Principal::default();
        item.properties
            .insert(Property::Id, Value::Id { value: id });
        item
    }

    fn id(&self) -> Option<&jmap::types::jmap::JMAPId> {
        self.properties.get(&Property::Id).and_then(|id| match id {
            Value::Id { value } => Some(value),
            _ => None,
        })
    }

    fn required() -> &'static [Self::Property] {
        &[]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[
            (
                Property::Type,
                <u64 as Options>::F_KEYWORD | <u64 as Options>::F_INDEX,
            ),
            (
                Property::Name,
                <u64 as Options>::F_TOKENIZE | <u64 as Options>::F_INDEX,
            ),
            (
                Property::Email,
                <u64 as Options>::F_TOKENIZE | <u64 as Options>::F_INDEX,
            ),
            (
                Property::Aliases,
                <u64 as Options>::F_TOKENIZE | <u64 as Options>::F_INDEX,
            ),
            (Property::Members, <u64 as Options>::F_INDEX),
            (Property::Description, <u64 as Options>::F_TOKENIZE),
            (Property::Timezone, <u64 as Options>::F_TOKENIZE),
            (Property::Quota, <u64 as Options>::F_INDEX),
        ]
    }

    fn collection() -> Collection {
        Collection::Principal
    }
}

pub trait CreateAccount: Sized {
    fn new_account(email: &str, secret: &str, name: &str) -> Self;
    fn change_secret(self, secret: &str) -> Self;
}

impl CreateAccount for TinyORM<Principal> {
    fn new_account(email: &str, secret: &str, name: &str) -> Self {
        let mut account = TinyORM::<Principal>::new();
        account.set(
            Property::Name,
            Value::Text {
                value: name.to_string(),
            },
        );
        account.set(
            Property::Email,
            Value::Text {
                value: email.to_string(),
            },
        );
        account.set(
            Property::Secret,
            Value::Text {
                value: Scrypt
                    .hash_password(secret.as_bytes(), &SaltString::generate(&mut OsRng))
                    .unwrap()
                    .to_string(),
            },
        );
        account.set(
            Property::Type,
            Value::Type {
                value: Type::Individual,
            },
        );
        account
    }

    fn change_secret(mut self, secret: &str) -> Self {
        self.set(
            Property::Secret,
            Value::Text {
                value: Scrypt
                    .hash_password(secret.as_bytes(), &SaltString::generate(&mut OsRng))
                    .unwrap()
                    .to_string(),
            },
        );
        self
    }
}
