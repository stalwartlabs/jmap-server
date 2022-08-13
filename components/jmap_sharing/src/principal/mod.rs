use jmap::{
    orm::TinyORM,
    principal::schema::{Principal, Property, Type, Value},
};
use store::rand::{self, Rng};

pub mod account;
pub mod get;
pub mod query;
pub mod set;

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
                value: argon2::hash_encoded(
                    secret.as_bytes(),
                    &rand::thread_rng().gen::<[u8; 10]>(),
                    &argon2::Config::default(),
                )
                .unwrap_or_default(),
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
                value: argon2::hash_encoded(
                    secret.as_bytes(),
                    &rand::thread_rng().gen::<[u8; 10]>(),
                    &argon2::Config::default(),
                )
                .unwrap_or_default(),
            },
        );
        self
    }
}
