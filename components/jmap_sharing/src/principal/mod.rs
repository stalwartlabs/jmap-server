use argon2::{
    password_hash::{rand_core::OsRng, SaltString},
    Argon2, PasswordHasher,
};

use jmap::{
    orm::TinyORM,
    types::principal::{Principal, Property, Type, Value},
};

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
        //TODO scrypt performance
        account.set(
            Property::Secret,
            Value::Text {
                value: secret.to_string(),
                /*value: Scrypt
                .hash_password(secret.as_bytes(), &SaltString::generate(&mut OsRng))
                .unwrap()
                .to_string(),*/
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
                value: Argon2::default()
                    .hash_password(secret.as_bytes(), &SaltString::generate(&mut OsRng))
                    .unwrap()
                    .to_string(),
            },
        );
        self
    }
}
