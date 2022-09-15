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
