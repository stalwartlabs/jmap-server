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

use store::{
    core::collection::Collection,
    read::{
        filter::{self, Query},
        FilterMapper,
    },
    AccountId, JMAPStore, Store,
};

use crate::{error::set::SetError, orm::serialize::JMAPOrm, sanitize_email, SUPERUSER_ID};

use super::schema::{Principal, Property, Value};

pub trait JMAPPrincipals<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn principal_to_email(&self, id: AccountId) -> crate::Result<Option<String>>;
    fn principal_to_id<U>(&self, email: &str) -> crate::error::set::Result<AccountId, U>;
}

impl<T> JMAPPrincipals<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn principal_to_email(&self, id: AccountId) -> crate::Result<Option<String>> {
        Ok(self
            .get_orm::<Principal>(SUPERUSER_ID, id)?
            .and_then(|mut p| p.remove(&Property::Email))
            .and_then(|p| {
                if let Value::Text { value } = p {
                    Some(value)
                } else {
                    None
                }
            }))
    }

    fn principal_to_id<U>(&self, email: &str) -> crate::error::set::Result<AccountId, U> {
        let email_clean = sanitize_email(email).ok_or_else(|| {
            SetError::invalid_properties()
                .with_description(format!("E-mail {:?} is invalid.", email))
        })?;
        self.query_store::<FilterMapper>(
            SUPERUSER_ID,
            Collection::Principal,
            filter::Filter::or(vec![
                filter::Filter::eq(
                    Property::Email.into(),
                    Query::Index(email_clean.to_string()),
                ),
                filter::Filter::eq(Property::Aliases.into(), Query::Index(email_clean)),
            ]),
            store::read::comparator::Comparator::None,
        )
        .map_err(SetError::from)?
        .get_min()
        .ok_or_else(|| {
            SetError::invalid_properties()
                .with_description(format!("E-mail {:?} does not exist.", email))
        })
    }
}
