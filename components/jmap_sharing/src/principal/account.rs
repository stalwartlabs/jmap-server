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

use std::sync::Arc;

use jmap::{
    orm::serialize::JMAPOrm,
    principal::schema::{Principal, Property, Type, Value},
    types::jmap::JMAPId,
    SUPERUSER_ID,
};
use store::{
    core::{acl::ACLToken, collection::Collection, error::StoreError, JMAPIdPrefix},
    read::{
        comparator::Comparator,
        filter::{Filter, Query},
        FilterMapper,
    },
    tracing::debug,
    AccountId, JMAPStore, RecipientType, Store,
};

pub trait JMAPAccountStore {
    fn find_individual(&self, email: &str) -> store::Result<Option<AccountId>>;
    fn authenticate(&self, login: &str, password: &str) -> store::Result<Option<AccountId>>;
    fn get_acl_token(&self, primary_id: AccountId) -> store::Result<Arc<ACLToken>>;
    fn get_account_details(
        &self,
        account_id: AccountId,
    ) -> store::Result<Option<(String, String, Type)>>;
    fn get_account_secret_hash(&self, account_id: AccountId) -> store::Result<Option<String>>;
    fn expand_rcpt(&self, email: String) -> store::Result<Arc<RecipientType>>;
}

impl<T> JMAPAccountStore for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn find_individual(&self, email: &str) -> store::Result<Option<AccountId>> {
        Ok(self
            .query_store::<FilterMapper>(
                SUPERUSER_ID,
                Collection::Principal,
                Filter::and(vec![
                    Filter::eq(Property::Email.into(), Query::Index(email.to_string())),
                    Filter::eq(Property::Type.into(), Query::Keyword("i".to_string())),
                ]),
                Comparator::None,
            )?
            .into_iter()
            .next()
            .map(|id| id.get_document_id()))
    }

    fn authenticate(&self, login: &str, password: &str) -> store::Result<Option<AccountId>> {
        if let Some(account_id) = self.find_individual(login)? {
            if let Some(mut fields) = self.get_orm::<Principal>(SUPERUSER_ID, account_id)? {
                if !matches!(
                    fields.get(&Property::Type),
                    Some(Value::Type {
                        value: Type::Individual
                    })
                ) {
                    debug!("Account {} is not an individual", JMAPId::from(account_id));
                    return Ok(None);
                }
                if let (
                    Some(Value::Text { value: email }),
                    Some(Value::Text {
                        value: password_hash,
                    }),
                ) = (
                    fields.remove(&Property::Email),
                    fields.remove(&Property::Secret),
                ) {
                    if email != login {
                        debug!(
                            "Login failed: Account {} has email {} but {} was used.",
                            JMAPId::from(account_id),
                            email,
                            login
                        );
                        return Ok(None);
                    }

                    if let Ok(matches) = argon2::verify_encoded(&password_hash, password.as_bytes())
                    {
                        if matches {
                            Ok(Some(account_id))
                        } else {
                            debug!(
                                "Login failed: Invalid password for account {}.",
                                JMAPId::from(account_id)
                            );
                            Ok(None)
                        }
                    } else {
                        debug!(
                            "Login failed: Account {} has an invalid password hash.",
                            JMAPId::from(account_id)
                        );
                        Ok(None)
                    }
                } else {
                    debug!(
                        "Account {} has no email or secret",
                        JMAPId::from(account_id)
                    );
                    Ok(None)
                }
            } else {
                debug!(
                    "Login failed: ORM for account {} does not exist.",
                    JMAPId::from(account_id)
                );
                Ok(None)
            }
        } else {
            debug!("Login failed: Login '{}' not found.", login);
            Ok(None)
        }
    }

    fn get_acl_token(&self, primary_id: AccountId) -> store::Result<Arc<ACLToken>> {
        self.acl_tokens
            .try_get_with::<_, StoreError>(primary_id, || {
                // Find all groups this account is a member of
                let mut member_of = vec![primary_id];
                let mut iter_stack = Vec::new();
                let mut current_id = primary_id;

                'outer: loop {
                    let mut iter = self
                        .query_store::<FilterMapper>(
                            SUPERUSER_ID,
                            Collection::Principal,
                            Filter::and(vec![
                                Filter::eq(Property::Members.into(), Query::Integer(current_id)),
                                Filter::eq(Property::Type.into(), Query::Keyword("g".to_string())),
                            ]),
                            Comparator::None,
                        )?
                        .into_iter()
                        .map(|id| id.get_document_id())
                        .collect::<Vec<_>>()
                        .into_iter();

                    loop {
                        while let Some(member_account) = iter.next() {
                            if !member_of.contains(&member_account) {
                                member_of.push(member_account);
                                if iter_stack.len() < 10 {
                                    iter_stack.push(iter);
                                    current_id = member_account;
                                    continue 'outer;
                                }
                            }
                        }

                        if let Some(prev_it) = iter_stack.pop() {
                            iter = prev_it;
                        } else {
                            break 'outer;
                        }
                    }
                }

                let access_to = self.get_shared_accounts(&member_of)?;

                Ok(ACLToken {
                    member_of,
                    access_to,
                }
                .into())
            })
            .map_err(|e| e.as_ref().clone())
    }

    fn get_account_details(
        &self,
        account_id: AccountId,
    ) -> store::Result<Option<(String, String, Type)>> {
        if let Some(mut fields) = self.get_orm::<Principal>(SUPERUSER_ID, account_id)? {
            Ok(Some((
                fields
                    .remove(&Property::Email)
                    .and_then(|v| {
                        if let Value::Text { value } = v {
                            Some(value)
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default(),
                fields
                    .remove(&Property::Name)
                    .and_then(|v| {
                        if let Value::Text { value } = v {
                            Some(value)
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default(),
                fields
                    .remove(&Property::Type)
                    .and_then(|v| {
                        if let Value::Type { value } = v {
                            Some(value)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(Type::Individual),
            )))
        } else {
            debug!(
                "Account details failed: ORM for account {} does not exist.",
                JMAPId::from(account_id)
            );
            Ok(None)
        }
    }

    // Used as nonce for token encryption
    fn get_account_secret_hash(&self, account_id: AccountId) -> store::Result<Option<String>> {
        if let Some(mut fields) = self.get_orm::<Principal>(SUPERUSER_ID, account_id)? {
            Ok(Some(
                fields
                    .remove(&Property::Secret)
                    .and_then(|v| {
                        if let Value::Text { value } = v {
                            Some(value)
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default(),
            ))
        } else {
            Ok(None)
        }
    }

    fn expand_rcpt(&self, email: String) -> store::Result<Arc<RecipientType>> {
        self.recipients
            .try_get_with::<_, StoreError>(email.clone(), || {
                Ok(Arc::new(
                    if let Some(account_id) = self
                        .query_store::<FilterMapper>(
                            SUPERUSER_ID,
                            Collection::Principal,
                            Filter::or(vec![
                                Filter::eq(Property::Email.into(), Query::Index(email.clone())),
                                Filter::eq(Property::Aliases.into(), Query::Index(email)),
                            ]),
                            Comparator::None,
                        )?
                        .into_iter()
                        .next()
                        .map(|id| id.get_document_id())
                    {
                        if let Some(mut fields) =
                            self.get_orm::<Principal>(SUPERUSER_ID, account_id)?
                        {
                            match fields.get(&Property::Type) {
                                Some(Value::Type { value: Type::List }) => {
                                    if let Some(Value::Members { value }) =
                                        fields.remove(&Property::Members)
                                    {
                                        if !value.is_empty() {
                                            let mut list = Vec::with_capacity(value.len());
                                            for id in value {
                                                let account_id = id.get_document_id();
                                                match self.get_account_details(account_id)? {
                                                    Some((email, _, ptype))
                                                        if ptype == Type::Individual =>
                                                    {
                                                        list.push((account_id, email));
                                                    }
                                                    _ => (),
                                                }
                                            }
                                            return Ok(Arc::new(RecipientType::List(list)));
                                        }
                                    }
                                    RecipientType::NotFound
                                }
                                _ => RecipientType::Individual(account_id),
                            }
                        } else {
                            debug!(
                                "Rcpt expand failed: ORM for account {} does not exist.",
                                JMAPId::from(account_id)
                            );
                            RecipientType::NotFound
                        }
                    } else {
                        RecipientType::NotFound
                    },
                ))
            })
            .map_err(|e| e.as_ref().clone())
    }
}
