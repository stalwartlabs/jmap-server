use jmap::{
    orm::serialize::JMAPOrm,
    principal::schema::{Principal, Property, Type, Value},
    scrypt::{
        password_hash::{PasswordHash, PasswordVerifier},
        Scrypt,
    },
    types::jmap::JMAPId,
    SUPERUSER_ID,
};
use store::{
    core::{collection::Collection, JMAPIdPrefix},
    read::{
        comparator::Comparator,
        filter::{Filter, Query},
        FilterMapper,
    },
    tracing::debug,
    AccountId, JMAPStore, Store,
};

use super::Session;

pub trait JMAPSessionStore {
    fn find_account(&self, login: String) -> store::Result<Option<AccountId>>;
    fn auth(&self, account_id: AccountId, login: &str, password: &str) -> store::Result<bool>;
    fn build_session(&self, account_id: AccountId) -> store::Result<Option<Session>>;
    fn account_details(
        &self,
        account_id: AccountId,
    ) -> store::Result<Option<(String, String, Type)>>;
}

impl<T> JMAPSessionStore for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn find_account(&self, login: String) -> store::Result<Option<AccountId>> {
        Ok(self
            .query_store::<FilterMapper>(
                SUPERUSER_ID,
                Collection::Principal,
                Filter::and(vec![
                    Filter::eq(Property::Email.into(), Query::Index(login)),
                    Filter::eq(Property::Type.into(), Query::Keyword("i".to_string())),
                ]),
                Comparator::None,
            )?
            .into_iter()
            .next()
            .map(|id| id.get_document_id()))
    }

    fn auth(&self, account_id: AccountId, login: &str, password: &str) -> store::Result<bool> {
        if let Some(mut fields) = self.get_orm::<Principal>(SUPERUSER_ID, account_id)? {
            if !matches!(
                fields.get(&Property::Type),
                Some(Value::Type {
                    value: Type::Individual
                })
            ) {
                debug!("Account {} is not an individual", JMAPId::from(account_id));
                return Ok(false);
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
                    return Ok(false);
                }

                if let Ok(password_hash) = PasswordHash::new(&password_hash) {
                    if Scrypt
                        .verify_password(password.as_bytes(), &password_hash)
                        .is_ok()
                    {
                        Ok(true)
                    } else {
                        debug!(
                            "Login failed: Invalid password for account {}.",
                            JMAPId::from(account_id)
                        );
                        Ok(false)
                    }
                } else {
                    debug!(
                        "Login failed: Account {} has an invalid password hash.",
                        JMAPId::from(account_id)
                    );
                    Ok(false)
                }
            } else {
                debug!(
                    "Account {} has no email or secret",
                    JMAPId::from(account_id)
                );
                Ok(false)
            }
        } else {
            debug!(
                "Login failed: ORM for account {} does not exist.",
                JMAPId::from(account_id)
            );
            Ok(false)
        }
    }

    fn build_session(&self, primary_id: AccountId) -> store::Result<Option<Session>> {
        // Fetch Email
        let email = if let Some(mut fields) = self.get_orm::<Principal>(0, primary_id)? {
            if matches!(
                fields.get(&Property::Type),
                Some(Value::Type {
                    value: Type::Individual
                })
            ) {
                if let Some(Value::Text { value: email }) = fields.remove(&Property::Email) {
                    email
                } else {
                    debug!("Account {} has no email.", JMAPId::from(primary_id));
                    return Ok(None);
                }
            } else {
                debug!("Account {} is not an individual", JMAPId::from(primary_id));
                return Ok(None);
            }
        } else {
            debug!("Account {} does not exist.", JMAPId::from(primary_id));
            return Ok(None);
        };

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

        // Obtain accounts that have shared resources with this account
        let access_to = self.shared_accounts(&member_of)?;

        Ok(Some(Session::new(email, primary_id, member_of, access_to)))
    }

    fn account_details(
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
}
