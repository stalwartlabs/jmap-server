use jmap::{
    orm::serialize::JMAPOrm,
    principal::schema::{Principal, Property, Type, Value},
    scrypt::{
        password_hash::{PasswordHash, PasswordVerifier},
        Scrypt,
    },
    types::jmap::JMAPId,
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
    fn find_login(&self, login: String) -> store::Result<Option<AccountId>>;
    fn find_recipient(&self, email: String) -> store::Result<Option<AccountId>>;
    fn auth(&self, account_id: AccountId, login: &str, password: &str) -> store::Result<bool>;
    fn build_session(&self, account_id: AccountId) -> store::Result<Option<Session>>;
}

impl<T> JMAPSessionStore for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn find_login(&self, login: String) -> store::Result<Option<AccountId>> {
        Ok(self
            .query_store::<FilterMapper>(
                0,
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

    fn find_recipient(&self, email: String) -> store::Result<Option<AccountId>> {
        Ok(self
            .query_store::<FilterMapper>(
                0,
                Collection::Principal,
                Filter::or(vec![
                    Filter::eq(Property::Email.into(), Query::Index(email.clone())),
                    Filter::eq(Property::Aliases.into(), Query::Index(email)),
                ]),
                Comparator::None,
            )?
            .into_iter()
            .next()
            .map(|id| id.get_document_id()))
    }

    fn auth(&self, account_id: AccountId, login: &str, password: &str) -> store::Result<bool> {
        if let Some(mut fields) = self.get_orm::<Principal>(0, account_id)? {
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
                        "Login failed: Account {} has invalid password hash.",
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
            Ok(false)
        }
    }

    fn build_session(&self, account_id: AccountId) -> store::Result<Option<Session>> {
        // Fetch Email and Secret
        let (email, secret) = if let Some(mut fields) = self.get_orm::<Principal>(0, account_id)? {
            if !matches!(
                fields.get(&Property::Type),
                Some(Value::Type {
                    value: Type::Individual
                })
            ) {
                debug!("Account {} is not an individual", JMAPId::from(account_id));
                return Ok(None);
            }
            if let (Some(Value::Text { value: email }), Some(Value::Text { value: secret })) = (
                fields.remove(&Property::Email),
                fields.remove(&Property::Secret),
            ) {
                (email, secret)
            } else {
                debug!(
                    "Account {} has no email or secret",
                    JMAPId::from(account_id)
                );
                return Ok(None);
            }
        } else {
            return Ok(None);
        };

        Ok(None)
    }
}
