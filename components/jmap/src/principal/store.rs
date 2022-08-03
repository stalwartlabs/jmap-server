use store::{
    core::collection::Collection,
    read::{
        filter::{self, Query},
        FilterMapper,
    },
    AccountId, JMAPStore, Store,
};

use crate::{
    error::set::{SetError, SetErrorType},
    orm::serialize::JMAPOrm,
    sanitize_email, SUPERUSER_ID,
};

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
            SetError::new(
                SetErrorType::InvalidProperties,
                format!("E-mail {:?} is invalid.", email),
            )
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
            SetError::new(
                SetErrorType::InvalidProperties,
                format!("E-mail {:?} does not exist.", email),
            )
        })
    }
}
