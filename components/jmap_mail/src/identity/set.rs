use crate::identity::schema::Identity;
use jmap::error::set::{SetError, SetErrorType};
use jmap::jmap_store::set::SetHelper;
use jmap::jmap_store::Object;
use jmap::orm::{serialize::JMAPOrm, TinyORM};
use jmap::request::set::SetResponse;
use jmap::request::{ACLEnforce, ResultReference};
use jmap::types::jmap::JMAPId;
use jmap::types::principal;
use jmap::{jmap_store::set::SetObject, request::set::SetRequest};
use jmap::{sanitize_email, SUPERUSER_ID};
use store::core::collection::Collection;
use store::core::JMAPIdPrefix;
use store::read::comparator::Comparator;
use store::read::filter::{Filter, Query};
use store::read::FilterMapper;
use store::{JMAPStore, Store};

use super::schema::{Property, Value};

impl SetObject for Identity {
    type SetArguments = ();

    type NextCall = ();

    fn eval_id_references(&mut self, _fnc: impl FnMut(&str) -> Option<JMAPId>) {}
    fn eval_result_references(&mut self, _fnc: impl FnMut(&ResultReference) -> Option<Vec<u64>>) {}
}

pub trait JMAPSetIdentity<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn identity_set(&self, request: SetRequest<Identity>) -> jmap::Result<SetResponse<Identity>>;
}

impl<T> JMAPSetIdentity<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn identity_set(&self, request: SetRequest<Identity>) -> jmap::Result<SetResponse<Identity>> {
        let mut helper = SetHelper::new(self, request)?;

        helper.create(|_create_id, item, helper, document| {
            let mut fields = TinyORM::<Identity>::new();

            for (property, value) in item.properties {
                fields.set(
                    property,
                    match (property, value) {
                        (
                            Property::Name | Property::TextSignature | Property::HtmlSignature,
                            value @ Value::Text { .. },
                        ) => value,

                        (Property::Email, Value::Text { value }) => {
                            let value = sanitize_email(&value).ok_or_else(|| {
                                SetError::invalid_property(
                                    Property::Email,
                                    "Invalid e-mail address.".to_string(),
                                )
                            })?;
                            if !helper.acl.is_member(SUPERUSER_ID)
                                && !helper
                                    .store
                                    .query_store::<FilterMapper>(
                                        SUPERUSER_ID,
                                        Collection::Principal,
                                        Filter::or(vec![
                                            Filter::eq(
                                                principal::Property::Email.into(),
                                                Query::Index(value.clone()),
                                            ),
                                            Filter::eq(
                                                principal::Property::Aliases.into(),
                                                Query::Index(value.clone()),
                                            ),
                                        ]),
                                        Comparator::None,
                                    )?
                                    .into_iter()
                                    .any(|id| id.get_document_id() == helper.account_id)
                            {
                                return Err(SetError::invalid_property(
                                    Property::Email,
                                    "E-mail address not configured for this account.".to_string(),
                                ));
                            }
                            Value::Text { value }
                        }
                        (Property::ReplyTo | Property::Bcc, value @ Value::Addresses { .. }) => {
                            value
                        }
                        (
                            Property::Name
                            | Property::TextSignature
                            | Property::HtmlSignature
                            | Property::ReplyTo
                            | Property::Bcc,
                            Value::Null,
                        ) => Value::Null,
                        (property, _) => {
                            return Err(SetError::invalid_property(
                                property,
                                "Field could not be set.",
                            ));
                        }
                    },
                );
            }

            // Validate fields
            fields.insert_validate(document)?;

            Ok(Identity::new(document.document_id.into()))
        })?;

        helper.update(|id, item, helper, document| {
            let current_fields = self
                .get_orm::<Identity>(helper.account_id, id.get_document_id())?
                .ok_or_else(|| SetError::new_err(SetErrorType::NotFound))?;
            let mut fields = TinyORM::track_changes(&current_fields);

            for (property, value) in item.properties {
                fields.set(
                    property,
                    match (property, value) {
                        (
                            Property::Name | Property::TextSignature | Property::HtmlSignature,
                            value @ Value::Text { .. },
                        ) => value,

                        (Property::ReplyTo | Property::Bcc, value @ Value::Addresses { .. }) => {
                            value
                        }
                        (
                            Property::Name
                            | Property::TextSignature
                            | Property::HtmlSignature
                            | Property::ReplyTo
                            | Property::Bcc,
                            Value::Null,
                        ) => Value::Null,
                        (property, _) => {
                            return Err(SetError::invalid_property(
                                property,
                                "Field could not be set.",
                            ));
                        }
                    },
                );
            }

            // Merge changes
            current_fields.merge_validate(document, fields)?;
            Ok(None)
        })?;

        helper.destroy(|_id, helper, document| {
            if let Some(orm) = self.get_orm::<Identity>(helper.account_id, document.document_id)? {
                orm.delete(document);
            }
            Ok(())
        })?;

        helper.into_response()
    }
}
