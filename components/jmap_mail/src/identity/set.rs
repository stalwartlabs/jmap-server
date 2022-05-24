use crate::identity::schema::Identity;
use jmap::error::set::{SetError, SetErrorType};
use jmap::id::jmap::JMAPId;
use jmap::jmap_store::orm::{JMAPOrm, TinyORM};
use jmap::jmap_store::set::SetHelper;
use jmap::jmap_store::Object;
use jmap::request::set::SetResponse;
use jmap::{jmap_store::set::SetObject, request::set::SetRequest};
use store::parking_lot::MutexGuard;
use store::{JMAPStore, Store};

use super::schema::{Property, Value};

impl SetObject for Identity {
    type SetArguments = ();

    type NextCall = ();

    fn map_references(&mut self, fnc: impl FnMut(&str) -> Option<JMAPId>) {
        todo!()
    }
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

        helper.create(|_create_id, item, _helper, document| {
            let mut fields = TinyORM::<Identity>::new();

            for (property, value) in item.properties {
                fields.set(
                    property,
                    match (property, value) {
                        (
                            Property::Name
                            | Property::Email
                            | Property::TextSignature
                            | Property::HtmlSignature,
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

            // Validate fields
            fields.insert_validate(document)?;

            Ok((
                Identity::new(document.document_id.into()),
                None::<MutexGuard<'_, ()>>,
            ))
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
