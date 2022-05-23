use crate::vacation_response::schema::VacationResponse;
use jmap::error::set::{SetError, SetErrorType};
use jmap::id::jmap::JMAPId;
use jmap::jmap_store::orm::{self, JMAPOrm, TinyORM};
use jmap::jmap_store::set::SetHelper;
use jmap::jmap_store::Object;
use jmap::request::set::SetResponse;
use jmap::{jmap_store::set::SetObject, request::set::SetRequest};
use store::parking_lot::MutexGuard;
use store::{JMAPStore, Store};

use super::schema::{Property, Value};

impl SetObject for VacationResponse {
    type SetArguments = ();

    type NextInvocation = ();

    fn map_references(&mut self, fnc: impl FnMut(&str) -> Option<JMAPId>) {
        todo!()
    }
}

pub trait JMAPSetVacationResponse<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn vacation_response_set(
        &self,
        request: SetRequest<VacationResponse>,
    ) -> jmap::Result<SetResponse<VacationResponse>>;
}

impl<T> JMAPSetVacationResponse<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn vacation_response_set(
        &self,
        request: SetRequest<VacationResponse>,
    ) -> jmap::Result<SetResponse<VacationResponse>> {
        let mut helper = SetHelper::new(self, request)?;

        helper.create(|_create_id, item, _helper, document| {
            let mut fields = TinyORM::<VacationResponse>::new();

            for (property, value) in item.properties {
                fields.set(
                    property,
                    match (property, value) {
                        (
                            Property::Subject | Property::HtmlBody | Property::TextBody,
                            value @ Value::Text { .. },
                        ) => orm::Value::Object(value),

                        (Property::ToDate | Property::FromDate, value @ Value::DateTime { .. }) => {
                            orm::Value::Object(value)
                        }
                        (Property::IsEnabled, value @ Value::Bool { .. }) => {
                            orm::Value::Object(value)
                        }
                        (
                            Property::Subject
                            | Property::HtmlBody
                            | Property::TextBody
                            | Property::ToDate
                            | Property::FromDate,
                            Value::Null,
                        ) => orm::Value::Null,
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
                VacationResponse::new(document.document_id.into()),
                None::<MutexGuard<'_, ()>>,
            ))
        })?;

        helper.update(|id, item, helper, document| {
            let current_fields = self
                .get_orm::<VacationResponse>(helper.account_id, id.get_document_id())?
                .ok_or_else(|| SetError::new_err(SetErrorType::NotFound))?;
            let mut fields = TinyORM::track_changes(&current_fields);

            for (property, value) in item.properties {
                fields.set(
                    property,
                    match (property, value) {
                        (
                            Property::Subject | Property::HtmlBody | Property::TextBody,
                            value @ Value::Text { .. },
                        ) => orm::Value::Object(value),

                        (Property::ToDate | Property::FromDate, value @ Value::DateTime { .. }) => {
                            orm::Value::Object(value)
                        }
                        (Property::IsEnabled, value @ Value::Bool { .. }) => {
                            orm::Value::Object(value)
                        }
                        (
                            Property::Subject
                            | Property::HtmlBody
                            | Property::TextBody
                            | Property::ToDate
                            | Property::FromDate,
                            Value::Null,
                        ) => orm::Value::Null,
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
            if let Some(orm) =
                self.get_orm::<VacationResponse>(helper.account_id, document.document_id)?
            {
                orm.delete(document);
            }
            Ok(())
        })?;

        helper.into_response()
    }
}
