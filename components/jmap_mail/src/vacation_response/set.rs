use crate::vacation_response::schema::VacationResponse;
use jmap::error::set::{SetError, SetErrorType};
use jmap::jmap_store::set::SetHelper;
use jmap::jmap_store::Object;
use jmap::orm::{serialize::JMAPOrm, TinyORM};
use jmap::request::set::SetResponse;
use jmap::request::ResultReference;
use jmap::types::jmap::JMAPId;
use jmap::{jmap_store::set::SetObject, request::set::SetRequest};
use store::parking_lot::MutexGuard;
use store::{JMAPStore, Store};

use super::schema::{Property, Value};

impl SetObject for VacationResponse {
    type SetArguments = ();

    type NextCall = ();

    fn eval_id_references(&mut self, _fnc: impl FnMut(&str) -> Option<JMAPId>) {}
    fn eval_result_references(&mut self, _fnc: impl FnMut(&ResultReference) -> Option<Vec<u64>>) {}
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

        helper.create(|_create_id, item, helper, document| {
            // Create as a singleton
            let id = JMAPId::singleton();
            document.document_id = id.get_document_id();

            // Make sure the VacationResponse object does not exist already
            if self
                .get_orm::<VacationResponse>(helper.account_id, document.document_id)?
                .is_some()
            {
                return Err(SetError::new(
                    SetErrorType::Forbidden,
                    "VacationResponse already exists, use update instead.",
                ));
            }

            let mut fields = TinyORM::<VacationResponse>::new();

            for (property, value) in item.properties {
                fields.set(
                    property,
                    match (property, value) {
                        (
                            Property::Subject | Property::HtmlBody | Property::TextBody,
                            value @ Value::Text { .. },
                        ) => value,

                        (Property::ToDate | Property::FromDate, value @ Value::DateTime { .. }) => {
                            value
                        }
                        (Property::IsEnabled, value @ Value::Bool { .. }) => value,
                        (
                            Property::Subject
                            | Property::HtmlBody
                            | Property::TextBody
                            | Property::ToDate
                            | Property::FromDate,
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

            Ok((VacationResponse::new(id), None::<MutexGuard<'_, ()>>))
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
                        ) => value,

                        (Property::ToDate | Property::FromDate, value @ Value::DateTime { .. }) => {
                            value
                        }
                        (Property::IsEnabled, value @ Value::Bool { .. }) => value,
                        (
                            Property::Subject
                            | Property::HtmlBody
                            | Property::TextBody
                            | Property::ToDate
                            | Property::FromDate,
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

            // Remove sent responses
            fields.remove(&Property::SentResponses_);

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
