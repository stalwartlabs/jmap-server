use std::collections::HashMap;

use super::VacationResponseProperty;
use jmap::error::set::SetError;
use jmap::jmap_store::orm::{JMAPOrm, TinyORM};
use jmap::jmap_store::set::{DefaultCreateItem, DefaultUpdateItem};
use jmap::protocol::invocation::Invocation;
use jmap::request::parse_utc_date;
use jmap::{
    jmap_store::set::{SetObject, SetObjectData, SetObjectHelper},
    protocol::json::JSONValue,
    request::set::SetRequest,
};
use store::batch::Document;
use store::{JMAPId, JMAPStore, Store};

#[derive(Default)]
pub struct SetVacationResponse {
    pub current_vacation_response: Option<TinyORM<VacationResponseProperty>>,
    pub vacation_response: TinyORM<VacationResponseProperty>,
}

pub struct SetVacationResponseHelper {}

impl<T> SetObjectData<T> for SetVacationResponseHelper
where
    T: for<'x> Store<'x> + 'static,
{
    fn new(_store: &JMAPStore<T>, _request: &mut SetRequest) -> jmap::Result<Self> {
        Ok(SetVacationResponseHelper {})
    }

    fn unwrap_invocation(self) -> Option<Invocation> {
        None
    }
}

impl<'y, T> SetObject<'y, T> for SetVacationResponse
where
    T: for<'x> Store<'x> + 'static,
{
    type Property = VacationResponseProperty;
    type Helper = SetVacationResponseHelper;
    type CreateItemResult = DefaultCreateItem;
    type UpdateItemResult = DefaultUpdateItem;

    fn new(
        helper: &mut SetObjectHelper<T, Self::Helper>,
        _fields: &mut HashMap<String, JSONValue>,
        jmap_id: Option<JMAPId>,
    ) -> jmap::error::set::Result<Self> {
        Ok(SetVacationResponse {
            current_vacation_response: if jmap_id.is_some() {
                helper
                    .store
                    .get_orm::<VacationResponseProperty>(helper.account_id, 0)?
                    .unwrap_or_default()
                    .into()
            } else {
                None
            },
            ..Default::default()
        })
    }

    fn set_field(
        &mut self,
        _helper: &mut SetObjectHelper<T, Self::Helper>,
        field: Self::Property,
        value: JSONValue,
    ) -> jmap::error::set::Result<()> {
        match (field, &value) {
            (
                VacationResponseProperty::Subject
                | VacationResponseProperty::TextBody
                | VacationResponseProperty::HtmlBody,
                JSONValue::String(text),
            ) if text.len() < 255 => {
                self.vacation_response.set(field, value);
            }
            (
                VacationResponseProperty::FromDate | VacationResponseProperty::ToDate,
                JSONValue::String(date_time),
            ) => {
                self.vacation_response.set(
                    field,
                    parse_utc_date(date_time)
                        .ok_or_else(|| {
                            SetError::invalid_property(
                                field.to_string(),
                                format!("Invalid date: {}", date_time),
                            )
                        })?
                        .into(),
                );
            }
            (VacationResponseProperty::IsEnabled, JSONValue::Bool(_)) => {
                self.vacation_response.set(field, value);
            }
            (VacationResponseProperty::IsEnabled, JSONValue::Null) => {
                self.vacation_response.set(field, false.into());
            }
            (_, JSONValue::Null) => {
                self.vacation_response.set(field, value);
            }
            (field, _) => {
                return Err(SetError::invalid_property(
                    field.to_string(),
                    "Field cannot be set.",
                ));
            }
        }
        Ok(())
    }

    fn patch_field(
        &mut self,
        _helper: &mut SetObjectHelper<T, Self::Helper>,
        field: Self::Property,
        _property: String,
        _value: JSONValue,
    ) -> jmap::error::set::Result<()> {
        Err(SetError::invalid_property(
            field.to_string(),
            "Patch operations not supported on this field.",
        ))
    }

    fn create(
        self,
        _helper: &mut SetObjectHelper<T, Self::Helper>,
        _create_id: &str,
        document: &mut Document,
    ) -> jmap::error::set::Result<Self::CreateItemResult> {
        TinyORM::default().merge_validate(document, self.vacation_response)?;
        Ok(DefaultCreateItem::new(document.document_id as JMAPId))
    }

    fn update(
        self,
        _helper: &mut SetObjectHelper<T, Self::Helper>,
        document: &mut Document,
    ) -> jmap::error::set::Result<Option<Self::UpdateItemResult>> {
        if self
            .current_vacation_response
            .unwrap()
            .merge_validate(document, self.vacation_response)?
        {
            Ok(Some(DefaultUpdateItem::default()))
        } else {
            Ok(None)
        }
    }

    fn delete(
        _helper: &mut SetObjectHelper<T, Self::Helper>,
        _jmap_id: store::JMAPId,
    ) -> jmap::error::set::Result<()> {
        Ok(())
    }
}
