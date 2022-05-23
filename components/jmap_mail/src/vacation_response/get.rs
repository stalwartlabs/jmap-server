use std::collections::HashMap;

use jmap::jmap_store::get::{default_mapper, GetHelper, GetObject};
use jmap::jmap_store::orm::{self, JMAPOrm};
use jmap::request::get::{GetRequest, GetResponse};

use store::core::error::StoreError;
use store::JMAPStore;
use store::Store;

use super::schema::{Property, VacationResponse, Value};

impl GetObject for VacationResponse {
    type GetArguments = ();

    fn default_properties() -> Vec<Self::Property> {
        vec![
            Property::Id,
            Property::IsEnabled,
            Property::FromDate,
            Property::ToDate,
            Property::Subject,
            Property::TextBody,
            Property::HtmlBody,
        ]
    }
}

pub trait JMAPGetVacationResponse<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn vacation_response_get(
        &self,
        request: GetRequest<VacationResponse>,
    ) -> jmap::Result<GetResponse<VacationResponse>>;
}

impl<T> JMAPGetVacationResponse<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn vacation_response_get(
        &self,
        request: GetRequest<VacationResponse>,
    ) -> jmap::Result<GetResponse<VacationResponse>> {
        let helper = GetHelper::new(self, request, default_mapper.into())?;
        let account_id = helper.account_id;

        helper.get(|id, properties| {
            let document_id = id.get_document_id();
            let mut fields = self
                .get_orm::<VacationResponse>(account_id, document_id)?
                .ok_or_else(|| {
                    StoreError::InternalError("VacationResponse data not found".to_string())
                })?;
            let mut vacation_response = HashMap::with_capacity(properties.len());

            for property in properties {
                vacation_response.insert(
                    *property,
                    if let Property::Id = property {
                        Value::Id { value: id }
                    } else if let Some(orm::Value::Object(value)) = fields.remove(property) {
                        value
                    } else {
                        Value::Null
                    },
                );
            }
            Ok(Some(VacationResponse {
                properties: vacation_response,
            }))
        })
    }
}
