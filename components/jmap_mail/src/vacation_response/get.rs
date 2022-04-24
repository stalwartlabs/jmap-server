use std::collections::hash_map::Entry;
use std::collections::HashMap;

use jmap::jmap_store::get::GetObject;
use jmap::jmap_store::orm::JMAPOrm;
use jmap::protocol::json::JSONValue;
use jmap::request::get::GetRequest;

use store::{AccountId, JMAPId, JMAPStore};
use store::{DocumentId, Store};

use super::VacationResponseProperty;

pub struct GetVacationResponse<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    store: &'y JMAPStore<T>,
    account_id: AccountId,
}

impl<'y, T> GetObject<'y, T> for GetVacationResponse<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    type Property = VacationResponseProperty;

    fn new(
        store: &'y JMAPStore<T>,
        request: &mut GetRequest,
        _properties: &[Self::Property],
    ) -> jmap::Result<Self> {
        Ok(GetVacationResponse {
            store,
            account_id: request.account_id,
        })
    }

    fn get_item(
        &self,
        _jmap_id: JMAPId,
        properties: &[Self::Property],
    ) -> jmap::Result<Option<JSONValue>> {
        let mut vacation_response = self
            .store
            .get_orm::<VacationResponseProperty>(self.account_id, 0)?
            .unwrap_or_default();

        let mut result: HashMap<String, JSONValue> = HashMap::new();

        for property in properties {
            if let Entry::Vacant(entry) = result.entry(property.to_string()) {
                let value = match property {
                    VacationResponseProperty::Id => "singleton".to_string().into(),
                    VacationResponseProperty::IsEnabled => vacation_response
                        .remove(property)
                        .unwrap_or_else(|| false.into()),

                    VacationResponseProperty::Subject
                    | VacationResponseProperty::TextBody
                    | VacationResponseProperty::HtmlBody => {
                        vacation_response.remove(property).unwrap_or_default()
                    }
                    VacationResponseProperty::FromDate | VacationResponseProperty::ToDate => {
                        vacation_response
                            .remove(property)
                            .map(|utc_date| utc_date.into_utc_date())
                            .unwrap_or_default()
                    }
                };

                entry.insert(value);
            }
        }

        Ok(Some(result.into()))
    }

    fn map_ids<W>(&self, _document_ids: W) -> jmap::Result<Vec<JMAPId>>
    where
        W: Iterator<Item = DocumentId>,
    {
        Ok(vec![])
    }

    fn is_virtual() -> bool {
        true
    }

    fn default_properties() -> Vec<Self::Property> {
        vec![
            VacationResponseProperty::Id,
            VacationResponseProperty::IsEnabled,
            VacationResponseProperty::FromDate,
            VacationResponseProperty::ToDate,
            VacationResponseProperty::Subject,
            VacationResponseProperty::TextBody,
            VacationResponseProperty::HtmlBody,
        ]
    }
}
