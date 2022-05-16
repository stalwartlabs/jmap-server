use crate::id::JMAPIdSerialize;
use crate::jmap_store::get::GetObject;
use crate::jmap_store::orm::JMAPOrm;
use crate::protocol::json::JSONValue;
use crate::request::get::GetRequest;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use store::core::error::StoreError;
use store::core::JMAPIdPrefix;
use store::{AccountId, DocumentId, Store};
use store::{JMAPId, JMAPStore};

use super::PushSubscriptionProperty;

pub struct GetPushSubscription<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    store: &'y JMAPStore<T>,
    account_id: AccountId,
}

impl<'y, T> GetObject<'y, T> for GetPushSubscription<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    type Property = PushSubscriptionProperty;

    fn new(
        store: &'y JMAPStore<T>,
        request: &mut GetRequest,
        _properties: &[Self::Property],
    ) -> crate::Result<Self> {
        Ok(GetPushSubscription {
            store,
            account_id: request.account_id,
        })
    }

    fn get_item(
        &self,
        jmap_id: JMAPId,
        properties: &[Self::Property],
    ) -> crate::Result<Option<JSONValue>> {
        let document_id = jmap_id.get_document_id();
        let mut subscription = self
            .store
            .get_orm::<PushSubscriptionProperty>(self.account_id, document_id)?
            .ok_or_else(|| {
                StoreError::InternalError("PushSubscription data not found".to_string())
            })?;

        let mut result: HashMap<String, JSONValue> = HashMap::new();

        for property in properties {
            if let Entry::Vacant(entry) = result.entry(property.to_string()) {
                let value = match property {
                    PushSubscriptionProperty::Id => jmap_id.to_jmap_string().into(),
                    PushSubscriptionProperty::DeviceClientId
                    | PushSubscriptionProperty::VerificationCode
                    | PushSubscriptionProperty::Types => {
                        subscription.remove(property).unwrap_or_default()
                    }
                    PushSubscriptionProperty::Expires => subscription
                        .remove(property)
                        .map(|utc_date| utc_date.into_utc_date())
                        .unwrap_or_default(),
                    PushSubscriptionProperty::Url
                    | PushSubscriptionProperty::Keys
                    | PushSubscriptionProperty::VerificationCode_ => {
                        continue;
                    }
                };

                entry.insert(value);
            }
        }

        Ok(Some(result.into()))
    }

    fn map_ids<W>(&self, document_ids: W) -> crate::Result<Vec<JMAPId>>
    where
        W: Iterator<Item = DocumentId>,
    {
        Ok(document_ids.map(|id| id as JMAPId).collect())
    }

    fn is_virtual() -> bool {
        false
    }

    fn default_properties() -> Vec<Self::Property> {
        vec![
            PushSubscriptionProperty::Id,
            PushSubscriptionProperty::DeviceClientId,
            PushSubscriptionProperty::VerificationCode,
            PushSubscriptionProperty::Expires,
            PushSubscriptionProperty::Types,
        ]
    }
}
