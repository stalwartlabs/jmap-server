use std::collections::HashMap;

use crate::jmap_store::get::{default_mapper, GetHelper, GetObject, SharedDocsFnc};
use crate::orm::serialize::JMAPOrm;
use crate::request::get::{GetRequest, GetResponse};
use crate::types::jmap::JMAPId;

use store::core::error::StoreError;
use store::JMAPStore;
use store::Store;

use super::schema::{Property, PushSubscription, Value};

impl GetObject for PushSubscription {
    type GetArguments = ();

    fn default_properties() -> Vec<Self::Property> {
        vec![
            Property::Id,
            Property::DeviceClientId,
            Property::VerificationCode,
            Property::Expires,
            Property::Types,
        ]
    }

    fn get_as_id(&self, _property: &Self::Property) -> Option<Vec<JMAPId>> {
        None
    }
}

pub trait JMAPGetPushSubscription<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn push_subscription_get(
        &self,
        request: GetRequest<PushSubscription>,
    ) -> crate::Result<GetResponse<PushSubscription>>;
}

impl<T> JMAPGetPushSubscription<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn push_subscription_get(
        &self,
        request: GetRequest<PushSubscription>,
    ) -> crate::Result<GetResponse<PushSubscription>> {
        let helper = GetHelper::new(self, request, default_mapper.into(), None::<SharedDocsFnc>)?;
        let account_id = helper.account_id;

        helper.get(|id, properties| {
            let document_id = id.get_document_id();
            let mut fields = self
                .get_orm::<PushSubscription>(account_id, document_id)?
                .ok_or_else(|| {
                    StoreError::InternalError("PushSubscription data not found".to_string())
                })?;
            let mut push_subscription = HashMap::with_capacity(properties.len());

            for property in properties {
                push_subscription.insert(
                    *property,
                    match property {
                        Property::Id => Value::Id { value: id },
                        _ => fields.remove(property).unwrap_or_default(),
                    },
                );
            }
            Ok(Some(PushSubscription {
                properties: push_subscription,
            }))
        })
    }
}
