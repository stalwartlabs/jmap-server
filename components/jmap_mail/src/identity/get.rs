use std::collections::HashMap;

use jmap::jmap_store::get::{default_mapper, GetHelper, GetObject, SharedDocsFnc};
use jmap::orm::serialize::JMAPOrm;
use jmap::request::get::{GetRequest, GetResponse};
use jmap::types::jmap::JMAPId;

use store::core::error::StoreError;
use store::JMAPStore;
use store::Store;

use super::schema::{Identity, Property, Value};

impl GetObject for Identity {
    type GetArguments = ();

    fn default_properties() -> Vec<Self::Property> {
        vec![
            Property::Id,
            Property::Name,
            Property::Email,
            Property::ReplyTo,
            Property::Bcc,
            Property::TextSignature,
            Property::HtmlSignature,
            Property::MayDelete,
        ]
    }

    fn get_as_id(&self, _property: &Self::Property) -> Option<Vec<JMAPId>> {
        None
    }
}

pub trait JMAPGetIdentity<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn identity_get(&self, request: GetRequest<Identity>) -> jmap::Result<GetResponse<Identity>>;
}

impl<T> JMAPGetIdentity<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn identity_get(&self, request: GetRequest<Identity>) -> jmap::Result<GetResponse<Identity>> {
        let helper = GetHelper::new(self, request, default_mapper.into(), None::<SharedDocsFnc>)?;
        let account_id = helper.account_id;

        helper.get(|id, properties| {
            let document_id = id.get_document_id();
            let mut fields = self
                .get_orm::<Identity>(account_id, document_id)?
                .ok_or_else(|| StoreError::InternalError("Identity data not found".to_string()))?;
            let mut identity = HashMap::with_capacity(properties.len());

            for property in properties {
                identity.insert(
                    *property,
                    match property {
                        Property::Id => Value::Id { value: id },
                        Property::MayDelete => Value::Bool { value: true },
                        _ => fields.remove(property).unwrap_or_default(),
                    },
                );
            }
            Ok(Some(Identity {
                properties: identity,
            }))
        })
    }
}
