use std::collections::HashMap;

use crate::jmap_store::get::{default_mapper, GetHelper, GetObject};
use crate::orm::acl::ACLUpdate;
use crate::orm::serialize::JMAPOrm;
use crate::request::get::{GetRequest, GetResponse};
use crate::types::jmap::JMAPId;

use store::core::error::StoreError;
use store::JMAPStore;
use store::Store;

use super::schema::{Principal, Property, Value};

impl GetObject for Principal {
    type GetArguments = ();

    fn default_properties() -> Vec<Self::Property> {
        vec![
            Property::Id,
            Property::Name,
            Property::Email,
            Property::Type,
            Property::Description,
        ]
    }

    fn get_as_id(&self, _property: &Self::Property) -> Option<Vec<JMAPId>> {
        None
    }
}

pub trait JMAPGetPrincipal<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn identity_get(&self, request: GetRequest<Principal>)
        -> crate::Result<GetResponse<Principal>>;
}

impl<T> JMAPGetPrincipal<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn identity_get(
        &self,
        request: GetRequest<Principal>,
    ) -> crate::Result<GetResponse<Principal>> {
        let helper = GetHelper::new(self, request, default_mapper.into())?;
        let account_id = helper.account_id;

        helper.get(|id, properties| {
            let document_id = id.get_document_id();
            let mut fields = self
                .get_orm::<Principal>(account_id, document_id)?
                .ok_or_else(|| StoreError::InternalError("Principal data not found".to_string()))?;
            let mut identity = HashMap::with_capacity(properties.len());

            for property in properties {
                identity.insert(
                    *property,
                    match property {
                        Property::Id => Value::Id { value: id },
                        Property::ACL => Value::ACL(ACLUpdate {
                            acl: fields.get_acls(),
                            set: true,
                        }),
                        Property::Secret => Value::Null,
                        _ => fields.remove(property).unwrap_or_default(),
                    },
                );
            }
            Ok(Some(Principal {
                properties: identity,
            }))
        })
    }
}
