use jmap::jmap_store::get::{default_mapper, GetHelper, SharedDocsFnc};
use jmap::orm::serialize::JMAPOrm;
use jmap::principal::schema::{Principal, Property, Value};
use jmap::principal::store::JMAPPrincipals;
use jmap::request::get::{GetRequest, GetResponse};
use store::core::error::StoreError;
use store::core::vec_map::VecMap;
use store::JMAPStore;
use store::Store;

pub trait JMAPGetPrincipal<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn principal_get(&self, request: GetRequest<Principal>)
        -> jmap::Result<GetResponse<Principal>>;
}

impl<T> JMAPGetPrincipal<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn principal_get(
        &self,
        request: GetRequest<Principal>,
    ) -> jmap::Result<GetResponse<Principal>> {
        let helper = GetHelper::new(self, request, default_mapper.into(), None::<SharedDocsFnc>)?;
        let account_id = helper.account_id;

        helper.get(|id, properties| {
            let document_id = id.get_document_id();
            let mut fields = self
                .get_orm::<Principal>(account_id, document_id)?
                .ok_or_else(|| StoreError::NotFound("Principal data not found".to_string()))?;
            let mut principal = VecMap::with_capacity(properties.len());

            for property in properties {
                principal.append(
                    *property,
                    match property {
                        Property::Id => Value::Id { value: id },
                        Property::ACL => {
                            let mut acl_get = VecMap::new();
                            for (account_id, acls) in fields.get_acls() {
                                if let Some(email) = self.principal_to_email(account_id)? {
                                    acl_get.append(email, acls);
                                }
                            }
                            Value::ACL(acl_get)
                        }

                        Property::Secret => Value::Null,
                        _ => fields.remove(property).unwrap_or_default(),
                    },
                );
            }
            Ok(Some(Principal {
                properties: principal,
            }))
        })
    }
}
