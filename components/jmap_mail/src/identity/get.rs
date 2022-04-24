use std::collections::hash_map::Entry;
use std::collections::HashMap;

use jmap::id::JMAPIdSerialize;
use jmap::jmap_store::get::GetObject;
use jmap::jmap_store::orm::JMAPOrm;
use jmap::protocol::json::JSONValue;
use jmap::request::get::GetRequest;

use store::{AccountId, JMAPId, JMAPIdPrefix, JMAPStore, StoreError};
use store::{DocumentId, Store};

use super::IdentityProperty;

pub struct GetIdentity<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    store: &'y JMAPStore<T>,
    account_id: AccountId,
}

impl<'y, T> GetObject<'y, T> for GetIdentity<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    type Property = IdentityProperty;

    fn new(
        store: &'y JMAPStore<T>,
        request: &mut GetRequest,
        _properties: &[Self::Property],
    ) -> jmap::Result<Self> {
        Ok(GetIdentity {
            store,
            account_id: request.account_id,
        })
    }

    fn get_item(
        &self,
        jmap_id: JMAPId,
        properties: &[Self::Property],
    ) -> jmap::Result<Option<JSONValue>> {
        let document_id = jmap_id.get_document_id();
        let mut identity = self
            .store
            .get_orm::<IdentityProperty>(self.account_id, document_id)?
            .ok_or_else(|| StoreError::InternalError("Identity data not found".to_string()))?;

        let mut result: HashMap<String, JSONValue> = HashMap::new();

        for property in properties {
            if let Entry::Vacant(entry) = result.entry(property.to_string()) {
                let value = match property {
                    IdentityProperty::Id => jmap_id.to_jmap_string().into(),
                    IdentityProperty::Name
                    | IdentityProperty::Email
                    | IdentityProperty::ReplyTo
                    | IdentityProperty::Bcc
                    | IdentityProperty::TextSignature
                    | IdentityProperty::HtmlSignature => {
                        identity.remove(property).unwrap_or_default()
                    }
                    IdentityProperty::MayDelete => true.into(),
                };

                entry.insert(value);
            }
        }

        Ok(Some(result.into()))
    }

    fn map_ids<W>(&self, document_ids: W) -> jmap::Result<Vec<JMAPId>>
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
            IdentityProperty::Id,
            IdentityProperty::Name,
            IdentityProperty::Email,
            IdentityProperty::ReplyTo,
            IdentityProperty::Bcc,
            IdentityProperty::TextSignature,
            IdentityProperty::HtmlSignature,
            IdentityProperty::MayDelete,
        ]
    }
}
