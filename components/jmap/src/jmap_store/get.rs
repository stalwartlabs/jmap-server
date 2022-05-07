use std::collections::HashMap;

use store::{
    core::JMAPIdPrefix, roaring::RoaringBitmap, AccountId, DocumentId, JMAPId, JMAPStore, Store,
};

use crate::{
    error::method::MethodError,
    id::{state::JMAPState, JMAPIdSerialize},
    protocol::json::JSONValue,
    request::get::GetRequest,
    Property,
};

use super::changes::JMAPChanges;

pub trait GetObject<'y, T>: Sized
where
    T: for<'x> Store<'x> + 'static,
{
    type Property: Property;

    fn new(
        store: &'y JMAPStore<T>,
        request: &mut GetRequest,
        properties: &[Self::Property],
    ) -> crate::Result<Self>;
    fn get_item(
        &self,
        jmap_id: JMAPId,
        properties: &[Self::Property],
    ) -> crate::Result<Option<JSONValue>>;
    fn map_ids<W>(&self, document_ids: W) -> crate::Result<Vec<JMAPId>>
    where
        W: Iterator<Item = DocumentId>;
    fn is_virtual() -> bool;
    fn default_properties() -> Vec<Self::Property>;
}

#[derive(Default)]
pub struct GetResult {
    pub account_id: AccountId,
    pub state: JMAPState,
    pub list: Vec<JSONValue>,
    pub not_found: Vec<JSONValue>,
}

pub trait JMAPGet<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get<'y, 'z: 'y, U>(&'z self, request: GetRequest) -> crate::Result<GetResult>
    where
        U: GetObject<'y, T>;
}

impl<T> JMAPGet<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get<'y, 'z: 'y, U>(&'z self, mut request: GetRequest) -> crate::Result<GetResult>
    where
        U: GetObject<'y, T>,
    {
        let collection = U::Property::collection();
        let is_virtual = U::is_virtual();
        let properties: Vec<U::Property> = request
            .properties
            .to_array()
            .map(|properties| {
                properties
                    .iter()
                    .filter_map(|property| property.to_string().and_then(U::Property::parse))
                    .collect::<Vec<U::Property>>()
            })
            .unwrap_or_else(|| U::default_properties());
        let object = U::new(self, &mut request, &properties)?;

        let document_ids = if !is_virtual {
            self.get_document_ids(request.account_id, collection)?
                .unwrap_or_default()
        } else {
            RoaringBitmap::new()
        };

        let request_ids = if let Some(request_ids) = request.ids {
            if request_ids.len() > self.config.max_objects_in_get {
                return Err(MethodError::RequestTooLarge);
            } else {
                request_ids
            }
        } else if !document_ids.is_empty() {
            object.map_ids(document_ids.iter().take(self.config.max_objects_in_get))?
        } else {
            Vec::new()
        };

        let mut not_found = Vec::new();
        let mut list = Vec::with_capacity(request_ids.len());

        for jmap_id in request_ids {
            if is_virtual || document_ids.contains(jmap_id.get_document_id()) {
                if let Some(result) = object.get_item(jmap_id, &properties)? {
                    list.push(result);
                    continue;
                }
            }
            not_found.push(jmap_id.to_jmap_string().into());
        }

        Ok(GetResult {
            account_id: request.account_id,
            state: self.get_state(request.account_id, collection)?,
            list,
            not_found,
        })
    }
}
impl GetResult {
    pub fn no_account_id(mut self) -> Self {
        self.account_id = AccountId::MAX;
        self
    }
}

impl From<GetResult> for JSONValue {
    fn from(get_result: GetResult) -> Self {
        let mut result = HashMap::new();
        if get_result.account_id != AccountId::MAX {
            result.insert(
                "accountId".to_string(),
                (get_result.account_id as JMAPId).to_jmap_string().into(),
            );
            result.insert("state".to_string(), get_result.state.into());
        }
        result.insert("list".to_string(), get_result.list.into());
        result.insert("notFound".to_string(), get_result.not_found.into());
        result.into()
    }
}
