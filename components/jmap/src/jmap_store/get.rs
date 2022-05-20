use std::collections::HashMap;

use store::{core::JMAPIdPrefix, roaring::RoaringBitmap, AccountId, DocumentId, JMAPStore, Store};

use crate::{
    error::method::MethodError,
    id::{jmap::JMAPId, state::JMAPState, JMAPIdSerialize},
    protocol::json::JSONValue,
    request::get::{GetRequest, GetResponse},
};

use super::{changes::JMAPChanges, Object};

pub struct GetHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: GetObject,
{
    pub store: &'y JMAPStore<T>,
    pub account_id: AccountId,
    pub document_ids: RoaringBitmap,
    pub properties: Vec<O::Property>,
    pub request_ids: Vec<JMAPId>,
    pub validate_ids: bool,
    pub request: GetRequest<O>,
    pub response: GetResponse<O>,
}

pub trait GetObject: Object {
    type GetArguments;

    fn default_properties() -> Vec<Self::Property>;
}

impl<'y, O, T> GetHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: GetObject,
{
    pub fn new<X, W>(
        store: &'y JMAPStore<T>,
        mut request: GetRequest<O>,
        id_mapper: Option<impl FnMut(Vec<DocumentId>) -> crate::Result<Vec<JMAPId>>>,
    ) -> crate::Result<Self> {
        let collection = O::collection();
        let validate_ids = id_mapper.is_some();
        let properties: Vec<O::Property> = request
            .properties
            .take()
            .unwrap_or_else(|| O::default_properties());

        let account_id = request.account_id.as_ref().unwrap().get_document_id();
        let document_ids = if validate_ids {
            store
                .get_document_ids(account_id, collection)?
                .unwrap_or_default()
        } else {
            RoaringBitmap::new()
        };

        let request_ids = if let Some(request_ids) = request.ids.take() {
            if request_ids.len() > store.config.max_objects_in_get {
                return Err(MethodError::RequestTooLarge);
            } else {
                request_ids
            }
        } else if !document_ids.is_empty() {
            id_mapper.unwrap()(
                document_ids
                    .iter()
                    .take(store.config.max_objects_in_get)
                    .collect(),
            )?
        } else {
            Vec::new()
        };

        Ok(GetHelper {
            store,
            properties: if !properties.is_empty() {
                properties
            } else {
                O::default_properties()
            },
            response: GetResponse {
                account_id: request.account_id.clone(),
                state: store.get_state(account_id, collection)?,
                list: Vec::with_capacity(request_ids.len()),
                not_found: Vec::new(),
            },
            account_id,
            request,
            document_ids,
            validate_ids,
            request_ids,
        })
    }

    pub fn update(
        mut self,
        mut get_fnc: impl FnMut(JMAPId, &[O::Property]) -> crate::Result<Option<O>>,
    ) -> crate::Result<GetResponse<O>> {
        for id in self.request_ids {
            if !self.validate_ids || self.document_ids.contains(id.get_document_id()) {
                if let Some(result) = get_fnc(id, &self.properties)? {
                    self.response.list.push(result);
                    continue;
                }
            }
            self.response.not_found.push(id.into());
        }
        Ok(self.response)
    }
}

/*
pub trait JMAPGet<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get<'y, 'z: 'y, O>(&'z self, request: GetRequest<O>) -> crate::Result<GetResponse<O>>
    where
        O: GetObject<T>;
}

impl<T> JMAPGet<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get<'y, 'z: 'y, O>(&'z self, mut request: GetRequest<O>) -> crate::Result<GetResponse<O>>
    where
        O: GetObject<T>,
    {
        O::init_get(&mut helper)?;

        Ok(helper.response)
    }
}
*/
