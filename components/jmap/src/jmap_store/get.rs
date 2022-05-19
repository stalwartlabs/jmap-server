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
    O: GetObject<T>,
{
    pub store: &'y JMAPStore<T>,
    pub account_id: AccountId,
    pub properties: Vec<O::Property>,
    pub request: GetRequest<O, T>,
    pub response: GetResponse<O, T>,
    pub data: O::GetHelper,
}

pub trait GetObject<T>: Object
where
    T: for<'x> Store<'x> + 'static,
{
    type GetArguments;
    type GetHelper: Default;

    fn init_get(helper: &mut GetHelper<Self, T>) -> crate::Result<()>;
    fn get_item(helper: &mut GetHelper<Self, T>, jmap_id: JMAPId) -> crate::Result<Option<Self>>;
    fn map_ids<W>(store: &JMAPStore<T>, document_ids: W) -> crate::Result<Vec<JMAPId>>
    where
        W: Iterator<Item = DocumentId>;

    fn is_virtual() -> bool;
    fn default_properties() -> Vec<Self::Property>;
}

pub trait JMAPGet<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get<'y, 'z: 'y, O>(&'z self, request: GetRequest<O, T>) -> crate::Result<GetResponse<O, T>>
    where
        O: GetObject<T>;
}

impl<T> JMAPGet<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get<'y, 'z: 'y, O>(
        &'z self,
        mut request: GetRequest<O, T>,
    ) -> crate::Result<GetResponse<O, T>>
    where
        O: GetObject<T>,
    {
        let collection = O::collection();
        let is_virtual = O::is_virtual();
        let properties: Vec<O::Property> = request
            .properties
            .take()
            .unwrap_or_else(|| O::default_properties());

        let account_id = request.account_id.as_ref().unwrap().get_document_id();
        let document_ids = if !is_virtual {
            self.get_document_ids(account_id, collection)?
                .unwrap_or_default()
        } else {
            RoaringBitmap::new()
        };

        let request_ids = if let Some(request_ids) = request.ids.take() {
            if request_ids.len() > self.config.max_objects_in_get {
                return Err(MethodError::RequestTooLarge);
            } else {
                request_ids
            }
        } else if !document_ids.is_empty() {
            O::map_ids(
                self,
                document_ids.iter().take(self.config.max_objects_in_get),
            )?
        } else {
            Vec::new()
        };

        let mut helper = GetHelper {
            store: self,
            properties: if !properties.is_empty() {
                properties
            } else {
                O::default_properties()
            },
            response: GetResponse {
                account_id: request.account_id.clone(),
                state: self.get_state(account_id, collection)?,
                list: Vec::with_capacity(request_ids.len()),
                not_found: Vec::new(),
                _p: Default::default(),
            },
            account_id,
            request,
            data: O::GetHelper::default(),
        };

        O::init_get(&mut helper)?;

        for jmap_id in request_ids {
            if is_virtual || document_ids.contains(jmap_id.get_document_id()) {
                if let Some(result) = O::get_item(&mut helper, jmap_id)? {
                    helper.response.list.push(result);
                    continue;
                }
            }
            helper.response.not_found.push(jmap_id.into());
        }

        Ok(helper.response)
    }
}
