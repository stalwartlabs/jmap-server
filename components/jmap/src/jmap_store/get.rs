/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use std::sync::Arc;

use store::{core::acl::ACLToken, roaring::RoaringBitmap, AccountId, DocumentId, JMAPStore, Store};

use crate::{
    error::method::MethodError,
    request::{
        get::{GetRequest, GetResponse},
        ACLEnforce, ArgumentDeserializer,
    },
    types::jmap::JMAPId,
};

use super::{changes::JMAPChanges, Object};

pub struct GetHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: GetObject,
{
    pub store: &'y JMAPStore<T>,
    pub account_id: AccountId,
    pub acl: Arc<ACLToken>,
    pub document_ids: RoaringBitmap,
    pub properties: Vec<O::Property>,
    pub request_ids: Vec<JMAPId>,
    pub validate_ids: bool,
    pub request: GetRequest<O>,
    pub response: GetResponse<O>,
}

pub trait GetObject: Object {
    type GetArguments: Default + ArgumentDeserializer;

    fn get_as_id(&self, property: &Self::Property) -> Option<Vec<JMAPId>>;
    fn default_properties() -> Vec<Self::Property>;
}

pub fn default_mapper(document_ids: Vec<DocumentId>) -> crate::Result<Vec<JMAPId>> {
    Ok(document_ids.into_iter().map(|id| id.into()).collect())
}

pub type IdMapper = fn(Vec<DocumentId>) -> crate::Result<Vec<JMAPId>>;
pub type SharedDocsFnc = fn(AccountId, &[AccountId]) -> store::Result<Arc<Option<RoaringBitmap>>>;

impl<'y, O, T> GetHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: GetObject,
{
    pub fn new(
        store: &'y JMAPStore<T>,
        mut request: GetRequest<O>,
        id_mapper: Option<impl FnMut(Vec<DocumentId>) -> crate::Result<Vec<JMAPId>>>,
        shared_documents: Option<
            impl FnOnce(AccountId, &[AccountId]) -> store::Result<Arc<Option<RoaringBitmap>>>,
        >,
    ) -> crate::Result<Self> {
        let collection = O::collection();
        let validate_ids = id_mapper.is_some();
        let acl = request.acl.take().unwrap();
        let properties: Vec<O::Property> = request
            .properties
            .take()
            .and_then(|p| p.unwrap_value())
            .unwrap_or_else(|| O::default_properties());

        let account_id = request.account_id.get_document_id();
        let document_ids = if validate_ids {
            let mut document_ids = store
                .get_document_ids(account_id, collection)?
                .unwrap_or_default();
            match shared_documents {
                Some(fnc) if acl.is_shared(account_id) => {
                    if let Some(shared_ids) = fnc(account_id, &acl.member_of)?.as_ref() {
                        document_ids &= shared_ids;
                        document_ids
                    } else {
                        RoaringBitmap::new()
                    }
                }
                _ => {
                    debug_assert!(!acl.is_shared(account_id));
                    document_ids
                }
            }
        } else {
            RoaringBitmap::new()
        };

        let request_ids =
            if let Some(request_ids) = request.ids.take().and_then(|ids| ids.unwrap_value()) {
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
            properties,
            acl,
            response: GetResponse {
                account_id: request.account_id.into(),
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

    pub fn get(
        mut self,
        mut get_fnc: impl FnMut(JMAPId, &[O::Property]) -> crate::Result<Option<O>>,
    ) -> crate::Result<GetResponse<O>> {
        for id in self.request_ids {
            if !self.validate_ids || self.document_ids.contains(id.get_document_id()) {
                match get_fnc(id, &self.properties) {
                    Ok(Some(result)) => {
                        self.response.list.push(result);
                        continue;
                    }
                    Ok(None) | Err(MethodError::NotFound) => (),
                    Err(err) => {
                        return Err(err);
                    }
                }
            }
            self.response.not_found.push(id);
        }
        Ok(self.response)
    }
}
