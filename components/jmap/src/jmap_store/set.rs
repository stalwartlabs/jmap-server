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

use super::changes::JMAPChanges;
use super::Object;
use crate::error::set::SetError;
use crate::request::set::SetResponse;
use crate::request::{ArgumentDeserializer, MaybeIdReference, ResultReference};
use crate::types::jmap::JMAPId;
use crate::types::state::JMAPState;
use crate::types::type_state::TypeState;
use crate::{
    error::{method::MethodError, set::SetErrorType},
    request::set::SetRequest,
};
use store::ahash::AHashMap;
use store::core::acl::ACLToken;
use store::core::collection::Collection;
use store::core::document::Document;

use store::core::vec_map::VecMap;
use store::log::changes::ChangeId;
use store::parking_lot::MutexGuard;
use store::write::batch::WriteBatch;
use store::AccountId;
use store::{roaring::RoaringBitmap, JMAPStore, Store};

pub trait SetObject: Object {
    type SetArguments: Default + ArgumentDeserializer;
    type NextCall;

    fn set_property(&mut self, property: Self::Property, value: Self::Value);
    fn eval_id_references(&mut self, fnc: impl FnMut(&str) -> Option<JMAPId>);
    fn eval_result_references(&mut self, fnc: impl FnMut(&ResultReference) -> Option<Vec<u64>>);
}

pub struct SetHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: SetObject,
{
    pub store: &'y JMAPStore<T>,
    pub lock: MutexGuard<'y, ()>,
    pub changes: WriteBatch,
    pub document_ids: RoaringBitmap,
    pub account_id: AccountId,
    pub acl: Arc<ACLToken>,
    pub collection: Collection,
    pub will_destroy: Vec<JMAPId>,
    pub batch_writes: bool,

    pub change_id: ChangeId,
    pub state_changes: Vec<(TypeState, ChangeId)>,

    pub request: SetRequest<O>,
    pub response: SetResponse<O>,
}

impl<'y, O, T> SetHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: SetObject,
{
    pub fn new(store: &'y JMAPStore<T>, mut request: SetRequest<O>) -> crate::Result<Self> {
        let collection = O::collection();
        let account_id = request.account_id.get_document_id();

        let old_state = store.get_state(account_id, collection)?;
        if let Some(if_in_state) = request.if_in_state.take() {
            if old_state != if_in_state {
                return Err(MethodError::StateMismatch);
            }
        }
        let will_destroy = request
            .destroy
            .take()
            .and_then(|d| d.unwrap_value())
            .unwrap_or_default();
        Ok(SetHelper {
            store,
            lock: store.lock_collection(account_id, collection),
            changes: WriteBatch::new(account_id),
            document_ids: store
                .get_document_ids(account_id, collection)?
                .unwrap_or_else(RoaringBitmap::new),
            account_id,
            acl: request.acl.take().unwrap(),
            collection,
            change_id: ChangeId::MAX,
            state_changes: Vec::new(),
            batch_writes: true,
            response: SetResponse {
                account_id: request.account_id.into(),
                new_state: old_state.clone().into(),
                old_state: old_state.into(),
                created: AHashMap::with_capacity(request.create.as_ref().map_or(0, |v| v.len())),
                not_created: VecMap::with_capacity(0),
                updated: VecMap::with_capacity(request.update.as_ref().map_or(0, |v| v.len())),
                not_updated: VecMap::with_capacity(0),
                destroyed: Vec::with_capacity(will_destroy.len()),
                not_destroyed: VecMap::with_capacity(0),
                next_call: None,
                change_id: None,
                state_changes: None,
            },
            will_destroy,
            request,
        })
    }

    pub fn disable_write_batch(&mut self) {
        self.batch_writes = false;
    }

    pub fn map_id_reference(&self, create_id: &str) -> Option<JMAPId> {
        self.response
            .created
            .get(create_id)
            .and_then(|o| o.id())
            .cloned()
    }

    pub fn unwrap_id_reference(
        &self,
        property: O::Property,
        id: &MaybeIdReference,
    ) -> crate::error::set::Result<JMAPId, O::Property> {
        Ok(match id {
            MaybeIdReference::Value(id) => *id,
            MaybeIdReference::Reference(create_id) => {
                self.map_id_reference(create_id).ok_or_else(|| {
                    SetError::invalid_properties()
                        .with_property(property)
                        .with_description(format!("Could not find id '{}'.", create_id))
                })?
            }
        })
    }

    pub fn get_id_reference(
        &self,
        property: O::Property,
        id: &str,
    ) -> crate::error::set::Result<JMAPId, O::Property> {
        self.map_id_reference(id).ok_or_else(|| {
            SetError::invalid_properties()
                .with_property(property)
                .with_description(format!("Could not find id '{}'.", id))
        })
    }

    pub fn create(
        &mut self,
        mut create_fnc: impl FnMut(
            &str,
            O,
            &mut Self,
            &mut Document,
        ) -> crate::error::set::Result<O, O::Property>,
    ) -> crate::Result<()> {
        for (create_id, item) in self.request.create.take().unwrap_or_default() {
            let mut document = Document::new(
                self.collection,
                self.store
                    .assign_document_id(self.account_id, self.collection)?,
            );

            match create_fnc(&create_id, item, self, &mut document) {
                Ok(result) => {
                    self.document_ids.insert(document.document_id);
                    self.changes.insert_document(document);
                    self.changes
                        .log_insert(self.collection, result.id().unwrap());
                    if !self.batch_writes {
                        self.write()?;
                    }
                    self.response.created.insert(create_id, result);
                }
                Err(err) => {
                    self.response.not_created.append(create_id, err);
                }
            };
        }
        Ok(())
    }

    pub fn update(
        &mut self,
        mut update_fnc: impl FnMut(
            JMAPId,
            O,
            &mut Self,
            &mut Document,
        ) -> crate::error::set::Result<Option<O>, O::Property>,
    ) -> crate::Result<()> {
        for (id, item) in self.request.update.take().unwrap_or_default() {
            let document_id = id.get_document_id();
            if !self.document_ids.contains(document_id) {
                self.response.not_updated.append(
                    id,
                    SetError::new(SetErrorType::NotFound).with_description("ID not found."),
                );
                continue;
            } else if self.will_destroy.contains(&id) {
                self.response.not_updated.append(
                    id,
                    SetError::new(SetErrorType::WillDestroy)
                        .with_description("ID will be destroyed."),
                );
                continue;
            }

            let mut document = Document::new(self.collection, document_id);
            match update_fnc(id, item, self, &mut document) {
                Ok(result) => {
                    if !document.is_empty() {
                        self.changes.update_document(document);
                        self.changes.log_update(self.collection, id);
                    }
                    self.response.updated.append(id, result);
                }
                Err(err) => {
                    self.response.not_updated.append(id, err);
                }
            };
        }
        Ok(())
    }

    pub fn destroy(
        &mut self,
        mut destroy_fnc: impl FnMut(
            JMAPId,
            &mut Self,
            &mut Document,
        ) -> crate::error::set::Result<(), O::Property>,
    ) -> crate::Result<()> {
        for id in std::mem::take(&mut self.will_destroy) {
            let document_id = id.get_document_id();
            if self.document_ids.contains(document_id) {
                let mut document = Document::new(self.collection, document_id);
                match destroy_fnc(id, self, &mut document) {
                    Ok(_) => {
                        self.changes.delete_document(document);
                        self.changes.log_delete(self.collection, id);
                        self.response.destroyed.push(id);
                    }
                    Err(err) => {
                        self.response.not_destroyed.append(id, err);
                    }
                };
            } else {
                self.response.not_destroyed.append(
                    id,
                    SetError::new(SetErrorType::NotFound).with_description("ID not found."),
                );
            }
        }
        Ok(())
    }

    fn write(&mut self) -> crate::Result<()> {
        if let Some(changes) = self.store.write(self.changes.take())? {
            self.change_id = changes.change_id;
            for collection in changes.collections {
                if let Ok(type_state) = TypeState::try_from(collection) {
                    if let Some(entry) = self.state_changes.iter_mut().find(|e| e.0 == type_state) {
                        entry.1 = changes.change_id;
                    } else {
                        self.state_changes.push((type_state, changes.change_id));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn commit_changes(&mut self) -> crate::Result<()> {
        if !self.changes.is_empty() {
            self.write()?;
            self.changes = WriteBatch::new(self.account_id);
        }
        Ok(())
    }

    pub fn into_response(mut self) -> crate::Result<SetResponse<O>> {
        if !self.changes.is_empty() {
            self.write()?;
        }
        if self.change_id != ChangeId::MAX {
            self.response.new_state = JMAPState::from(self.change_id).into();
            self.response.change_id = self.change_id.into();
            if !self.state_changes.is_empty() {
                self.response.state_changes = self.state_changes.into();
            }
        }

        Ok(self.response)
    }

    pub fn set_created_property(
        &mut self,
        create_id: &str,
        property: O::Property,
        value: O::Value,
    ) {
        for (id, response) in &mut self.response.created {
            if id == create_id {
                response.set_property(property, value);
                return;
            }
        }
    }

    pub fn set_updated_property(&mut self, set_id: JMAPId, property: O::Property, value: O::Value) {
        for (id, response) in self.response.updated.iter_mut() {
            if id == &set_id {
                response
                    .get_or_insert_with(O::default)
                    .set_property(property, value);
                return;
            }
        }

        let mut response = O::default();
        response.set_property(property, value);
        self.response.updated.set(set_id, response.into());
    }
}
