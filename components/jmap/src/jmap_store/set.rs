use std::collections::HashMap;

use super::changes::JMAPChanges;
use super::Object;
use crate::error::set::SetError;
use crate::id::jmap::JMAPId;
use crate::id::state::JMAPState;
use crate::protocol::type_state::TypeState;
use crate::request::set::SetResponse;
use crate::{
    error::{method::MethodError, set::SetErrorType},
    request::set::SetRequest,
};
use store::core::collection::Collection;
use store::core::document::Document;

use store::log::changes::ChangeId;
use store::parking_lot::MutexGuard;
use store::write::batch::WriteBatch;
use store::AccountId;
use store::{roaring::RoaringBitmap, JMAPStore, Store};

pub trait SetObject: Object {
    type SetArguments;
    type NextCall;

    fn map_references(&mut self, fnc: impl FnMut(&str) -> Option<JMAPId>);
}

pub struct SetHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: SetObject,
{
    pub store: &'y JMAPStore<T>,
    pub changes: WriteBatch,
    pub document_ids: RoaringBitmap,
    pub account_id: AccountId,
    pub collection: Collection,

    pub change_id: ChangeId,
    pub state_changes: HashMap<TypeState, ChangeId>,

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
        let account_id = request.account_id.as_ref().unwrap().get_document_id();

        let old_state = store.get_state(account_id, collection)?;
        if let Some(if_in_state) = request.if_in_state.take() {
            if old_state != if_in_state {
                return Err(MethodError::StateMismatch);
            }
        }
        Ok(SetHelper {
            store,
            changes: WriteBatch::new(account_id),
            document_ids: store
                .get_document_ids(account_id, collection)?
                .unwrap_or_else(RoaringBitmap::new),
            account_id,
            collection,
            change_id: ChangeId::MAX,
            state_changes: HashMap::new(),
            response: SetResponse {
                account_id: request.account_id.take(),
                new_state: old_state.clone().into(),
                old_state: old_state.into(),
                created: HashMap::with_capacity(request.create.as_ref().map_or(0, |v| v.len())),
                not_created: HashMap::new(),
                updated: HashMap::with_capacity(request.update.as_ref().map_or(0, |v| v.len())),
                not_updated: HashMap::new(),
                destroyed: Vec::with_capacity(request.destroy.as_ref().map_or(0, |v| v.len())),
                not_destroyed: HashMap::new(),
                next_call: None,
                change_id: None,
                state_changes: None,
            },
            request,
        })
    }

    pub fn map_references(&self, create_id: &str) -> Option<JMAPId> {
        self.response
            .created
            .get(create_id)
            .and_then(|o| o.id())
            .cloned()
    }

    pub fn create(
        &mut self,
        mut create_fnc: impl FnMut(
            &str,
            O,
            &mut Self,
            &mut Document,
        ) -> crate::error::set::Result<
            (O, Option<MutexGuard<'y, ()>>),
            O::Property,
        >,
    ) -> crate::Result<()> {
        for (create_id, mut item) in self.request.create.take().unwrap_or_default() {
            let mut document = Document::new(
                self.collection,
                self.store
                    .assign_document_id(self.account_id, self.collection)?,
            );

            item.map_references(|create_id| self.map_references(create_id));

            match create_fnc(&create_id, item, self, &mut document) {
                Ok((result, lock)) => {
                    self.document_ids.insert(document.document_id);
                    self.changes.insert_document(document);
                    self.changes
                        .log_insert(self.collection, result.id().unwrap());
                    if lock.is_some() {
                        self.write()?;
                    }
                    self.response.created.insert(create_id, result);
                }
                Err(err) => {
                    self.response.not_created.insert(create_id, err);
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
        for (id, mut item) in self.request.update.take().unwrap_or_default() {
            let document_id = id.get_document_id();
            if !self.document_ids.contains(document_id) {
                self.response
                    .not_updated
                    .insert(id, SetError::new(SetErrorType::NotFound, "ID not found."));
                continue;
            } else if self
                .request
                .destroy
                .as_ref()
                .map_or(false, |l| l.contains(&id))
            {
                self.response.not_updated.insert(
                    id,
                    SetError::new(SetErrorType::WillDestroy, "ID will be destroyed."),
                );
                continue;
            }

            let mut document = Document::new(self.collection, document_id);
            item.map_references(|create_id| self.map_references(create_id));

            match update_fnc(id, item, self, &mut document) {
                Ok(result) => {
                    if !document.is_empty() {
                        self.changes.update_document(document);
                        self.changes.log_update(self.collection, id);
                    }
                    self.response.updated.insert(id, result);
                }
                Err(err) => {
                    self.response.not_updated.insert(id, err);
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
        for id in self.request.destroy.take().unwrap_or_default() {
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
                        self.response.not_destroyed.insert(id, err);
                    }
                };
            } else {
                self.response
                    .not_destroyed
                    .insert(id, SetError::new(SetErrorType::NotFound, "ID not found."));
            }
        }
        Ok(())
    }

    fn write(&mut self) -> crate::Result<()> {
        if let Some(changes) = self.store.write(self.changes.take())? {
            self.change_id = changes.change_id;
            for collection in changes.collections {
                if let Ok(type_state) = TypeState::try_from(collection) {
                    self.state_changes.insert(type_state, changes.change_id);
                }
            }
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
}
