use std::collections::{HashMap, HashSet};

use super::changes::JMAPChanges;
use super::Object;
use crate::error::set::SetError;
use crate::id::jmap::JMAPId;
use crate::id::state::JMAPState;
use crate::id::JMAPIdSerialize;
use crate::request::set::SetResponse;
use crate::{
    error::{method::MethodError, set::SetErrorType},
    protocol::{json::JSONValue, json_pointer::JSONPointer},
    request::set::SetRequest,
};
use store::core::collection::Collection;
use store::core::document::Document;
use store::core::JMAPIdPrefix;
use store::parking_lot::MutexGuard;
use store::write::batch::WriteBatch;
use store::AccountId;
use store::{roaring::RoaringBitmap, JMAPStore, Store};

pub struct SetHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: SetObject<T>,
{
    pub store: &'y JMAPStore<T>,
    pub lock: Option<MutexGuard<'y, ()>>,
    pub changes: WriteBatch,
    pub document_ids: RoaringBitmap,
    pub account_id: AccountId,

    pub request: SetRequest<O, T>,
    pub response: SetResponse<O, T>,

    pub data: O::SetHelper,
}

pub trait SetObject<T>: Object
where
    T: for<'x> Store<'x> + 'static,
{
    type SetArguments;
    type SetHelper: Default;
    type NextInvocation;

    fn init_set(helper: &mut SetHelper<Self, T>) -> crate::Result<()>;
    fn create(
        helper: &mut SetHelper<Self, T>,
        create_id: &str,
        document: &mut Document,
        item: Self,
    ) -> crate::error::set::Result<Self, Self::Property>;
    fn update(
        helper: &mut SetHelper<Self, T>,
        document: &mut Document,
        item: Self,
    ) -> crate::error::set::Result<Option<Self>, Self::Property>;
    fn validate_delete(
        helper: &mut SetHelper<Self, T>,
        id: JMAPId,
    ) -> crate::error::set::Result<(), Self::Property>;
    fn delete(
        store: &JMAPStore<T>,
        account_id: AccountId,
        document: &mut Document,
    ) -> store::Result<()>;
    fn map_references(&self, fnc: impl FnMut(&str) -> Option<JMAPId>);
}

pub trait JMAPSet<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn set<'y, 'z: 'y, O>(&'z self, request: SetRequest<O, T>) -> crate::Result<SetResponse<O, T>>
    where
        O: SetObject<T>;
}

impl<T> JMAPSet<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn set<'y, 'z: 'y, O>(
        &'z self,
        mut request: SetRequest<O, T>,
    ) -> crate::Result<SetResponse<O, T>>
    where
        O: SetObject<T>,
    {
        let collection = O::collection();
        let account_id = request.account_id.as_ref().unwrap().get_document_id();

        let old_state = self.get_state(account_id, collection)?;
        if let Some(if_in_state) = request.if_in_state.take() {
            if old_state != if_in_state {
                return Err(MethodError::StateMismatch);
            }
        }

        let mut helper = SetHelper {
            store: self,
            lock: None,
            changes: WriteBatch::new(account_id),
            document_ids: self
                .get_document_ids(account_id, collection)?
                .unwrap_or_else(RoaringBitmap::new),
            account_id,
            data: O::SetHelper::default(),
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
                _p: Default::default(),
                next_invocation: None,
            },
            request,
        };

        O::init_set(&mut helper)?;
        let mut change_id = None;
        for (create_id, item) in helper.request.create.take().unwrap_or_default() {
            let mut document = Document::new(
                collection,
                helper
                    .store
                    .assign_document_id(helper.account_id, collection)?,
            );

            match O::create(&mut helper, &create_id, &mut document, item) {
                Ok(result) => {
                    helper.document_ids.insert(document.document_id);
                    helper.changes.insert_document(document);
                    helper.changes.log_insert(collection, result.id().unwrap());
                    if helper.lock.is_some() {
                        change_id = self.write(helper.changes)?;
                        helper.changes = WriteBatch::new(account_id);
                        helper.lock = None;
                    }
                    helper.response.created.insert(create_id, result.into());
                }
                Err(err) => {
                    helper.response.not_created.insert(create_id, err.into());
                }
            };
        }

        for (id, item) in helper.request.update.take().unwrap_or_default() {
            let document_id = id.get_document_id();
            if !helper.document_ids.contains(document_id) {
                helper
                    .response
                    .not_updated
                    .insert(id, SetError::new(SetErrorType::NotFound, "ID not found."));
                continue;
            } else if helper
                .request
                .destroy
                .as_ref()
                .map_or(false, |l| l.contains(&id))
            {
                helper.response.not_updated.insert(
                    id,
                    SetError::new(SetErrorType::WillDestroy, "ID will be destroyed.").into(),
                );
                continue;
            }

            let mut document = Document::new(collection, document_id);
            match O::update(&mut helper, &mut document, item) {
                Ok(Some(result)) => {
                    helper.changes.update_document(document);
                    helper.changes.log_update(collection, id);
                    helper.response.updated.insert(id, result.into());
                }
                Ok(None) => {
                    helper.response.updated.insert(id, None);
                }
                Err(err) => {
                    helper.response.not_updated.insert(id, err.into());
                }
            };
        }

        for id in helper.request.destroy.take().unwrap_or_default() {
            let document_id = id.get_document_id();
            if helper.document_ids.contains(document_id) {
                if let Err(err) = O::validate_delete(&mut helper, id) {
                    helper.response.not_destroyed.insert(id, err.into());
                } else {
                    let mut document = Document::new(collection, document_id);
                    O::delete(self, helper.account_id, &mut document)?;
                    if !self.tombstone_deletions() {
                        helper.changes.delete_document(document);
                    } else {
                        helper.changes.tombstone_document(document);
                    }
                    helper.changes.log_delete(collection, id);
                    helper.response.destroyed.push(id.into());
                }
            } else {
                helper.response.not_destroyed.insert(
                    id,
                    SetError::new(SetErrorType::NotFound, "ID not found.").into(),
                );
            }
        }

        if !helper.changes.is_empty() {
            change_id = self.write(helper.changes)?;
        }
        if let Some(change_id) = change_id {
            helper.response.new_state = JMAPState::from(change_id).into()
        }

        Ok(helper.response)
    }
}

impl<'y, O, T> SetHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: SetObject<T>,
{
    pub fn lock(&mut self, collection: Collection) {
        self.lock = self.store.lock_account(self.account_id, collection).into();
    }
}
