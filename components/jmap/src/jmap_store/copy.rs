use std::{collections::HashMap, sync::Arc};

use store::{
    core::{acl::ACLToken, collection::Collection, document::Document},
    log::changes::ChangeId,
    parking_lot::MutexGuard,
    roaring::RoaringBitmap,
    write::batch::WriteBatch,
    AccountId, JMAPStore, Store,
};

use crate::{
    error::{
        method::MethodError,
        set::{SetError, SetErrorType},
    },
    request::copy::{CopyRequest, CopyResponse},
    types::{jmap::JMAPId, state::JMAPState, type_state::TypeState},
};

use super::{changes::JMAPChanges, set::SetObject};

pub struct CopyHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: SetObject,
{
    pub store: &'y JMAPStore<T>,
    pub changes: WriteBatch,
    pub from_account_id: AccountId,
    pub document_ids: RoaringBitmap,
    pub account_id: AccountId,
    pub acl: Arc<ACLToken>,
    pub collection: Collection,

    pub change_id: ChangeId,
    pub state_changes: Vec<(TypeState, ChangeId)>,

    pub request: CopyRequest<O>,
    pub response: CopyResponse<O>,
}

impl<'y, O, T> CopyHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: SetObject,
{
    pub fn new(store: &'y JMAPStore<T>, mut request: CopyRequest<O>) -> crate::Result<Self> {
        let collection = O::collection();
        let account_id = request.account_id.get_document_id();
        let from_account_id = request.from_account_id.get_document_id();

        if account_id == from_account_id {
            return Err(MethodError::InvalidArguments(
                "From accountId is equal to fromAccountId".to_string(),
            ));
        }

        let old_state = store.get_state(account_id, collection)?;
        if let Some(if_in_state) = request.if_in_state.take() {
            if old_state != if_in_state {
                return Err(MethodError::StateMismatch);
            }
        }

        Ok(CopyHelper {
            store,
            changes: WriteBatch::new(account_id),
            account_id,
            acl: request.acl.take().unwrap(),
            from_account_id,
            collection,
            document_ids: store
                .get_document_ids(from_account_id, collection)?
                .unwrap_or_default(),
            change_id: ChangeId::MAX,
            state_changes: Vec::new(),
            response: CopyResponse {
                account_id: request.account_id,
                from_account_id: request.from_account_id,
                new_state: old_state.clone(),
                old_state,
                created: HashMap::with_capacity(request.create.len()),
                not_created: HashMap::new(),
                next_call: None,
                change_id: None,
                state_changes: None,
            },
            request,
        })
    }

    pub fn create(
        &mut self,
        mut create_fnc: impl FnMut(
            &JMAPId,
            O,
            &mut Self,
            &mut Document,
        ) -> crate::error::set::Result<
            (O, Option<MutexGuard<'y, ()>>),
            O::Property,
        >,
    ) -> crate::Result<()> {
        for (create_id, item) in std::mem::take(&mut self.request.create) {
            let create_id = create_id.unwrap_value().unwrap_or_else(JMAPId::singleton);
            // Validate id
            if !self.document_ids.contains(create_id.get_document_id()) {
                self.response.not_created.insert(
                    create_id,
                    SetError::new(
                        SetErrorType::NotFound,
                        format!(
                            "Item {} not found not found in account {}.",
                            create_id, self.response.from_account_id
                        ),
                    ),
                );
                continue;
            }
            let mut document = Document::new(
                self.collection,
                self.store
                    .assign_document_id(self.account_id, self.collection)?,
            );

            match create_fnc(&create_id, item, self, &mut document) {
                Ok((result, lock)) => {
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

    pub fn into_response(mut self) -> crate::Result<CopyResponse<O>> {
        if !self.changes.is_empty() {
            self.write()?;
        }
        if self.change_id != ChangeId::MAX {
            self.response.new_state = JMAPState::from(self.change_id);
            self.response.change_id = self.change_id.into();
            if !self.state_changes.is_empty() {
                self.response.state_changes = self.state_changes.into();
            }
        }

        Ok(self.response)
    }
}
