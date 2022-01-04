use std::sync::MutexGuard;

use store::{mutex_map::MutexMap, AccountId, CollectionId, Store, StoreError};

use crate::changes::JMAPState;

pub struct JMAPLocalStore<T> {
    pub store: T,
    pub account_lock: MutexMap,
}

impl<'x, T> JMAPLocalStore<T>
where
    T: Store<'x>,
{
    pub fn new(store: T) -> JMAPLocalStore<T> {
        JMAPLocalStore {
            store,
            account_lock: MutexMap::with_capacity(1024),
        }
    }

    pub fn lock_account(&self, account: AccountId) -> store::Result<MutexGuard<usize>> {
        self.account_lock
            .lock(account)
            .map_err(|_| StoreError::InternalError("Failed to obtain mutex".to_string()))
    }

    pub fn get_store(&self) -> &T {
        &self.store
    }

    pub fn get_state(
        &self,
        account: AccountId,
        collection: CollectionId,
    ) -> store::Result<JMAPState> {
        Ok(self
            .store
            .get_last_change_id(account, collection)?
            .map(JMAPState::Exact)
            .unwrap_or(JMAPState::Initial))
    }
}
