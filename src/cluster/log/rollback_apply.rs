use super::Update;
use crate::cluster::log::update_apply::RaftStoreApplyUpdate;
use crate::JMAPServer;
use store::core::collection::Collection;
use store::write::batch::WriteBatch;
use store::{tracing::debug, AccountId, Store};

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn apply_rollback_updates(&self, updates: Vec<Update>) -> store::Result<bool> {
        let store = self.store.clone();
        self.spawn_worker(move || {
            let mut write_batch = WriteBatch::new(AccountId::MAX);

            debug!("Inserting {} rollback changes...", updates.len(),);
            let mut is_done = false;
            let mut account_id = AccountId::MAX;
            let mut collection = Collection::None;

            for update in updates {
                match update {
                    Update::Begin {
                        account_id: update_account_id,
                        collection: update_collection,
                    } => {
                        account_id = update_account_id;
                        collection = update_collection;
                    }
                    Update::Document { update } => {
                        debug_assert!(
                            account_id != AccountId::MAX && collection != Collection::None
                        );

                        if account_id != write_batch.account_id {
                            if !write_batch.is_empty() {
                                store.write(write_batch)?;
                                write_batch = WriteBatch::new(account_id);
                            } else {
                                write_batch.account_id = account_id;
                            }
                        }

                        store.apply_update(&mut write_batch, collection, update)?;
                    }
                    Update::Eof => {
                        is_done = true;
                    }
                    _ => debug_assert!(false, "Invalid update type: {:?}", update),
                }
            }
            if !write_batch.is_empty() {
                store.write(write_batch)?;
            }

            Ok(is_done)
        })
        .await
    }
}
