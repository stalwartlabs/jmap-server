use crate::JMAPServer;
use store::core::collection::Collection;
use store::serialize::key::LogKey;
use store::{AccountId, ColumnFamily, JMAPStore, Store};

pub trait RaftStoreRollbackRemove {
    fn remove_rollback_change(
        &self,
        account_id: AccountId,
        collection: Collection,
    ) -> store::Result<()>;
}

impl<T> RaftStoreRollbackRemove for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn remove_rollback_change(
        &self,
        account_id: AccountId,
        collection: Collection,
    ) -> store::Result<()> {
        self.db.delete(
            ColumnFamily::Logs,
            &LogKey::serialize_rollback(account_id, collection),
        )
    }
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn remove_rollback_change(
        &self,
        account_id: AccountId,
        collection: Collection,
    ) -> store::Result<()> {
        let store = self.store.clone();
        self.spawn_worker(move || store.remove_rollback_change(account_id, collection))
            .await
    }
}
