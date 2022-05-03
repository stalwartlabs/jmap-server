use jmap::jmap_store::raft::{RaftObject, RaftUpdate};
use store::{write::batch::WriteBatch, AccountId, DocumentId, JMAPStore, Store};

use super::set::SetMailbox;

impl<T> RaftObject<T> for SetMailbox
where
    T: for<'x> Store<'x> + 'static,
{
    fn raft_prepare_update(
        store: &JMAPStore<T>,
        document_id: DocumentId,
        as_insert: bool,
    ) -> store::Result<Option<RaftUpdate>> {
        Ok(None)
    }

    fn raft_apply_update(
        store: &JMAPStore<T>,
        write_batch: &mut WriteBatch,
        account_id: AccountId,
        update: RaftUpdate,
    ) -> store::Result<()> {
        Ok(())
    }
}
