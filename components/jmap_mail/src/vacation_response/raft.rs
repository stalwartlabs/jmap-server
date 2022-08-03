use jmap::jmap_store::raft::RaftObject;
use store::{
    blob::BlobId, write::batch::WriteBatch, AccountId, DocumentId, JMAPId, JMAPStore, Store,
};

use super::schema::VacationResponse;

impl<T> RaftObject<T> for VacationResponse
where
    T: for<'x> Store<'x> + 'static,
{
    fn on_raft_update(
        _store: &JMAPStore<T>,
        _write_batch: &mut WriteBatch,
        _document: &mut store::core::document::Document,
        _jmap_id: store::JMAPId,
        _as_insert: Option<Vec<BlobId>>,
    ) -> store::Result<()> {
        Ok(())
    }

    fn get_jmap_id(
        _store: &JMAPStore<T>,
        _account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Option<store::JMAPId>> {
        Ok((document_id as JMAPId).into())
    }

    fn get_blobs(
        _store: &JMAPStore<T>,
        _account_id: AccountId,
        _document_id: DocumentId,
    ) -> store::Result<Vec<store::blob::BlobId>> {
        Ok(Vec::with_capacity(0))
    }
}
