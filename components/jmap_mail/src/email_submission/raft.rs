/*
impl<T> RaftObject<T> for SetEmailSubmission
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
*/
