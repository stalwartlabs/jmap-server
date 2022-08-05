use super::RaftUpdate;
use jmap::{jmap_store::RaftObject, orm::serialize::JMAPOrm};
use store::serialize::StoreSerialize;
use store::{core::error::StoreError, AccountId, DocumentId, JMAPStore, Store};

pub trait RaftStorePrepareUpdate<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn raft_prepare_update<U>(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
        as_insert: bool,
    ) -> store::Result<Option<RaftUpdate>>
    where
        U: RaftObject<T> + 'static;
}

impl<T> RaftStorePrepareUpdate<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn raft_prepare_update<U>(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
        as_insert: bool,
    ) -> store::Result<Option<RaftUpdate>>
    where
        U: RaftObject<T> + 'static,
    {
        Ok(
            if let (Some(fields), Some(jmap_id)) = (
                self.get_orm::<U>(account_id, document_id)?,
                U::get_jmap_id(self, account_id, document_id)?,
            ) {
                let fields = fields.serialize().ok_or_else(|| {
                    StoreError::SerializeError("Failed to serialize ORM.".to_string())
                })?;

                Some(if as_insert {
                    RaftUpdate::Insert {
                        blobs: U::get_blobs(self, account_id, document_id)?,
                        term_index: self.get_term_index_id(
                            account_id,
                            U::collection(),
                            document_id,
                        )?,
                        jmap_id,
                        fields,
                    }
                } else {
                    RaftUpdate::Update { jmap_id, fields }
                })
            } else {
                None
            },
        )
    }
}
