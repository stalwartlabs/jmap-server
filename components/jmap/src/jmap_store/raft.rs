use store::{
    blob::BlobId, write::batch::WriteBatch, AccountId, DocumentId, JMAPId, JMAPStore, Store,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum RaftUpdate {
    Insert {
        jmap_id: JMAPId,
        fields: Vec<u8>,
        blobs: Vec<BlobId>,
        term_index: Option<BlobId>,
    },
    Update {
        jmap_id: JMAPId,
        fields: Vec<u8>,
    },
}

impl RaftUpdate {
    pub fn size(&self) -> usize {
        match self {
            RaftUpdate::Insert {
                fields,
                blobs,
                term_index,
                ..
            } => {
                fields.len()
                    + std::mem::size_of::<JMAPId>()
                    + ((blobs.len() + term_index.as_ref().map(|_| 1).unwrap_or(0))
                        * std::mem::size_of::<BlobId>())
            }
            RaftUpdate::Update { fields, .. } => fields.len() + std::mem::size_of::<JMAPId>(),
        }
    }
}

pub trait RaftObject<T>: Sized
where
    T: for<'x> Store<'x> + 'static,
{
    fn raft_prepare_update(
        store: &JMAPStore<T>,
        document_id: DocumentId,
        as_insert: bool,
    ) -> store::Result<Option<RaftUpdate>>;
    fn raft_apply_update(
        store: &JMAPStore<T>,
        write_batch: &mut WriteBatch,
        account_id: AccountId,
        update: RaftUpdate,
    ) -> store::Result<()>;
}
