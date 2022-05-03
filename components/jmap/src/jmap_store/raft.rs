use super::{
    get::GetObject,
    orm::{JMAPOrm, PropertySchema, TinyORM},
    set::SetObject,
};
use crate::Property;
use store::{
    blob::BlobId,
    core::{document::Document, error::StoreError, JMAPIdPrefix},
    serialize::{StoreDeserialize, StoreSerialize},
    write::{batch::WriteBatch, options::IndexOptions},
    AccountId, DocumentId, JMAPId, JMAPStore, Store,
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

pub trait JMAPRaftStore<T>
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
        U: RaftObject<T>;

    fn raft_apply_update<U>(
        &self,
        write_batch: &mut WriteBatch,
        update: RaftUpdate,
    ) -> store::Result<()>
    where
        U: RaftObject<T>;
}

impl<T> JMAPRaftStore<T> for JMAPStore<T>
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
        U: RaftObject<T>,
    {
        Ok(
            if let (Some(fields), Some(jmap_id)) = (
                self.get_orm::<U::Property>(account_id, document_id)?,
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
                            U::Property::collection(),
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

    fn raft_apply_update<U>(
        &self,
        write_batch: &mut WriteBatch,
        update: RaftUpdate,
    ) -> store::Result<()>
    where
        U: RaftObject<T>,
    {
        match update {
            RaftUpdate::Insert {
                jmap_id,
                fields,
                blobs,
                term_index,
            } => {
                let document_id = jmap_id.get_document_id();
                let mut document = Document::new(U::Property::collection(), document_id);
                TinyORM::<U::Property>::deserialize(&fields)
                    .ok_or_else(|| {
                        StoreError::InternalError("Failed to deserialize ORM.".to_string())
                    })?
                    .insert(&mut document)?;
                if let Some(term_index) = term_index {
                    document.term_index(term_index, IndexOptions::new());
                }

                U::on_raft_update(self, write_batch, &mut document, jmap_id, blobs.into())?;

                write_batch.insert_document(document);
            }
            RaftUpdate::Update { jmap_id, fields } => {
                let document_id = jmap_id.get_document_id();
                let mut document = Document::new(U::Property::collection(), document_id);
                self.get_orm::<U::Property>(write_batch.account_id, document_id)?
                    .ok_or_else(|| {
                        StoreError::InternalError(format!(
                            "ORM for document {:?}/{} not found.",
                            U::Property::collection(),
                            document_id
                        ))
                    })?
                    .merge(
                        &mut document,
                        TinyORM::<U::Property>::deserialize(&fields).ok_or_else(|| {
                            StoreError::InternalError("Failed to deserialize ORM.".to_string())
                        })?,
                    )?;

                U::on_raft_update(self, write_batch, &mut document, jmap_id, None)?;

                if !document.is_empty() {
                    write_batch.update_document(document);
                }
            }
        }

        Ok(())
    }
}

pub trait RaftObject<T>
where
    T: for<'x> Store<'x> + 'static,
{
    type Property: PropertySchema + 'static;

    fn on_raft_update(
        store: &JMAPStore<T>,
        write_batch: &mut WriteBatch,
        document: &mut Document,
        jmap_id: JMAPId,
        as_insert: Option<Vec<BlobId>>,
    ) -> store::Result<()>;

    fn get_jmap_id(
        store: &JMAPStore<T>,
        account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Option<JMAPId>>;

    fn get_blobs(
        store: &JMAPStore<T>,
        account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Vec<BlobId>>;
}
