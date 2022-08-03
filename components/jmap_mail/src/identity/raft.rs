use jmap::jmap_store::raft::RaftObject;
use store::{
    blob::BlobId,
    core::{collection::Collection, error::StoreError},
    write::{batch::WriteBatch, options::IndexOptions},
    AccountId, DocumentId, JMAPId, JMAPStore, Store,
};

use super::schema::Identity;
use crate::email_submission::schema::Property;
use store::serialize::StoreSerialize;

impl<T> RaftObject<T> for Identity
where
    T: for<'x> Store<'x> + 'static,
{
    fn on_raft_update(
        _store: &JMAPStore<T>,
        _write_batch: &mut WriteBatch,
        document: &mut store::core::document::Document,
        _jmap_id: store::JMAPId,
        as_insert: Option<Vec<BlobId>>,
    ) -> store::Result<()> {
        if let Some(blobs) = as_insert {
            // First blobId contains the email
            let email_blob_id = blobs.into_iter().next().ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to get message email blob for {}.",
                    document.document_id
                ))
            })?;

            // Link metadata blob
            document.binary(
                Property::EmailId,
                email_blob_id.serialize().unwrap(),
                IndexOptions::new(),
            );
            document.blob(email_blob_id, IndexOptions::new());
        }
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
        store: &JMAPStore<T>,
        account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Vec<store::blob::BlobId>> {
        Ok(vec![store
            .get_document_value(
                account_id,
                Collection::EmailSubmission,
                document_id,
                Property::EmailId.into(),
            )?
            .ok_or_else(|| {
                StoreError::NotFound(format!(
                    "Failed to get message email blobId for {}.",
                    document_id
                ))
            })?])
    }
}
