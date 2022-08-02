use jmap::jmap_store::raft::RaftObject;
use store::blob::BlobId;
use store::core::document::Document;
use store::core::error::StoreError;
use store::core::tag::Tag;
use store::core::JMAPIdPrefix;
use store::serialize::{StoreDeserialize, StoreSerialize};
use store::write::options::{IndexOptions, Options};
use store::{
    core::collection::Collection, write::batch::WriteBatch, AccountId, DocumentId, JMAPId,
    JMAPStore, Store,
};

use super::schema::Email;
use super::MessageData;
use super::MessageField;

impl<T> RaftObject<T> for Email
where
    T: for<'x> Store<'x> + 'static,
{
    fn on_raft_update(
        store: &JMAPStore<T>,
        write_batch: &mut WriteBatch,
        document: &mut Document,
        jmap_id: store::JMAPId,
        as_insert: Option<Vec<BlobId>>,
    ) -> store::Result<()> {
        if let Some(blobs) = as_insert {
            // First blobId contains the message metadata
            let metadata_blob_id = blobs.into_iter().next().ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to get message metadata blob for {}.",
                    document.document_id
                ))
            })?;

            // Build index from message metadata
            MessageData::deserialize(&store.blob_get(&metadata_blob_id)?.ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Could not find message metadata blob for {}.",
                    document.document_id
                ))
            })?)
            .ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to get deserialize message data for {}.",
                    document.document_id
                ))
            })?
            .build_index(document, true)?;

            // Add thread id
            let thread_id = jmap_id.get_prefix_id();
            document.tag(
                MessageField::ThreadId,
                Tag::Id(thread_id),
                IndexOptions::new(),
            );
            document.number(
                MessageField::ThreadId,
                thread_id,
                IndexOptions::new().store(),
            );

            // Link metadata blob
            document.binary(
                MessageField::Metadata,
                metadata_blob_id.serialize().unwrap(),
                IndexOptions::new(),
            );
            document.blob(metadata_blob_id, IndexOptions::new());
        } else {
            let thread_id = jmap_id.get_prefix_id();
            let current_thread_id = store
                .get_document_value::<DocumentId>(
                    write_batch.account_id,
                    Collection::Mail,
                    document.document_id,
                    MessageField::ThreadId.into(),
                )?
                .ok_or_else(|| {
                    StoreError::NotFound(format!(
                        "No thread id for document {}",
                        document.document_id
                    ))
                })?;

            if thread_id != current_thread_id {
                document.tag(
                    MessageField::ThreadId,
                    Tag::Id(thread_id),
                    IndexOptions::new(),
                );
                document.tag(
                    MessageField::ThreadId,
                    Tag::Id(current_thread_id),
                    IndexOptions::new().clear(),
                );
                document.number(
                    MessageField::ThreadId,
                    thread_id,
                    IndexOptions::new().store(),
                );
            }
        }
        Ok(())
    }

    fn get_jmap_id(
        store: &JMAPStore<T>,
        account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Option<store::JMAPId>> {
        Ok(store
            .get_document_value::<DocumentId>(
                account_id,
                Collection::Mail,
                document_id,
                MessageField::ThreadId.into(),
            )?
            .map(|thread_id| JMAPId::from_parts(thread_id, document_id)))
    }

    fn get_blobs(
        store: &JMAPStore<T>,
        account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Vec<BlobId>> {
        let mut blobs = vec![store
            .get_document_value(
                account_id,
                Collection::Mail,
                document_id,
                MessageField::Metadata.into(),
            )?
            .ok_or_else(|| {
                StoreError::NotFound(format!(
                    "Failed to get message metadata blobId for {}.",
                    document_id
                ))
            })?];
        let message_data =
            MessageData::deserialize(&store.blob_get(blobs.last().unwrap())?.ok_or_else(|| {
                StoreError::NotFound(format!(
                    "Failed to get message metadata blob for {}.",
                    document_id
                ))
            })?)
            .ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to get deserialize message data for {}.",
                    document_id
                ))
            })?;
        blobs.push(message_data.raw_message);
        Ok(blobs)
    }
}
