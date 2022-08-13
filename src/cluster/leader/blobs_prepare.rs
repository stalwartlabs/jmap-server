use crate::cluster::log::Update;
use crate::JMAPServer;
use store::blob::BlobId;
use store::core::error::StoreError;
use store::Store;

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn prepare_blobs(
        &self,
        pending_blob_ids: Vec<BlobId>,
        max_batch_size: usize,
    ) -> store::Result<(Vec<Update>, Vec<BlobId>)> {
        let store = self.store.clone();
        self.spawn_worker(move || {
            let mut remaining_blobs = Vec::new();
            let mut updates = Vec::new();
            let mut bytes_sent = 0;

            for pending_blob_id in pending_blob_ids {
                if bytes_sent < max_batch_size {
                    let blob = store::lz4_flex::compress_prepend_size(
                        &store.blob_get(&pending_blob_id)?.ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Blob {} not found.",
                                pending_blob_id
                            ))
                        })?,
                    );
                    bytes_sent += blob.len();
                    updates.push(Update::Blob {
                        blob_id: pending_blob_id,
                        blob,
                    });
                } else {
                    remaining_blobs.push(pending_blob_id);
                }
            }

            Ok((updates, remaining_blobs))
        })
        .await
    }
}
