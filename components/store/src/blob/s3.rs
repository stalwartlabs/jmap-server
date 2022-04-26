use super::BlobStore;

pub struct S3BlobStore {}

impl BlobStore for S3BlobStore {
    fn new(settings: &crate::config::EnvSettings) -> crate::Result<Self> {
        todo!()
    }

    fn get_range(
        &self,
        blob_id: &super::BlobId,
        range: std::ops::Range<u32>,
    ) -> crate::Result<Option<Vec<u8>>> {
        todo!()
    }

    fn put(&self, blob_id: &super::BlobId, blob: &[u8]) -> crate::Result<bool> {
        todo!()
    }

    fn delete(&self, blob_id: &super::BlobId) -> crate::Result<bool> {
        todo!()
    }
}
