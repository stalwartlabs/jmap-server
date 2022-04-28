use super::BlobStore;

pub struct S3BlobStore {}

impl BlobStore for S3BlobStore {
    fn new(_settings: &crate::config::EnvSettings) -> crate::Result<Self> {
        todo!()
    }

    fn get_range(
        &self,
        _blob_id: &super::BlobId,
        _range: std::ops::Range<u32>,
    ) -> crate::Result<Option<Vec<u8>>> {
        todo!()
    }

    fn put(&self, _blob_id: &super::BlobId, _blob: &[u8]) -> crate::Result<bool> {
        todo!()
    }

    fn delete(&self, _blob_id: &super::BlobId) -> crate::Result<bool> {
        todo!()
    }
}
