use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    ops::Range,
    path::PathBuf,
};

use crate::{config::EnvSettings, StoreError};

use super::{BlobId, BlobStore};

pub struct LocalBlobStore {
    pub base_path: PathBuf,
    pub hash_levels: Vec<usize>,
}

impl BlobStore for LocalBlobStore {
    fn new(settings: &EnvSettings) -> crate::Result<Self> {
        let mut base_path = PathBuf::from(
            settings
                .get("db-path")
                .unwrap_or_else(|| "stalwart-jmap".to_string()),
        );
        base_path.push("blobs");
        Ok(LocalBlobStore {
            base_path,
            hash_levels: vec![1], //TODO configure
        })
    }

    fn put(&self, blob_id: &BlobId, blob: &[u8]) -> crate::Result<bool> {
        let blob_path = self.get_path(blob_id)?;

        if blob_path.exists() {
            let metadata = fs::metadata(&blob_path)?;
            if metadata.len() as u32 == blob_id.size {
                return Ok(false);
            }
        }

        fs::create_dir_all(blob_path.parent().unwrap())?;
        let mut blob_file = File::create(&blob_path)?;
        blob_file.write_all(blob)?;
        blob_file.flush()?;

        Ok(true)
    }

    fn get_range(&self, blob_id: &BlobId, range: Range<u32>) -> crate::Result<Option<Vec<u8>>> {
        let blob_path = self.get_path(blob_id)?;
        if !blob_path.exists() {
            return Ok(None);
        }
        let mut blob = File::open(&blob_path)?;
        Ok(Some(if range.start != 0 || range.end != u32::MAX {
            let from_offset = if range.start < blob_id.size {
                range.start
            } else {
                0
            };
            let mut buf = vec![0; (std::cmp::min(range.end, blob_id.size) - from_offset) as usize];

            if from_offset > 0 {
                blob.seek(SeekFrom::Start(from_offset as u64))?;
            }
            blob.read_exact(&mut buf)?;
            buf
        } else {
            let mut buf = Vec::with_capacity(blob_id.size as usize);
            blob.read_to_end(&mut buf)?;
            buf
        }))
    }

    fn delete(&self, blob_id: &BlobId) -> crate::Result<bool> {
        let blob_path = self.get_path(blob_id)?;
        if blob_path.exists() {
            fs::remove_file(&blob_path)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl LocalBlobStore {
    fn get_path(&self, blob_id: &BlobId) -> crate::Result<PathBuf> {
        let mut path = self.base_path.clone();
        let mut hash_pos = 0;
        for hash_level in &self.hash_levels {
            let mut path_buf = String::with_capacity(10);
            for _ in 0..*hash_level {
                path_buf.push_str(&format!(
                    "{:02x}",
                    blob_id
                        .hash
                        .get(hash_pos)
                        .ok_or_else(|| StoreError::InternalError("Invalid hash".to_string()))?
                ));
                hash_pos += 1;
            }
            path.push(path_buf);
        }

        path.push(blob_id.to_string());

        Ok(path)
    }
}
