use std::path::PathBuf;

use store::leb128::Leb128;
use store::StoreError;
use store::{leb128::skip_leb128_value, serialize::BLOB_KEY};
pub struct BlobFile {
    pub path: PathBuf,
    pub is_commited: bool,
}

impl BlobFile {
    pub fn new(
        base_path: PathBuf,
        name: &[u8],
        hash_levels: &[usize],
        create_if_missing: bool,
    ) -> std::io::Result<Self> {
        let mut path = base_path;
        let mut hash_pos = 0;
        for hash_level in hash_levels {
            let mut path_buf = String::with_capacity(10);
            for _ in 0..*hash_level {
                path_buf.push_str(&format!(
                    "{:02x}",
                    name.get(hash_pos).ok_or_else(|| std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Invalid hash"
                    ))?
                ));
                hash_pos += 1;
            }
            path.push(path_buf);
        }

        if create_if_missing {
            std::fs::create_dir_all(&path)?;
        }

        path.push(base32::encode(
            base32::Alphabet::RFC4648 { padding: false },
            name,
        ));

        Ok(Self {
            path,
            is_commited: true,
        })
    }

    pub fn get_path(&self) -> &PathBuf {
        &self.path
    }

    pub fn delete(&mut self) -> std::io::Result<()> {
        self.is_commited = true;
        std::fs::remove_file(&self.path)
    }

    pub fn commit(mut self) -> Self {
        self.is_commited = true;
        self
    }

    pub fn needs_commit(mut self) -> Self {
        self.is_commited = false;
        self
    }
}

impl Drop for BlobFile {
    fn drop(&mut self) {
        if !self.is_commited {
            self.delete().unwrap_or_else(|_| {
                //TODO log error properly
                println!("Failed to remove blob file: {}", self.path.display());
            });
        }
    }
}

pub struct BlobIndex {
    pub file: BlobFile,
    pub index: Vec<usize>,
}

pub fn serialize_blob_key_from_value(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut key = Vec::with_capacity(32 + std::mem::size_of::<usize>() + BLOB_KEY.len());
    key.extend_from_slice(BLOB_KEY);
    key.extend_from_slice(bytes.get(0..32 + skip_leb128_value(bytes.get(32..)?)?)?);
    key.into()
}

pub fn deserialize_blob_index(
    base_path: PathBuf,
    hash_levels: &[usize],
    bytes: &[u8],
) -> store::Result<BlobIndex> {
    let (mut num_entries, bytes_read) =
        usize::from_leb128_bytes(bytes.get(32..).ok_or(StoreError::DataCorruption)?)
            .ok_or(StoreError::DataCorruption)?;
    let blob_name = bytes
        .get(0..32 + bytes_read)
        .ok_or(StoreError::DataCorruption)?;
    let mut index = Vec::with_capacity(num_entries);
    let mut bytes_it = bytes.get(32 + bytes_read).into_iter();

    while num_entries > 0 {
        index.push(usize::from_leb128_it(&mut bytes_it).ok_or_else(|| {
            StoreError::DeserializeError(format!(
                "Failed to deserialize total inserts from bytes: {:?}",
                bytes
            ))
        })?);
        num_entries -= 1;
    }

    if num_entries > 0 {
        return Err(StoreError::DeserializeError(
            "Failed to deserialize blob index".into(),
        ));
    }

    Ok(BlobIndex {
        file: BlobFile::new(base_path, blob_name, hash_levels, false)
            .map_err(|err| StoreError::DeserializeError(err.to_string()))?,
        index,
    })
}
