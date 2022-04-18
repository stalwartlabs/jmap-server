use std::{collections::HashMap, io::Write};

use store::{blob::BlobIndex, leb128::Leb128, AccountId, Collection, DocumentId, JMAPId};

use crate::{json::JSONValue, JMAPError};

pub trait JMAPIdSerialize {
    fn from_jmap_string(id: &str) -> Option<Self>
    where
        Self: Sized;
    fn to_jmap_string(&self) -> String;
}

impl JMAPIdSerialize for JMAPId {
    fn from_jmap_string(id: &str) -> Option<Self>
    where
        Self: Sized,
    {
        if id.as_bytes().get(0)? == &b'i' {
            JMAPId::from_str_radix(id.get(1..)?, 16).ok()?.into()
        } else {
            None
        }
    }

    fn to_jmap_string(&self) -> String {
        format!("i{:02x}", self)
    }
}

pub trait JMAPIdReference {
    fn from_jmap_ref(id: &str, created_ids: &HashMap<String, JSONValue>) -> crate::Result<Self>
    where
        Self: Sized;
}

impl JMAPIdReference for JMAPId {
    fn from_jmap_ref(id: &str, created_ids: &HashMap<String, JSONValue>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        if !id.starts_with('#') {
            JMAPId::from_jmap_string(id)
                .ok_or_else(|| JMAPError::InvalidArguments(format!("Invalid JMAP Id: {}", id)))
        } else {
            let id_ref = id.get(1..).ok_or_else(|| {
                JMAPError::InvalidArguments(format!("Invalid reference to JMAP Id: {}", id))
            })?;

            if let Some(created_id) = created_ids.get(id_ref) {
                let created_id = created_id
                    .to_object()
                    .unwrap()
                    .get("id")
                    .unwrap()
                    .to_string()
                    .unwrap();
                JMAPId::from_jmap_string(created_id).ok_or_else(|| {
                    JMAPError::InvalidArguments(format!(
                        "Invalid referenced JMAP Id: {} ({})",
                        id_ref, created_id
                    ))
                })
            } else {
                Err(JMAPError::InvalidArguments(format!(
                    "Reference '{}' not found in createdIds.",
                    id_ref
                )))
            }
        }
    }
}

pub struct HexWriter {
    pub result: String,
}

impl HexWriter {
    pub fn with_capacity(capacity: usize) -> Self {
        HexWriter {
            result: String::with_capacity(capacity),
        }
    }
}

impl std::io::Write for HexWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        use std::fmt::Write;

        for &byte in buf {
            write!(&mut self.result, "{:02x}", byte).unwrap();
        }
        Ok(2 * buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[inline(always)]
pub fn hex_reader(id: &str, start_pos: usize) -> impl Iterator<Item = u8> + '_ {
    (start_pos..id.len())
        .step_by(2)
        .map(move |i| u8::from_str_radix(id.get(i..i + 2).unwrap_or(""), 16).unwrap_or(u8::MAX))
}

#[derive(Clone, Debug)]
pub struct OwnedBlob {
    pub account_id: AccountId,
    pub collection: Collection,
    pub document: DocumentId,
    pub blob_index: BlobIndex,
}

#[derive(Clone, Debug)]
pub struct TemporaryBlob {
    pub account_id: AccountId,
    pub timestamp: u64,
    pub hash: u64,
}

#[derive(Clone, Debug)]
pub struct InnerBlob<T> {
    pub blob_id: T,
    pub blob_index: BlobIndex,
}

#[derive(Clone, Debug)]
pub enum BlobId {
    Owned(OwnedBlob),
    Temporary(TemporaryBlob),
    InnerOwned(InnerBlob<OwnedBlob>),
    InnerTemporary(InnerBlob<TemporaryBlob>),
}

impl BlobId {
    pub fn new_owned(
        account_id: AccountId,
        collection: Collection,
        document: DocumentId,
        blob_index: BlobIndex,
    ) -> Self {
        BlobId::Owned(OwnedBlob {
            account_id,
            collection,
            document,
            blob_index,
        })
    }

    pub fn new_temporary(account_id: AccountId, timestamp: u64, hash: u64) -> Self {
        BlobId::Temporary(TemporaryBlob {
            account_id,
            timestamp,
            hash,
        })
    }

    pub fn new_inner(blob_id: BlobId, blob_index: BlobIndex) -> Option<Self> {
        match blob_id {
            BlobId::Owned(blob_id) => BlobId::InnerOwned(InnerBlob {
                blob_id,
                blob_index,
            })
            .into(),
            BlobId::Temporary(blob_id) => BlobId::InnerTemporary(InnerBlob {
                blob_id,
                blob_index,
            })
            .into(),
            BlobId::InnerOwned(_) | BlobId::InnerTemporary(_) => None,
        }
    }

    pub fn owner_id(&self) -> AccountId {
        match self {
            BlobId::Owned(blob_id) => blob_id.account_id,
            BlobId::Temporary(blob_id) => blob_id.account_id,
            BlobId::InnerOwned(blob_id) => blob_id.blob_id.account_id,
            BlobId::InnerTemporary(blob_id) => blob_id.blob_id.account_id,
        }
    }
}

impl JMAPIdSerialize for BlobId {
    fn from_jmap_string(id: &str) -> Option<Self>
    where
        Self: Sized,
    {
        match id.as_bytes().get(0)? {
            b'o' => {
                let mut it = hex_reader(id, 1);

                Some(BlobId::Owned(OwnedBlob {
                    account_id: AccountId::from_leb128_it(&mut it)?,
                    collection: it.next()?.into(),
                    document: DocumentId::from_leb128_it(&mut it)?,
                    blob_index: BlobIndex::from_leb128_it(&mut it)?,
                }))
            }
            b't' => {
                let mut it = hex_reader(id, 1);

                Some(BlobId::Temporary(TemporaryBlob {
                    account_id: AccountId::from_leb128_it(&mut it)?,
                    timestamp: u64::from_leb128_it(&mut it)?,
                    hash: u64::from_leb128_it(&mut it)?,
                }))
            }
            b'q' => {
                let mut it = hex_reader(id, 1);

                Some(BlobId::InnerTemporary(InnerBlob {
                    blob_id: TemporaryBlob {
                        account_id: AccountId::from_leb128_it(&mut it)?,
                        timestamp: u64::from_leb128_it(&mut it)?,
                        hash: u64::from_leb128_it(&mut it)?,
                    },
                    blob_index: BlobIndex::from_leb128_it(&mut it)?,
                }))
            }
            b'p' => {
                let mut it = hex_reader(id, 1);

                Some(BlobId::InnerOwned(InnerBlob {
                    blob_id: OwnedBlob {
                        account_id: AccountId::from_leb128_it(&mut it)?,
                        collection: it.next()?.into(),
                        document: DocumentId::from_leb128_it(&mut it)?,
                        blob_index: BlobIndex::from_leb128_it(&mut it)?,
                    },
                    blob_index: BlobIndex::from_leb128_it(&mut it)?,
                }))
            }
            _ => None,
        }
    }

    #[allow(clippy::unused_io_amount)]
    fn to_jmap_string(&self) -> String {
        let mut writer = HexWriter::with_capacity(10);
        match self {
            BlobId::Owned(blob_id) => {
                writer.result.push('o');
                blob_id.account_id.to_leb128_writer(&mut writer).unwrap();
                writer.write(&[blob_id.collection as u8]).unwrap();
                blob_id.document.to_leb128_writer(&mut writer).unwrap();
                blob_id.blob_index.to_leb128_writer(&mut writer).unwrap();
            }
            BlobId::Temporary(blob_id) => {
                writer.result.push('t');
                blob_id.account_id.to_leb128_writer(&mut writer).unwrap();
                blob_id.timestamp.to_leb128_writer(&mut writer).unwrap();
                blob_id.hash.to_leb128_writer(&mut writer).unwrap();
            }
            BlobId::InnerOwned(blob_id) => {
                writer.result.push('p');
                blob_id
                    .blob_id
                    .account_id
                    .to_leb128_writer(&mut writer)
                    .unwrap();
                writer.write(&[blob_id.blob_id.collection as u8]).unwrap();
                blob_id
                    .blob_id
                    .document
                    .to_leb128_writer(&mut writer)
                    .unwrap();
                blob_id
                    .blob_id
                    .blob_index
                    .to_leb128_writer(&mut writer)
                    .unwrap();
                blob_id.blob_index.to_leb128_writer(&mut writer).unwrap();
            }
            BlobId::InnerTemporary(blob_id) => {
                writer.result.push('q');
                blob_id
                    .blob_id
                    .account_id
                    .to_leb128_writer(&mut writer)
                    .unwrap();
                blob_id
                    .blob_id
                    .timestamp
                    .to_leb128_writer(&mut writer)
                    .unwrap();
                blob_id.blob_id.hash.to_leb128_writer(&mut writer).unwrap();
                blob_id.blob_index.to_leb128_writer(&mut writer).unwrap();
            }
        }
        writer.result
    }
}
