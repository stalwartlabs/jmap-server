use std::fmt::Write;

use store::{leb128::Leb128, AccountId, BlobIndex, CollectionId, DocumentId};

use crate::JMAPId;

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

pub struct BlobId {
    pub account: AccountId,
    pub collection: CollectionId,
    pub document: DocumentId,
    pub blob_index: BlobIndex,
}

impl JMAPIdSerialize for BlobId {
    fn from_jmap_string(id: &str) -> Option<Self>
    where
        Self: Sized,
    {
        if id.as_bytes().get(0)? == &b'b' {
            let mut it = hex_reader(id, 1);

            Some(BlobId {
                account: AccountId::from_leb128_it(&mut it)?,
                collection: CollectionId::from_leb128_it(&mut it)?,
                document: DocumentId::from_leb128_it(&mut it)?,
                blob_index: BlobIndex::from_leb128_it(&mut it)?,
            })
        } else {
            None
        }
    }

    fn to_jmap_string(&self) -> String {
        let mut writer = HexWriter::with_capacity(10);
        writer.result.push('b');
        self.account.to_leb128_writer(&mut writer).unwrap();
        self.collection.to_leb128_writer(&mut writer).unwrap();
        self.document.to_leb128_writer(&mut writer).unwrap();
        self.blob_index.to_leb128_writer(&mut writer).unwrap();
        writer.result
    }
}
