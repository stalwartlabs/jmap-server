use std::io::Write;

use store::blob::{BlobId, BLOB_HASH_LEN};
use store::serialize::base32::{Base32Reader, Base32Writer};
use store::serialize::leb128::Leb128;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct JMAPBlob {
    pub id: BlobId,
    pub section: Option<BlobSection>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BlobSection {
    pub offset_start: usize,
    pub size: usize,
    pub encoding: u8,
}

impl JMAPBlob {
    pub fn new(id: BlobId) -> Self {
        JMAPBlob { id, section: None }
    }

    pub fn new_section(id: BlobId, offset_start: usize, offset_end: usize, encoding: u8) -> Self {
        JMAPBlob {
            id,
            section: BlobSection {
                offset_start,
                size: offset_end - offset_start,
                encoding,
            }
            .into(),
        }
    }

    pub fn parse(id: &str) -> Option<Self>
    where
        Self: Sized,
    {
        let (is_local, encoding) = match id.as_bytes().get(0)? {
            b'b' => (false, None),
            b'a' => (true, None),
            b @ b'c'..=b'g' => (true, Some(*b - b'c')),
            b @ b'h'..=b'l' => (false, Some(*b - b'h')),
            _ => {
                return None;
            }
        };

        let mut it = Base32Reader::new(id.get(1..)?.as_bytes());
        let mut hash = [0; BLOB_HASH_LEN];

        for byte in hash.iter_mut().take(BLOB_HASH_LEN) {
            *byte = it.next()?;
        }

        Some(JMAPBlob {
            id: if is_local {
                BlobId::Local { hash }
            } else {
                BlobId::External { hash }
            },
            section: if let Some(encoding) = encoding {
                BlobSection {
                    offset_start: usize::from_leb128_it(&mut it)?,
                    size: usize::from_leb128_it(&mut it)?,
                    encoding,
                }
                .into()
            } else {
                None
            },
        })
    }

    pub fn start_offset(&self) -> usize {
        if let Some(section) = &self.section {
            section.offset_start
        } else {
            0
        }
    }
}

impl From<&BlobId> for JMAPBlob {
    fn from(id: &BlobId) -> Self {
        JMAPBlob::new(id.clone())
    }
}

impl From<BlobId> for JMAPBlob {
    fn from(id: BlobId) -> Self {
        JMAPBlob::new(id)
    }
}

impl Default for JMAPBlob {
    fn default() -> Self {
        Self {
            id: BlobId::Local {
                hash: [0; BLOB_HASH_LEN],
            },
            section: None,
        }
    }
}

impl serde::Serialize for JMAPBlob {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

struct JMAPBlobVisitor;

impl<'de> serde::de::Visitor<'de> for JMAPBlobVisitor {
    type Value = JMAPBlob;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a valid JMAP state")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(JMAPBlob::parse(v).unwrap_or_default())
    }
}

impl<'de> serde::Deserialize<'de> for JMAPBlob {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(JMAPBlobVisitor)
    }
}

impl std::fmt::Display for JMAPBlob {
    #[allow(clippy::unused_io_amount)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut writer;
        if let Some(section) = &self.section {
            writer =
                Base32Writer::with_capacity(BLOB_HASH_LEN + (std::mem::size_of::<u32>() * 2) + 1);
            writer.push_char(char::from(if self.id.is_local() {
                b'c' + section.encoding
            } else {
                b'h' + section.encoding
            }));
            writer.write(self.id.hash()).unwrap();
            section.offset_start.to_leb128_writer(&mut writer).unwrap();
            section.size.to_leb128_writer(&mut writer).unwrap();
        } else {
            writer = Base32Writer::with_capacity(BLOB_HASH_LEN + 1);
            writer.push_char(if self.id.is_local() { 'a' } else { 'b' });
            writer.write(self.id.hash()).unwrap();
        }

        f.write_str(&writer.finalize())
    }
}
