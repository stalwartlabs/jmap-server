use std::ops::Deref;

use store::DocumentId;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub struct JMAPId {
    id: u64,
}

impl JMAPId {
    pub fn parse(id: &str) -> Option<Self>
    where
        Self: Sized,
    {
        if id.as_bytes().get(0)? == &b'i' {
            JMAPId {
                id: u64::from_str_radix(id.get(1..)?, 16).ok()?,
            }
            .into()
        } else if id == "singleton" {
            JMAPId { id: u64::MAX }.into()
        } else {
            None
        }
    }

    pub fn from_parts(prefix_id: DocumentId, doc_id: DocumentId) -> JMAPId {
        JMAPId {
            id: (prefix_id as u64) << 32 | doc_id as u64,
        }
    }

    pub fn get_document_id(&self) -> DocumentId {
        (self.id & 0xFFFFFFFF) as DocumentId
    }

    pub fn get_prefix_id(&self) -> DocumentId {
        (self.id >> 32) as DocumentId
    }
}

impl From<u64> for JMAPId {
    fn from(id: u64) -> Self {
        JMAPId { id }
    }
}

impl From<u32> for JMAPId {
    fn from(id: u32) -> Self {
        JMAPId { id: id as u64 }
    }
}

impl From<JMAPId> for u64 {
    fn from(id: JMAPId) -> Self {
        id.id
    }
}

impl From<&JMAPId> for u64 {
    fn from(id: &JMAPId) -> Self {
        id.id
    }
}

impl From<(u32, u32)> for JMAPId {
    fn from(id: (u32, u32)) -> Self {
        JMAPId::from_parts(id.0, id.1)
    }
}

impl Deref for JMAPId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.id
    }
}

impl AsRef<u64> for JMAPId {
    fn as_ref(&self) -> &u64 {
        &self.id
    }
}

impl From<JMAPId> for u32 {
    fn from(id: JMAPId) -> Self {
        id.get_document_id()
    }
}

impl serde::Serialize for JMAPId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

struct JMAPIdVisitor;

impl<'de> serde::de::Visitor<'de> for JMAPIdVisitor {
    type Value = JMAPId;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a valid JMAP id")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        JMAPId::parse(v)
            .ok_or_else(|| serde::de::Error::custom(format!("Failed to parse JMAP id '{}'", v)))
    }
}

impl<'de> serde::Deserialize<'de> for JMAPId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(JMAPIdVisitor)
    }
}

impl std::fmt::Display for JMAPId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.id != u64::MAX {
            write!(f, "i{:02x}", self.id)
        } else {
            write!(f, "singleton")
        }
    }
}
