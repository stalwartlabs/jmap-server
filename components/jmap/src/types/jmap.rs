use std::ops::Deref;
use store::{
    serialize::base32::{BASE32_ALPHABET, BASE32_INVERSE},
    DocumentId,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub struct JMAPId {
    id: u64,
}

impl Default for JMAPId {
    fn default() -> Self {
        JMAPId { id: u64::MAX }
    }
}

impl JMAPId {
    pub fn new(id: u64) -> Self {
        Self { id }
    }

    pub fn singleton() -> Self {
        Self::new(u64::MAX)
    }

    pub fn parse(value: &str) -> Option<Self>
    where
        Self: Sized,
    {
        let value = value.as_bytes();

        if (1..=13).contains(&value.len()) {
            JMAPId {
                id: if value != b"singleton" {
                    let mut place = 0x20u64.pow(value.len() as u32 - 1);
                    let mut id = 0;

                    for ch in value {
                        id += BASE32_INVERSE
                            .get(*ch as usize)
                            .and_then(|&i| if i != u8::MAX { Some(i as u64) } else { None })?
                            .wrapping_mul(place);
                        place >>= 5;
                    }

                    id
                } else {
                    u64::MAX
                },
            }
            .into()
        } else {
            None
        }
    }

    // From https://github.com/archer884/crockford by J/A <archer884@gmail.com>
    // License: MIT/Apache 2.0
    pub fn as_string(&self) -> String {
        match self.id {
            u64::MAX => "singleton".to_string(),
            0 => "a".to_string(),
            mut n => {
                // Used for the initial shift.
                const QUAD_SHIFT: usize = 60;
                const QUAD_RESET: usize = 4;

                // Used for all subsequent shifts.
                const FIVE_SHIFT: usize = 59;
                const FIVE_RESET: usize = 5;

                // After we clear the four most significant bits, the four least significant bits will be
                // replaced with 0001. We can then know to stop once the four most significant bits are,
                // likewise, 0001.
                const STOP_BIT: u64 = 1 << QUAD_SHIFT;

                let mut buf = String::with_capacity(7);

                // Start by getting the most significant four bits. We get four here because these would be
                // leftovers when starting from the least significant bits. In either case, tag the four least
                // significant bits with our stop bit.
                match (n >> QUAD_SHIFT) as usize {
                    // Eat leading zero-bits. This should not be done if the first four bits were non-zero.
                    // Additionally, we *must* do this in increments of five bits.
                    0 => {
                        n <<= QUAD_RESET;
                        n |= 1;
                        n <<= n.leading_zeros() / 5 * 5;
                    }

                    // Write value of first four bytes.
                    i => {
                        n <<= QUAD_RESET;
                        n |= 1;
                        buf.push(char::from(BASE32_ALPHABET[i]));
                    }
                }

                // From now until we reach the stop bit, take the five most significant bits and then shift
                // left by five bits.
                while n != STOP_BIT {
                    buf.push(char::from(BASE32_ALPHABET[(n >> FIVE_SHIFT) as usize]));
                    n <<= FIVE_RESET;
                }

                buf
            }
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

impl From<JMAPId> for String {
    fn from(id: JMAPId) -> Self {
        id.as_string()
    }
}

impl serde::Serialize for JMAPId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_string().as_str())
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
        f.write_str(&self.as_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::types::jmap::JMAPId;

    #[test]
    fn parse_jmap_id() {
        for number in [0, 1, 10, 1000, u64::MAX / 2, u64::MAX - 1, u64::MAX] {
            let id = JMAPId::from(number);
            let id_string = id.as_string();
            assert_eq!(JMAPId::parse(&id_string).unwrap(), id);
        }
    }
}
