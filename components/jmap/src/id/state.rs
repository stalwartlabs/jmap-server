use super::{hex_reader, HexWriter};
use store::{log::changes::ChangeId, serialize::leb128::Leb128};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JMAPIntermediateState {
    pub from_id: ChangeId,
    pub to_id: ChangeId,
    pub items_sent: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JMAPState {
    Initial,
    Exact(ChangeId),
    Intermediate(JMAPIntermediateState),
}

impl Default for JMAPState {
    fn default() -> Self {
        JMAPState::Initial
    }
}

impl From<ChangeId> for JMAPState {
    fn from(change_id: ChangeId) -> Self {
        JMAPState::Exact(change_id)
    }
}

impl JMAPState {
    pub fn new_initial() -> Self {
        JMAPState::Initial
    }

    pub fn new_exact(id: ChangeId) -> Self {
        JMAPState::Exact(id)
    }

    pub fn new_intermediate(from_id: ChangeId, to_id: ChangeId, items_sent: usize) -> Self {
        JMAPState::Intermediate(JMAPIntermediateState {
            from_id,
            to_id,
            items_sent,
        })
    }

    pub fn get_change_id(&self) -> ChangeId {
        match self {
            JMAPState::Exact(id) => *id,
            JMAPState::Intermediate(intermediate) => intermediate.to_id,
            JMAPState::Initial => ChangeId::MAX,
        }
    }

    pub fn parse(id: &str) -> Option<Self> {
        match id.as_bytes().get(0)? {
            b'n' => JMAPState::Initial.into(),
            b's' => JMAPState::Exact(ChangeId::from_str_radix(id.get(1..)?, 16).ok()?).into(),
            b'r' => {
                let mut it = hex_reader(id, 1);

                let from_id = ChangeId::from_leb128_it(&mut it)?;
                let to_id = from_id.checked_add(ChangeId::from_leb128_it(&mut it)?)?;
                let items_sent = usize::from_leb128_it(&mut it)?;

                if items_sent > 0 {
                    JMAPState::Intermediate(JMAPIntermediateState {
                        from_id,
                        to_id,
                        items_sent,
                    })
                    .into()
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

impl serde::Serialize for JMAPState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

struct JMAPStateVisitor;

impl<'de> serde::de::Visitor<'de> for JMAPStateVisitor {
    type Value = JMAPState;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a valid JMAP state")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        JMAPState::parse(v)
            .ok_or_else(|| serde::de::Error::custom(format!("Failed to parse JMAP state '{}'", v)))
    }
}

impl<'de> serde::Deserialize<'de> for JMAPState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(JMAPStateVisitor)
    }
}

impl std::fmt::Display for JMAPState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JMAPState::Initial => write!(f, "n"),
            JMAPState::Exact(id) => write!(f, "s{:02x}", id),
            JMAPState::Intermediate(intermediate) => {
                let mut writer = HexWriter::with_capacity(10);
                writer.result.push('r');
                intermediate.from_id.to_leb128_writer(&mut writer).unwrap();
                (intermediate.to_id - intermediate.from_id)
                    .to_leb128_writer(&mut writer)
                    .unwrap();
                intermediate
                    .items_sent
                    .to_leb128_writer(&mut writer)
                    .unwrap();
                write!(f, "{}", writer.result)
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use store::log::changes::ChangeId;

    use super::JMAPState;

    #[test]
    fn test_state_id() {
        for id in [
            JMAPState::new_initial(),
            JMAPState::new_exact(0),
            JMAPState::new_exact(12345678),
            JMAPState::new_exact(ChangeId::MAX),
            JMAPState::new_intermediate(0, 0, 1),
            JMAPState::new_intermediate(1024, 2048, 100),
            JMAPState::new_intermediate(12345678, 87654321, 1),
            JMAPState::new_intermediate(0, 0, 12345678),
            JMAPState::new_intermediate(0, 87654321, 12345678),
            JMAPState::new_intermediate(12345678, 87654321, 1),
            JMAPState::new_intermediate(12345678, 87654321, 12345678),
            JMAPState::new_intermediate(ChangeId::MAX, ChangeId::MAX, ChangeId::MAX as usize),
        ] {
            assert_eq!(JMAPState::parse(&id.to_string()).unwrap(), id);
        }

        for invalid_id in [
            "z",
            "",
            "blah",
            "izzzz",
            "i00zz",
            "r00",
            "r00zz",
            "r00z",
            "rffffffffffffffffff01ffffffffffffffffff01ffffffffffffffffff01",
            "rcec2f105e3bcf42300",
        ] {
            assert!(JMAPState::parse(invalid_id).is_none());
        }
    }
}
