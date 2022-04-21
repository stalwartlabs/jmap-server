use store::log::ChangeId;

use crate::{error::method::MethodError, protocol::json::JSONValue};

use super::{hex_reader, HexWriter, JMAPIdSerialize};
use store::leb128::Leb128;

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
}

impl JMAPIdSerialize for JMAPState {
    fn from_jmap_string(id: &str) -> Option<Self>
    where
        Self: Sized,
    {
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

    fn to_jmap_string(&self) -> String {
        match self {
            JMAPState::Initial => "n".to_string(),
            JMAPState::Exact(id) => format!("s{:02x}", id),
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
                writer.result
            }
        }
    }
}

impl From<JMAPState> for JSONValue {
    fn from(state: JMAPState) -> Self {
        JSONValue::String(state.to_jmap_string())
    }
}

impl JSONValue {
    pub fn to_jmap_state(&self) -> Option<JMAPState> {
        match self {
            JSONValue::String(string) => JMAPState::from_jmap_string(string),
            _ => None,
        }
    }

    pub fn parse_jmap_state(self, optional: bool) -> crate::Result<Option<JMAPState>> {
        match self {
            JSONValue::String(string) => {
                Ok(Some(JMAPState::from_jmap_string(&string).ok_or_else(
                    || MethodError::InvalidArguments("Failed to parse JMAP state.".to_string()),
                )?))
            }
            JSONValue::Null if optional => Ok(None),
            _ => Err(MethodError::InvalidArguments(
                "Expected string.".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {

    use store::log::ChangeId;

    use crate::id::JMAPIdSerialize;

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
            assert_eq!(
                JMAPState::from_jmap_string(&id.to_jmap_string()).unwrap(),
                id
            );
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
            assert!(JMAPState::from_jmap_string(invalid_id).is_none());
        }
    }
}
