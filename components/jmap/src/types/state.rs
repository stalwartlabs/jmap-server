/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use store::{
    log::changes::ChangeId,
    serialize::{
        base32::{Base32Reader, Base32Writer},
        leb128::{Leb128Iterator, Leb128Writer},
    },
};

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
        match id.as_bytes().first()? {
            b'n' => JMAPState::Initial.into(),
            b's' => {
                JMAPState::Exact(Base32Reader::new(id.get(1..)?.as_bytes()).next_leb128()?).into()
            }
            b'r' => {
                let mut it = Base32Reader::new(id.get(1..)?.as_bytes());

                let from_id = it.next_leb128::<ChangeId>()?;
                let to_id = from_id.checked_add(it.next_leb128()?)?;
                let items_sent = it.next_leb128()?;

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
        let mut writer = Base32Writer::with_capacity(10);

        match self {
            JMAPState::Initial => {
                writer.push_char('n');
            }
            JMAPState::Exact(id) => {
                writer.push_char('s');
                writer.write_leb128(*id).unwrap();
            }
            JMAPState::Intermediate(intermediate) => {
                writer.push_char('r');
                writer.write_leb128(intermediate.from_id).unwrap();
                writer
                    .write_leb128(intermediate.to_id - intermediate.from_id)
                    .unwrap();
                writer.write_leb128(intermediate.items_sent).unwrap();
            }
        }

        f.write_str(&writer.finalize())
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
    }
}
