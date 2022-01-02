use std::{collections::HashSet, fmt::Write};

use store::{
    leb128::Leb128, AccountId, ChangeLogEntry, ChangeLogId, ChangeLogQuery, CollectionId, Store,
    StoreError,
};

use crate::{local_store::JMAPLocalStore, JMAPChangesResponse, JMAPIdSerialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JMAPIntermediateState {
    pub from_id: ChangeLogId,
    pub to_id: ChangeLogId,
    pub items_sent: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JMAPState {
    Initial,
    Exact(ChangeLogId),
    Intermediate(JMAPIntermediateState),
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

impl JMAPState {
    pub fn new_initial() -> Self {
        JMAPState::Initial
    }

    pub fn new_exact(id: ChangeLogId) -> Self {
        JMAPState::Exact(id)
    }

    pub fn new_intermediate(from_id: ChangeLogId, to_id: ChangeLogId, items_sent: usize) -> Self {
        JMAPState::Intermediate(JMAPIntermediateState {
            from_id,
            to_id,
            items_sent,
        })
    }
}

impl JMAPIdSerialize for JMAPState {
    fn from_jmap_id(id: &str) -> Option<Self>
    where
        Self: Sized,
    {
        match id.as_bytes().get(0)? {
            b'n' => JMAPState::Initial.into(),
            b's' => JMAPState::Exact(ChangeLogId::from_str_radix(id.get(1..)?, 16).ok()?).into(),
            b'r' => {
                let mut it = (1..id.len()).step_by(2).map(|i| {
                    u8::from_str_radix(id.get(i..i + 2).unwrap_or(""), 16).unwrap_or(u8::MAX)
                });

                let from_id = ChangeLogId::from_leb128_it(&mut it)?;
                let to_id = from_id.checked_add(ChangeLogId::from_leb128_it(&mut it)?)?;
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

    fn to_jmap_id(&self) -> String {
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

impl<'x, T> JMAPLocalStore<T>
where
    T: Store<'x>,
{
    pub fn get_changes(
        &self,
        account: AccountId,
        collection: CollectionId,
        since_state: JMAPState,
        max_changes: usize,
    ) -> store::Result<JMAPChangesResponse> {
        let (items_sent, mut changelog) = match &since_state {
            JMAPState::Initial => {
                let changelog = self
                    .store
                    .get_changes(account, collection, ChangeLogQuery::All)?
                    .unwrap();
                if changelog.changes.is_empty() && changelog.from_change_id == 0 {
                    return Ok(JMAPChangesResponse {
                        new_state: since_state.clone(),
                        old_state: since_state,
                        has_more_changes: false,
                        total_changes: 0,
                        created: HashSet::new(),
                        updated: HashSet::new(),
                        destroyed: HashSet::new(),
                    });
                }

                (0, changelog)
            }
            JMAPState::Exact(change_id) => (
                0,
                self.store
                    .get_changes(account, collection, ChangeLogQuery::Since(*change_id))?
                    .ok_or(StoreError::NotFound)?,
            ),
            JMAPState::Intermediate(intermediate_state) => {
                let mut changelog = self
                    .store
                    .get_changes(
                        account,
                        collection,
                        ChangeLogQuery::RangeInclusive(
                            intermediate_state.from_id,
                            intermediate_state.to_id,
                        ),
                    )?
                    .ok_or(StoreError::NotFound)?;
                if intermediate_state.items_sent >= changelog.changes.len() {
                    (
                        0,
                        self.store
                            .get_changes(
                                account,
                                collection,
                                ChangeLogQuery::Since(intermediate_state.to_id),
                            )?
                            .ok_or(StoreError::NotFound)?,
                    )
                } else {
                    changelog.changes.drain(
                        (changelog.changes.len() - intermediate_state.items_sent)
                            ..changelog.changes.len(),
                    );
                    (intermediate_state.items_sent, changelog)
                }
            }
        };

        let has_more_changes = if max_changes > 0 && changelog.changes.len() > max_changes {
            changelog
                .changes
                .drain(0..(changelog.changes.len() - max_changes));
            true
        } else {
            false
        };

        let mut created;
        let mut updated;
        let mut destroyed;

        let total_changes = changelog.changes.len();
        if total_changes > 0 {
            created = HashSet::with_capacity(total_changes);
            updated = HashSet::with_capacity(total_changes);
            destroyed = HashSet::with_capacity(total_changes);

            for change in changelog.changes {
                match change {
                    ChangeLogEntry::Insert(item) => created.insert(item),
                    ChangeLogEntry::Update(item) => updated.insert(item),
                    ChangeLogEntry::Delete(item) => destroyed.insert(item),
                };
            }
        } else {
            created = HashSet::new();
            updated = HashSet::new();
            destroyed = HashSet::new();
        }

        Ok(JMAPChangesResponse {
            old_state: since_state,
            new_state: (if has_more_changes {
                JMAPState::new_intermediate(
                    changelog.from_change_id,
                    changelog.to_change_id,
                    items_sent + max_changes,
                )
            } else {
                JMAPState::new_exact(changelog.to_change_id)
            }),
            has_more_changes,
            total_changes,
            created,
            updated,
            destroyed,
        })
    }
}

#[cfg(test)]
mod tests {
    use store::ChangeLogId;

    use crate::JMAPIdSerialize;

    use super::JMAPState;

    #[test]
    fn test_state_id() {
        for id in [
            JMAPState::new_initial(),
            JMAPState::new_exact(0),
            JMAPState::new_exact(12345678),
            JMAPState::new_exact(ChangeLogId::MAX),
            JMAPState::new_intermediate(0, 0, 1),
            JMAPState::new_intermediate(1024, 2048, 100),
            JMAPState::new_intermediate(12345678, 87654321, 1),
            JMAPState::new_intermediate(0, 0, 12345678),
            JMAPState::new_intermediate(0, 87654321, 12345678),
            JMAPState::new_intermediate(12345678, 87654321, 1),
            JMAPState::new_intermediate(12345678, 87654321, 12345678),
            JMAPState::new_intermediate(
                ChangeLogId::MAX,
                ChangeLogId::MAX,
                ChangeLogId::MAX as usize,
            ),
        ] {
            assert_eq!(JMAPState::from_jmap_id(&id.to_jmap_id()).unwrap(), id);
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
            assert!(JMAPState::from_jmap_id(invalid_id).is_none());
        }
    }
}
