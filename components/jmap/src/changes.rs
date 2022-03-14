use std::collections::{HashMap, HashSet};

use store::{
    changes::{Change, ChangeId, Query},
    leb128::Leb128,
    AccountId, Collection, JMAPId, JMAPStore, Store, StoreError,
};

use crate::{
    id::{hex_reader, HexWriter, JMAPIdSerialize},
    json::JSONValue,
    JMAPQueryResponse,
};

#[derive(Debug)]
pub struct JMAPChangesRequest {
    pub account: AccountId,
    pub since_state: JMAPState,
    pub max_changes: usize,
}

#[derive(Debug)]
pub struct JMAPQueryChangesResponseItem {
    pub id: JMAPId,
    pub index: usize,
}

#[derive(Debug)]
pub struct JMAPQueryChangesResponse {
    pub old_query_state: JMAPState,
    pub new_query_state: JMAPState,
    pub total: usize,
    pub removed: Vec<JMAPId>,
    pub added: Vec<JMAPQueryChangesResponseItem>,
}

#[derive(Debug)]
pub struct JMAPChangesResponse<T> {
    pub old_state: JMAPState,
    pub new_state: JMAPState,
    pub has_more_changes: bool,
    pub total_changes: usize,
    pub created: HashSet<ChangeId>,
    pub updated: HashSet<ChangeId>,
    pub destroyed: HashSet<ChangeId>,
    pub arguments: T,
}

impl<T> From<JMAPChangesResponse<T>> for JSONValue
where
    T: Into<JSONValue>,
{
    fn from(r: JMAPChangesResponse<T>) -> Self {
        let mut obj = HashMap::new();
        obj.insert("oldState".to_string(), r.old_state.into());
        obj.insert("newState".to_string(), r.new_state.into());
        obj.insert("hasMoreChanges".to_string(), r.has_more_changes.into());
        obj.insert(
            "created".to_string(),
            r.created
                .into_iter()
                .map(|id| id.to_jmap_string().into())
                .collect::<Vec<JSONValue>>()
                .into(),
        );
        obj.insert(
            "updated".to_string(),
            r.updated
                .into_iter()
                .map(|id| id.to_jmap_string().into())
                .collect::<Vec<JSONValue>>()
                .into(),
        );
        obj.insert(
            "destroyed".to_string(),
            r.destroyed
                .into_iter()
                .map(|id| id.to_jmap_string().into())
                .collect::<Vec<JSONValue>>()
                .into(),
        );
        if let JSONValue::Object(arguments) = r.arguments.into() {
            obj.extend(arguments);
        }
        obj.into()
    }
}

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

pub trait JMAPChanges {
    fn get_state(&self, account: AccountId, collection: Collection)
        -> store::Result<JMAPState>;
    fn get_jmap_changes(
        &self,
        account: AccountId,
        collection: Collection,
        since_state: JMAPState,
        max_changes: usize,
    ) -> store::Result<JMAPChangesResponse<()>>;
}

impl<T> JMAPChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get_state(
        &self,
        account: AccountId,
        collection: Collection,
    ) -> store::Result<JMAPState> {
        Ok(self
            .get_last_change_id(account, collection)?
            .map(JMAPState::Exact)
            .unwrap_or(JMAPState::Initial))
    }

    fn get_jmap_changes(
        &self,
        account: AccountId,
        collection: Collection,
        since_state: JMAPState,
        max_changes: usize,
    ) -> store::Result<JMAPChangesResponse<()>> {
        let (items_sent, mut changelog) = match &since_state {
            JMAPState::Initial => {
                let changelog = self.get_changes(account, collection, Query::All)?.unwrap();
                if changelog.changes.is_empty() && changelog.from_change_id == 0 {
                    return Ok(JMAPChangesResponse {
                        new_state: since_state.clone(),
                        old_state: since_state,
                        has_more_changes: false,
                        total_changes: 0,
                        created: HashSet::new(),
                        updated: HashSet::new(),
                        destroyed: HashSet::new(),
                        arguments: (),
                    });
                }

                (0, changelog)
            }
            JMAPState::Exact(change_id) => (
                0,
                self.get_changes(account, collection, Query::Since(*change_id))?
                    .ok_or(StoreError::NotFound)?,
            ),
            JMAPState::Intermediate(intermediate_state) => {
                let mut changelog = self
                    .get_changes(
                        account,
                        collection,
                        Query::RangeInclusive(intermediate_state.from_id, intermediate_state.to_id),
                    )?
                    .ok_or(StoreError::NotFound)?;
                if intermediate_state.items_sent >= changelog.changes.len() {
                    (
                        0,
                        self.get_changes(
                            account,
                            collection,
                            Query::Since(intermediate_state.to_id),
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
                    Change::Insert(item) => created.insert(item),
                    Change::Update(item) => updated.insert(item),
                    Change::Delete(item) => destroyed.insert(item),
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
            arguments: (),
        })
    }
}

pub fn query_changes(
    changes: JMAPChangesResponse<()>,
    query_results: Option<JMAPQueryResponse>,
    up_to_id: Option<JMAPId>,
) -> JMAPQueryChangesResponse {
    let mut removed;
    let mut added;

    let total = if let Some(query_results) = query_results {
        removed = Vec::with_capacity(changes.total_changes);
        added = Vec::with_capacity(changes.total_changes);

        if changes.total_changes > 0 {
            if !query_results.is_immutable {
                for updated_id in &changes.updated {
                    removed.push(*updated_id);
                }
                for (index, id) in query_results.ids.into_iter().enumerate() {
                    if changes.created.contains(&id) || changes.updated.contains(&id) {
                        added.push(JMAPQueryChangesResponseItem { id, index });
                    }
                }
            } else {
                for (index, id) in query_results.ids.into_iter().enumerate() {
                    //TODO test up to id properly
                    if let Some(up_to_id) = up_to_id.as_ref() {
                        if &id == up_to_id {
                            break;
                        }
                    }
                    if changes.created.contains(&id) {
                        added.push(JMAPQueryChangesResponseItem { id, index });
                    }
                }
            }
            for deleted_id in changes.destroyed {
                removed.push(deleted_id);
            }
        }

        query_results.total
    } else {
        removed = Vec::new();
        added = Vec::new();
        0
    };

    JMAPQueryChangesResponse {
        old_query_state: changes.old_state,
        new_query_state: changes.new_state,
        total,
        removed,
        added,
    }
}

#[cfg(test)]
mod tests {

    use store::changes::ChangeId;

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
