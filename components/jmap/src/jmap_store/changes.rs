use std::{collections::HashMap, iter::FromIterator};

use store::{
    log::{Change, Query},
    AccountId, Collection, JMAPStore, Store, StoreError,
};

use crate::{
    id::{state::JMAPState, JMAPIdSerialize},
    protocol::json::JSONValue,
    request::query::QueryResult,
};

pub struct ChangesResult {
    pub total_changes: usize,
    pub has_children_changes: bool,
    pub result: JSONValue,
}

pub trait JMAPChanges {
    fn get_state(&self, account: AccountId, collection: Collection) -> store::Result<JMAPState>;
    fn get_jmap_changes(
        &self,
        account: AccountId,
        collection: Collection,
        since_state: JMAPState,
        max_changes: usize,
    ) -> store::Result<ChangesResult>;
}

impl<T> JMAPChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get_state(&self, account: AccountId, collection: Collection) -> store::Result<JMAPState> {
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
    ) -> store::Result<ChangesResult> {
        let (items_sent, mut changelog) = match &since_state {
            JMAPState::Initial => {
                let changelog = self.get_changes(account, collection, Query::All)?.unwrap();
                if changelog.changes.is_empty() && changelog.from_change_id == 0 {
                    return Ok(ChangesResult {
                        total_changes: 0,
                        has_children_changes: false,
                        result: HashMap::from_iter([
                            ("hasMoreChanges".to_string(), false.into()),
                            ("totalChanges".to_string(), 0u64.into()),
                            ("newState".to_string(), since_state.clone().into()),
                            ("oldState".to_string(), since_state.into()),
                            ("created".to_string(), vec![].into()),
                            ("updated".to_string(), vec![].into()),
                            ("destroyed".to_string(), vec![].into()),
                        ])
                        .into(),
                    });
                }

                (0, changelog)
            }
            JMAPState::Exact(change_id) => (
                0,
                self.get_changes(account, collection, Query::Since(*change_id))?
                    .ok_or_else(|| {
                        StoreError::InvalidArguments(
                            "The specified stateId does could not be found.".to_string(),
                        )
                    })?,
            ),
            JMAPState::Intermediate(intermediate_state) => {
                let mut changelog = self
                    .get_changes(
                        account,
                        collection,
                        Query::RangeInclusive(intermediate_state.from_id, intermediate_state.to_id),
                    )?
                    .ok_or_else(|| {
                        StoreError::InvalidArguments(
                            "The specified stateId does could not be found.".to_string(),
                        )
                    })?;
                if intermediate_state.items_sent >= changelog.changes.len() {
                    (
                        0,
                        self.get_changes(
                            account,
                            collection,
                            Query::Since(intermediate_state.to_id),
                        )?
                        .ok_or_else(|| {
                            StoreError::InvalidArguments(
                                "The specified stateId does could not be found.".to_string(),
                            )
                        })?,
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

        let mut created = Vec::new();
        let mut updated = Vec::new();
        let mut destroyed = Vec::new();
        let mut items_changed = false;

        let total_changes = changelog.changes.len();
        if total_changes > 0 {
            for change in changelog.changes {
                match change {
                    Change::Insert(item) => created.push(item.to_jmap_string().into()),
                    Change::Update(item) => {
                        items_changed = true;
                        updated.push(item.to_jmap_string().into())
                    }
                    Change::Delete(item) => destroyed.push(item.to_jmap_string().into()),
                    Change::ChildUpdate(item) => updated.push(item.to_jmap_string().into()),
                };
            }
        }

        Ok(ChangesResult {
            total_changes,
            has_children_changes: !updated.is_empty() && !items_changed,
            result: HashMap::from_iter([
                ("hasMoreChanges".to_string(), has_more_changes.into()),
                ("totalChanges".to_string(), total_changes.into()),
                (
                    "newState".to_string(),
                    if has_more_changes {
                        JMAPState::new_intermediate(
                            changelog.from_change_id,
                            changelog.to_change_id,
                            items_sent + max_changes,
                        )
                    } else {
                        JMAPState::new_exact(changelog.to_change_id)
                    }
                    .into(),
                ),
                ("oldState".to_string(), since_state.into()),
                ("created".to_string(), created.into()),
                ("updated".to_string(), updated.into()),
                ("destroyed".to_string(), destroyed.into()),
            ])
            .into(),
        })
    }
}

impl ChangesResult {
    pub fn query(mut self, query_result: QueryResult, up_to_id: JSONValue) -> JSONValue {
        let mut result = HashMap::new();
        let changes = self.result.as_object_mut();

        if let JSONValue::Object(mut query_results) = query_result.result {
            let mut removed = Vec::with_capacity(self.total_changes);
            let mut added = Vec::with_capacity(self.total_changes);

            if self.total_changes > 0 {
                let changes_updated = changes.remove("updated").unwrap().unwrap_array().unwrap();
                let changes_created = changes.remove("created").unwrap().unwrap_array().unwrap();
                let changes_destroyed =
                    changes.remove("destroyed").unwrap().unwrap_array().unwrap();

                if !query_result.is_immutable {
                    for (index, id) in query_results
                        .remove("ids")
                        .unwrap()
                        .unwrap_array()
                        .unwrap()
                        .into_iter()
                        .enumerate()
                    {
                        if id == up_to_id {
                            break;
                        } else if changes_created.contains(&id) || changes_updated.contains(&id) {
                            added.push(
                                HashMap::from_iter([
                                    ("index".to_string(), index.into()),
                                    ("id".to_string(), id),
                                ])
                                .into(),
                            );
                        }
                    }

                    removed = changes_updated;
                } else {
                    for (index, id) in query_results
                        .remove("ids")
                        .unwrap()
                        .unwrap_array()
                        .unwrap()
                        .into_iter()
                        .enumerate()
                    {
                        //TODO test up to id properly
                        if id == up_to_id {
                            break;
                        } else if changes_created.contains(&id) {
                            added.push(
                                HashMap::from_iter([
                                    ("index".to_string(), index.into()),
                                    ("id".to_string(), id),
                                ])
                                .into(),
                            );
                        }
                    }
                }

                if !changes_destroyed.is_empty() {
                    removed.extend(changes_destroyed);
                }
            }

            if let Some(total) = query_results.remove("total") {
                result.insert("total".to_string(), total);
            }
            result.insert("added".to_string(), added.into());
            result.insert("removed".to_string(), removed.into());
        } else {
            result.insert("added".to_string(), vec![].into());
            result.insert("removed".to_string(), vec![].into());
        };

        result.insert(
            "oldQueryState".to_string(),
            changes.remove("oldState").unwrap(),
        );
        result.insert(
            "newQueryState".to_string(),
            changes.remove("newState").unwrap(),
        );
        result.into()
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
