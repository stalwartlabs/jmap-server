use std::collections::HashMap;

use store::{
    log::{Change, Query},
    AccountId, Collection, JMAPId, JMAPStore, Store, StoreError,
};

use crate::{
    id::{state::JMAPState, JMAPIdSerialize},
    protocol::json::JSONValue,
    request::changes::ChangesRequest,
};

pub trait ChangesObject {
    fn collection() -> Collection;
    fn handle_result(result: &mut ChangesResult);
}

#[derive(Default)]
pub struct ChangesResult {
    pub account_id: AccountId,
    pub total_changes: usize,
    pub has_children_changes: bool,
    pub has_more_changes: bool,
    pub old_state: JMAPState,
    pub new_state: JMAPState,
    pub created: Vec<JSONValue>,
    pub updated: Vec<JSONValue>,
    pub destroyed: Vec<JSONValue>,
    pub arguments: HashMap<String, JSONValue>,
}

pub trait JMAPChanges {
    fn get_state(&self, account: AccountId, collection: Collection) -> store::Result<JMAPState>;
    fn changes<T>(&self, request: ChangesRequest) -> crate::Result<ChangesResult>
    where
        T: ChangesObject;
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

    fn changes<U>(&self, request: ChangesRequest) -> crate::Result<ChangesResult>
    where
        U: ChangesObject,
    {
        let collection = U::collection();
        let max_changes = if self.config.changes_max_results > 0
            && self.config.changes_max_results < request.max_changes
        {
            self.config.changes_max_results
        } else {
            request.max_changes
        };

        let (items_sent, mut changelog) = match &request.since_state {
            JMAPState::Initial => {
                let changelog = self
                    .get_changes(request.account_id, collection, Query::All)?
                    .unwrap();
                if changelog.changes.is_empty() && changelog.from_change_id == 0 {
                    return Ok(ChangesResult::default());
                }

                (0, changelog)
            }
            JMAPState::Exact(change_id) => (
                0,
                self.get_changes(request.account_id, collection, Query::Since(*change_id))?
                    .ok_or_else(|| {
                        StoreError::InvalidArguments(
                            "The specified stateId does could not be found.".to_string(),
                        )
                    })?,
            ),
            JMAPState::Intermediate(intermediate_state) => {
                let mut changelog = self
                    .get_changes(
                        request.account_id,
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
                            request.account_id,
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
        let mut result = ChangesResult {
            account_id: request.account_id,
            total_changes,
            has_children_changes: !updated.is_empty() && !items_changed,
            has_more_changes,
            old_state: request.since_state,
            new_state: if has_more_changes {
                JMAPState::new_intermediate(
                    changelog.from_change_id,
                    changelog.to_change_id,
                    items_sent + max_changes,
                )
            } else {
                JMAPState::new_exact(changelog.to_change_id)
            },
            created,
            updated,
            destroyed,
            arguments: HashMap::with_capacity(0),
        };
        U::handle_result(&mut result);

        Ok(result)
    }
}

impl From<ChangesResult> for JSONValue {
    fn from(changes_result: ChangesResult) -> Self {
        let mut result = if changes_result.arguments.is_empty() {
            HashMap::with_capacity(7)
        } else {
            changes_result.arguments
        };
        result.insert(
            "accountId".to_string(),
            (changes_result.account_id as JMAPId)
                .to_jmap_string()
                .into(),
        );
        result.insert(
            "hasMoreChanges".to_string(),
            changes_result.has_more_changes.into(),
        );
        result.insert(
            "totalChanges".to_string(),
            changes_result.total_changes.into(),
        );
        result.insert("newState".to_string(), changes_result.new_state.into());
        result.insert("oldState".to_string(), changes_result.old_state.into());
        result.insert("created".to_string(), changes_result.created.into());
        result.insert("updated".to_string(), changes_result.updated.into());
        result.insert("destroyed".to_string(), changes_result.destroyed.into());

        result.into()
    }
}
