use std::{collections::HashMap, iter::FromIterator};

use store::{AccountId, JMAPId, JMAPStore, Store};

use crate::{
    id::{state::JMAPState, JMAPIdSerialize},
    protocol::json::JSONValue,
    request::{changes::ChangesRequest, query::QueryRequest, query_changes::QueryChangesRequest},
};

use super::{
    changes::{ChangesObject, JMAPChanges},
    query::{JMAPQuery, QueryObject, QueryResult},
};

pub struct QueryChangesResult {
    pub account_id: AccountId,
    pub old_query_state: JMAPState,
    pub new_query_state: JMAPState,
    pub total: Option<usize>,
    pub removed: Vec<JSONValue>,
    pub added: Vec<JSONValue>,
}

pub trait JMAPQueryChanges<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn query_changes<'y, 'z: 'y, U, V>(
        &'z self,
        request: QueryChangesRequest,
    ) -> crate::Result<QueryChangesResult>
    where
        U: ChangesObject,
        V: QueryObject<'y, T>;
}

impl<T> JMAPQueryChanges<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn query_changes<'y, 'z: 'y, U, V>(
        &'z self,
        request: QueryChangesRequest,
    ) -> crate::Result<QueryChangesResult>
    where
        U: ChangesObject,
        V: QueryObject<'y, T>,
    {
        let changes = self.changes::<U>(ChangesRequest {
            account_id: request.account_id,
            since_state: request.since_query_state,
            max_changes: request.max_changes,
            arguments: HashMap::new(),
        })?;

        let query_result = if changes.total_changes > 0 || request.calculate_total {
            JMAPQuery::query::<V>(
                self,
                QueryRequest {
                    account_id: request.account_id,
                    filter: request.filter,
                    sort: request.sort,
                    position: 0,
                    anchor: None,
                    anchor_offset: 0,
                    limit: 0,
                    calculate_total: true,
                    arguments: request.arguments,
                },
            )?
        } else {
            QueryResult {
                is_immutable: false,
                account_id: request.account_id,
                position: 0,
                query_state: JMAPState::Initial,
                total: None,
                limit: None,
                ids: vec![],
            }
        };

        let mut removed = Vec::with_capacity(changes.total_changes);
        let mut added = Vec::with_capacity(changes.total_changes);

        if changes.total_changes > 0 {
            if !query_result.is_immutable {
                for (index, id) in query_result.ids.into_iter().enumerate() {
                    if id == request.up_to_id {
                        break;
                    } else if changes.created.contains(&id) || changes.updated.contains(&id) {
                        added.push(
                            HashMap::from_iter([
                                ("index".to_string(), index.into()),
                                ("id".to_string(), id),
                            ])
                            .into(),
                        );
                    }
                }

                removed = changes.updated;
            } else {
                for (index, id) in query_result.ids.into_iter().enumerate() {
                    //TODO test up to id properly
                    if id == request.up_to_id {
                        break;
                    } else if changes.created.contains(&id) {
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

            if !changes.destroyed.is_empty() {
                removed.extend(changes.destroyed);
            }
        }

        Ok(QueryChangesResult {
            account_id: request.account_id,
            old_query_state: changes.old_state,
            new_query_state: changes.new_state,
            total: query_result.total,
            removed,
            added,
        })
    }
}

impl From<QueryChangesResult> for JSONValue {
    fn from(query_changes_result: QueryChangesResult) -> Self {
        let mut result = HashMap::with_capacity(6);
        result.insert(
            "accountId".to_string(),
            (query_changes_result.account_id as JMAPId)
                .to_jmap_string()
                .into(),
        );
        if let Some(total) = query_changes_result.total {
            result.insert("total".to_string(), total.into());
        }
        result.insert("added".to_string(), query_changes_result.added.into());
        result.insert("removed".to_string(), query_changes_result.removed.into());

        result.insert(
            "oldQueryState".to_string(),
            query_changes_result.old_query_state.into(),
        );
        result.insert(
            "newQueryState".to_string(),
            query_changes_result.new_query_state.into(),
        );
        result.into()
    }
}
