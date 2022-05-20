use std::{collections::HashMap, iter::FromIterator};

use store::{AccountId, JMAPId, JMAPStore, Store};

use crate::{
    id::{state::JMAPState, JMAPIdSerialize},
    protocol::json::JSONValue,
    request::{
        changes::{ChangesRequest, ChangesResponse},
        query::{QueryRequest, QueryResponse},
        query_changes::{AddedItem, QueryChangesRequest, QueryChangesResponse},
    },
};

use super::{
    changes::{ChangesObject, JMAPChanges},
    query::QueryObject,
};

pub struct QueryChangesHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: QueryObject + ChangesObject,
{
    pub store: &'y JMAPStore<T>,
    pub changes: ChangesResponse<O>,
    pub request: QueryChangesRequest<O>,
}

impl<'y, O, T> QueryChangesHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: QueryObject + ChangesObject,
{
    pub fn new(store: &'y JMAPStore<T>, request: QueryChangesRequest<O>) -> crate::Result<Self> {
        Ok(QueryChangesHelper {
            store,
            changes: store.changes::<O>(ChangesRequest {
                account_id: request.account_id,
                since_state: request.since_query_state.clone(),
                max_changes: request.max_changes,
            })?,
            request,
        })
    }

    pub fn has_changes(&self) -> bool {
        self.changes.total_changes > 0 || self.request.calculate_total.unwrap_or(false)
    }

    pub fn query_changes(
        self,
        query_response: Option<QueryResponse>,
    ) -> crate::Result<QueryChangesResponse> {
        if let Some(query_response) = query_response {
            let mut removed = Vec::with_capacity(self.changes.total_changes);
            let mut added = Vec::with_capacity(self.changes.total_changes);

            if self.changes.total_changes > 0 {
                if !query_response.is_immutable {
                    for (index, id) in query_response.ids.into_iter().enumerate() {
                        if matches!(self.request.up_to_id, Some(up_to_id) if up_to_id == id) {
                            break;
                        } else if self.changes.created.contains(&id)
                            || self.changes.updated.contains(&id)
                        {
                            added.push(AddedItem::new(id, index));
                        }
                    }

                    removed = self.changes.updated;
                } else {
                    for (index, id) in query_response.ids.into_iter().enumerate() {
                        //TODO test up to id properly
                        if matches!(self.request.up_to_id, Some(up_to_id) if up_to_id == id) {
                            break;
                        } else if self.changes.created.contains(&id) {
                            added.push(AddedItem::new(id, index));
                        }
                    }
                }

                if !self.changes.destroyed.is_empty() {
                    removed.extend(self.changes.destroyed);
                }
            }

            Ok(QueryChangesResponse {
                account_id: self.request.account_id,
                old_query_state: self.changes.old_state,
                new_query_state: self.changes.new_state,
                total: query_response.total,
                removed,
                added,
            })
        } else {
            Ok(QueryChangesResponse {
                account_id: self.request.account_id,
                old_query_state: self.changes.old_state,
                new_query_state: self.changes.new_state,
                total: None,
                removed: Vec::with_capacity(0),
                added: Vec::with_capacity(0),
            })
        }
    }
}

/*
pub trait JMAPQueryChanges<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn query_changes<'y, 'z: 'y, O>(
        &'z self,
        request: QueryChangesRequest<O>,
    ) -> crate::Result<QueryChangesResponse>
    where
        O: QueryObject + ChangesObject;
}

impl<T> JMAPQueryChanges<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn query_changes<'y, 'z: 'y, O>(
        &'z self,
        request: QueryChangesRequest<O>,
    ) -> crate::Result<QueryChangesResponse>
    where
        O: QueryObject + ChangesObject,
    {
        let query_response = if changes.total_changes > 0 || request.calculate_total.unwrap_or(false)
        {
            JMAPQuery::query::<O>(
                self,
                QueryRequest {
                    account_id: request.account_id,
                    filter: request.filter,
                    sort: request.sort,
                    position: None,
                    anchor: None,
                    anchor_offset: None,
                    limit: None,
                    calculate_total: None,
                    arguments: request.arguments,
                },
            )?
        } else {
            QueryResponse {
                is_immutable: false,
                account_id: request.account_id,
                position: 0,
                query_state: JMAPState::Initial,
                total: None,
                limit: None,
                ids: vec![],
                can_calculate_changes: true.into(),
            }
        };
    }
}
*/
