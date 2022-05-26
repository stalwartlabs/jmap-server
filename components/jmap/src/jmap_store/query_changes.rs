use store::{JMAPStore, Store};

use crate::{
    request::{
        changes::{ChangesRequest, ChangesResponse},
        query::{QueryRequest, QueryResponse},
        query_changes::{AddedItem, QueryChangesRequest, QueryChangesResponse},
    },
    types::jmap::JMAPId,
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
    pub request: Option<QueryChangesRequest<O>>,
    pub up_to_id: Option<JMAPId>,
    pub account_id: JMAPId,
}

impl<'y, O, T> QueryChangesHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: QueryObject + ChangesObject,
{
    pub fn new(
        store: &'y JMAPStore<T>,
        mut request: QueryChangesRequest<O>,
    ) -> crate::Result<Self> {
        Ok(QueryChangesHelper {
            store,
            account_id: request.account_id,
            up_to_id: request.up_to_id.take(),
            changes: store.changes::<O>(ChangesRequest {
                account_id: request.account_id,
                since_state: request.since_query_state.clone(),
                max_changes: request.max_changes,
            })?,
            request: request.into(),
        })
    }

    pub fn has_changes(&mut self) -> Option<QueryRequest<O>> {
        let request = self.request.take().unwrap();
        if self.changes.total_changes > 0 || request.calculate_total.unwrap_or(false) {
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
            }
            .into()
        } else {
            None
        }
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
                        if matches!(self.up_to_id, Some(up_to_id) if up_to_id == id) {
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
                        if matches!(self.up_to_id, Some(up_to_id) if up_to_id == id) {
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
                account_id: self.account_id,
                old_query_state: self.changes.old_state,
                new_query_state: self.changes.new_state,
                total: query_response.total,
                removed,
                added,
            })
        } else {
            Ok(QueryChangesResponse {
                account_id: self.account_id,
                old_query_state: self.changes.old_state,
                new_query_state: self.changes.new_state,
                total: None,
                removed: Vec::with_capacity(0),
                added: Vec::with_capacity(0),
            })
        }
    }
}
