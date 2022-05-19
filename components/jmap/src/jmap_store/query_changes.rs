use std::{collections::HashMap, iter::FromIterator};

use store::{AccountId, JMAPId, JMAPStore, Store};

use crate::{
    id::{state::JMAPState, JMAPIdSerialize},
    protocol::json::JSONValue,
    request::{
        changes::ChangesRequest,
        query::{QueryRequest, QueryResponse},
        query_changes::{AddedItem, QueryChangesRequest, QueryChangesResponse},
    },
};

use super::{
    changes::{ChangesObject, JMAPChanges},
    query::{JMAPQuery, QueryObject},
};

pub trait JMAPQueryChanges<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn query_changes<'y, 'z: 'y, O>(
        &'z self,
        request: QueryChangesRequest<O, T>,
    ) -> crate::Result<QueryChangesResponse>
    where
        O: QueryObject<T> + ChangesObject;
}

impl<T> JMAPQueryChanges<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn query_changes<'y, 'z: 'y, O>(
        &'z self,
        request: QueryChangesRequest<O, T>,
    ) -> crate::Result<QueryChangesResponse>
    where
        O: QueryObject<T> + ChangesObject,
    {
        let changes = self.changes::<O>(ChangesRequest {
            account_id: request.account_id,
            since_state: request.since_query_state,
            max_changes: request.max_changes,
        })?;

        let query_result = if changes.total_changes > 0 || request.calculate_total.unwrap_or(false)
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

        let mut removed = Vec::with_capacity(changes.total_changes);
        let mut added = Vec::with_capacity(changes.total_changes);

        if changes.total_changes > 0 {
            if !query_result.is_immutable {
                for (index, id) in query_result.ids.into_iter().enumerate() {
                    if matches!(request.up_to_id, Some(up_to_id) if up_to_id == id) {
                        break;
                    } else if changes.created.contains(&id) || changes.updated.contains(&id) {
                        added.push(AddedItem::new(id, index));
                    }
                }

                removed = changes.updated;
            } else {
                for (index, id) in query_result.ids.into_iter().enumerate() {
                    //TODO test up to id properly
                    if matches!(request.up_to_id, Some(up_to_id) if up_to_id == id) {
                        break;
                    } else if changes.created.contains(&id) {
                        added.push(AddedItem::new(id, index));
                    }
                }
            }

            if !changes.destroyed.is_empty() {
                removed.extend(changes.destroyed);
            }
        }

        Ok(QueryChangesResponse {
            account_id: request.account_id,
            old_query_state: changes.old_state,
            new_query_state: changes.new_state,
            total: query_result.total,
            removed,
            added,
        })
    }
}
