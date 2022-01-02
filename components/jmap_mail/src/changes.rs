use jmap_store::{
    changes::JMAPState, local_store::JMAPLocalStore, JMAPChangesResponse, JMAPQuery,
    JMAPQueryChanges, JMAPQueryChangesResponse, JMAPQueryChangesResponseItem, JMAP_MAIL,
};
use store::Store;

use crate::{
    query::{JMAPMailComparator, JMAPMailFilterCondition},
    JMAPMailId, JMAPMailStoreChanges, JMAPMailStoreQuery,
};

impl<'x, T> JMAPMailStoreChanges<'x> for JMAPLocalStore<T>
where
    T: Store<'x>,
{
    type Set = T::Set;

    fn mail_changes(
        &'x self,
        account: store::AccountId,
        since_state: JMAPState,
        max_changes: usize,
    ) -> store::Result<JMAPChangesResponse> {
        self.get_changes(account, JMAP_MAIL, since_state, max_changes)
    }

    fn mail_query_changes(
        &'x self,
        query: JMAPQueryChanges<JMAPMailFilterCondition<'x>, JMAPMailComparator<'x>, JMAPMailId>,
        collapse_threads: bool,
    ) -> store::Result<JMAPQueryChangesResponse<JMAPMailId>> {
        let changes = self.get_changes(
            query.account_id,
            JMAP_MAIL,
            query.since_query_state,
            query.max_changes,
        )?;

        let mut removed;
        let mut added;

        let total = if changes.total_changes > 0 || query.calculate_total {
            let query_results = self.mail_query(
                JMAPQuery {
                    account_id: query.account_id,
                    filter: query.filter,
                    sort: query.sort,
                    position: 0,
                    anchor: None,
                    anchor_offset: 0,
                    limit: 0,
                    calculate_total: true,
                },
                collapse_threads,
            )?;

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
                        if let Some(up_to_id) = &query.up_to_id {
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

        Ok(JMAPQueryChangesResponse {
            old_query_state: changes.old_state,
            new_query_state: changes.new_state,
            total,
            removed,
            added,
        })
    }
}
