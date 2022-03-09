use crate::query::JMAPMailQuery;
use crate::query::{JMAPMailComparator, JMAPMailFilterCondition, JMAPMailQueryArguments};
use jmap_store::changes::query_changes;
use jmap_store::JMAPQueryRequest;
use jmap_store::{
    changes::{JMAPChanges, JMAPChangesRequest, JMAPChangesResponse, JMAPQueryChangesResponse},
    JMAPQueryChangesRequest, JMAP_MAIL,
};
use store::{JMAPStore, Store};

pub trait JMAPMailChanges {
    fn mail_changes(
        &self,
        request: JMAPChangesRequest,
    ) -> jmap_store::Result<JMAPChangesResponse<()>>;

    fn mail_query_changes(
        &self,
        query: JMAPQueryChangesRequest<
            JMAPMailFilterCondition,
            JMAPMailComparator,
            JMAPMailQueryArguments,
        >,
    ) -> jmap_store::Result<JMAPQueryChangesResponse>;
}

impl<T> JMAPMailChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_changes(
        &self,
        request: JMAPChangesRequest,
    ) -> jmap_store::Result<JMAPChangesResponse<()>> {
        self.get_jmap_changes(
            request.account,
            JMAP_MAIL,
            request.since_state,
            request.max_changes,
        )
        .map_err(|e| e.into())
    }

    fn mail_query_changes(
        &self,
        query: JMAPQueryChangesRequest<
            JMAPMailFilterCondition,
            JMAPMailComparator,
            JMAPMailQueryArguments,
        >,
    ) -> jmap_store::Result<JMAPQueryChangesResponse> {
        let changes = self.get_jmap_changes(
            query.account_id,
            JMAP_MAIL,
            query.since_query_state,
            query.max_changes,
        )?;

        let query_results = if changes.total_changes > 0 || query.calculate_total {
            Some(self.mail_query(JMAPQueryRequest {
                account_id: query.account_id,
                filter: query.filter,
                sort: query.sort,
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 0,
                calculate_total: true,
                arguments: query.arguments,
            })?)
        } else {
            None
        };

        Ok(query_changes(changes, query_results, query.up_to_id))
    }
}
