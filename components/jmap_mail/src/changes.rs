use crate::query::JMAPMailQuery;
use crate::query::{JMAPMailComparator, JMAPMailFilterCondition, JMAPMailQueryArguments};
use jmap::json::JSONValue;
use jmap::query::JMAPQueryResult;
use jmap::{
    changes::{JMAPChanges, JMAPChangesRequest},
    JMAPQueryChangesRequest,
};
use jmap::{JMAPError, JMAPQueryRequest};
use store::{Collection, JMAPStore, Store};

pub trait JMAPMailChanges {
    fn mail_changes(&self, request: JMAPChangesRequest) -> jmap::Result<JSONValue>;

    fn mail_query_changes(
        &self,
        query: JMAPQueryChangesRequest<
            JMAPMailFilterCondition,
            JMAPMailComparator,
            JMAPMailQueryArguments,
        >,
    ) -> jmap::Result<JSONValue>;
}

impl<T> JMAPMailChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_changes(&self, request: JMAPChangesRequest) -> jmap::Result<JSONValue> {
        self.get_jmap_changes(
            request.account_id,
            Collection::Mail,
            request.since_state,
            request.max_changes,
        )
        .map(|r| r.result)
        .map_err(JMAPError::InternalError)
    }

    fn mail_query_changes(
        &self,
        query: JMAPQueryChangesRequest<
            JMAPMailFilterCondition,
            JMAPMailComparator,
            JMAPMailQueryArguments,
        >,
    ) -> jmap::Result<JSONValue> {
        let changes = self.get_jmap_changes(
            query.account_id,
            Collection::Mail,
            query.since_query_state,
            query.max_changes,
        )?;

        let query_result = if changes.total_changes > 0 || query.calculate_total {
            self.mail_query_ext(JMAPQueryRequest {
                account_id: query.account_id,
                filter: query.filter,
                sort: query.sort,
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 0,
                calculate_total: true,
                arguments: query.arguments,
            })?
        } else {
            JMAPQueryResult {
                is_immutable: false,
                result: JSONValue::Null,
            }
        };

        Ok(changes.query(query_result, query.up_to_id))
    }
}
