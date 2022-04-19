use crate::mail::query::JMAPMailQuery;

use jmap::{
    jmap_store::changes::JMAPChanges,
    protocol::json::JSONValue,
    request::{
        query::{QueryRequest, QueryResult},
        query_changes::QueryChangesRequest,
    },
};
use store::{Collection, JMAPStore, Store};

pub trait JMAPMailQueryChanges {
    fn mail_query_changes(&self, query: QueryChangesRequest) -> jmap::Result<JSONValue>;
}

impl<T> JMAPMailQueryChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_query_changes(&self, query: QueryChangesRequest) -> jmap::Result<JSONValue> {
        let changes = self.get_jmap_changes(
            query.account_id,
            Collection::Mail,
            query.since_query_state,
            query.max_changes,
        )?;

        let query_result = if changes.total_changes > 0 || query.calculate_total {
            self.mail_query_ext(QueryRequest {
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
            QueryResult {
                is_immutable: false,
                result: JSONValue::Null,
            }
        };

        Ok(changes.query(query_result, query.up_to_id))
    }
}
