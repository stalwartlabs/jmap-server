use jmap::jmap_store::changes::JMAPChanges;
use jmap::protocol::json::JSONValue;
use jmap::request::query::{QueryRequest, QueryResult};
use jmap::request::query_changes::QueryChangesRequest;
use store::Store;
use store::{Collection, JMAPStore};

use super::query::JMAPMailMailboxQuery;

pub trait JMAPMailMailboxQueryChanges {
    fn mailbox_query_changes(&self, request: QueryChangesRequest) -> jmap::Result<JSONValue>;
}

impl<T> JMAPMailMailboxQueryChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mailbox_query_changes(&self, request: QueryChangesRequest) -> jmap::Result<JSONValue> {
        let changes = self.get_jmap_changes(
            request.account_id,
            Collection::Mailbox,
            request.since_query_state,
            request.max_changes,
        )?;

        let result = if changes.total_changes > 0 || request.calculate_total {
            self.mailbox_query(QueryRequest {
                account_id: request.account_id,
                filter: request.filter,
                sort: request.sort,
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 0,
                calculate_total: true,
                arguments: request.arguments,
            })?
        } else {
            JSONValue::Null
        };

        Ok(changes.query(
            QueryResult {
                is_immutable: false,
                result,
            },
            request.up_to_id,
        ))
    }
}
