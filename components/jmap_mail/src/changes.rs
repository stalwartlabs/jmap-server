use jmap_store::{
    changes::{
        JMAPChangesRequest, JMAPChangesResponse, JMAPLocalChanges, JMAPLocalQueryChanges,
        JMAPQueryChangesResponse,
    },
    local_store::JMAPLocalStore,
    JMAPQueryChangesRequest, JMAP_MAIL,
};
use store::Store;

use crate::{
    query::{JMAPMailComparator, JMAPMailFilterCondition, JMAPMailQueryArguments},
    JMAPMailChanges, JMAPMailQuery,
};

impl<'x, T> JMAPMailChanges<'x> for JMAPLocalStore<T>
where
    T: Store<'x>,
{
    fn mail_changes(
        &'x self,
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
        &'x self,
        query: JMAPQueryChangesRequest<
            JMAPMailFilterCondition,
            JMAPMailComparator,
            JMAPMailQueryArguments,
        >,
    ) -> jmap_store::Result<JMAPQueryChangesResponse> {
        self.query_changes(query, JMAPLocalStore::mail_query, JMAP_MAIL)
    }
}
