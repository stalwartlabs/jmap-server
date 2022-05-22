use jmap::{
    jmap_store::{
        changes::{ChangesObject, JMAPChanges},
        query_changes::QueryChangesHelper,
    },
    request::{
        changes::{ChangesRequest, ChangesResponse},
        query_changes::{QueryChangesRequest, QueryChangesResponse},
    },
};
use store::{JMAPStore, Store};

use super::{query::JMAPMailQuery, schema::Email};

impl ChangesObject for Email {
    type ChangesResponse = ();
}

pub trait JMAPMailChanges {
    fn mail_changes(&self, request: ChangesRequest) -> jmap::Result<ChangesResponse<Email>>;
    fn mail_query_changes(
        &self,
        request: QueryChangesRequest<Email>,
    ) -> jmap::Result<QueryChangesResponse>;
}

impl<T> JMAPMailChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_changes(&self, request: ChangesRequest) -> jmap::Result<ChangesResponse<Email>> {
        self.changes(request)
    }

    fn mail_query_changes(
        &self,
        request: QueryChangesRequest<Email>,
    ) -> jmap::Result<QueryChangesResponse> {
        let mut helper = QueryChangesHelper::new(self, request)?;
        let has_changes = helper.has_changes();

        helper.query_changes(if let Some(has_changes) = has_changes {
            self.mail_query(has_changes)?.into()
        } else {
            None
        })
    }
}
