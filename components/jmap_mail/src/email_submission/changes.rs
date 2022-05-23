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

use super::{query::JMAPEmailSubmissionQuery, schema::EmailSubmission};

impl ChangesObject for EmailSubmission {
    type ChangesResponse = ();
}

pub trait JMAPEmailSubmissionChanges {
    fn email_submission_changes(
        &self,
        request: ChangesRequest,
    ) -> jmap::Result<ChangesResponse<EmailSubmission>>;
    fn email_submission_query_changes(
        &self,
        request: QueryChangesRequest<EmailSubmission>,
    ) -> jmap::Result<QueryChangesResponse>;
}

impl<T> JMAPEmailSubmissionChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn email_submission_changes(
        &self,
        request: ChangesRequest,
    ) -> jmap::Result<ChangesResponse<EmailSubmission>> {
        self.changes(request)
    }

    fn email_submission_query_changes(
        &self,
        request: QueryChangesRequest<EmailSubmission>,
    ) -> jmap::Result<QueryChangesResponse> {
        let mut helper = QueryChangesHelper::new(self, request)?;
        let has_changes = helper.has_changes();

        helper.query_changes(if let Some(has_changes) = has_changes {
            self.email_submission_query(has_changes)?.into()
        } else {
            None
        })
    }
}
