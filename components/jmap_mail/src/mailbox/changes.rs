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

use super::{
    query::JMAPMailboxQuery,
    schema::{Mailbox, Property},
};

#[derive(Debug, serde::Serialize, Default)]
pub struct ChangesResponseArguments {
    #[serde(rename = "updatedProperties")]
    updated_properties: Option<Vec<Property>>,
}

impl ChangesObject for Mailbox {
    type ChangesResponse = ChangesResponseArguments;
}

pub trait JMAPMailboxChanges {
    fn mailbox_changes(&self, request: ChangesRequest) -> jmap::Result<ChangesResponse<Mailbox>>;
    fn mailbox_query_changes(
        &self,
        request: QueryChangesRequest<Mailbox>,
    ) -> jmap::Result<QueryChangesResponse>;
}

impl<T> JMAPMailboxChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mailbox_changes(&self, request: ChangesRequest) -> jmap::Result<ChangesResponse<Mailbox>> {
        self.changes(request)
            .map(|mut r: ChangesResponse<Mailbox>| {
                if r.has_children_changes {
                    r.arguments.updated_properties = vec![
                        Property::TotalEmails,
                        Property::UnreadEmails,
                        Property::TotalThreads,
                        Property::UnreadThreads,
                    ]
                    .into();
                }
                r
            })
    }

    fn mailbox_query_changes(
        &self,
        request: QueryChangesRequest<Mailbox>,
    ) -> jmap::Result<QueryChangesResponse> {
        let mut helper = QueryChangesHelper::new(self, request)?;
        let has_changes = helper.has_changes();

        helper.query_changes(if let Some(has_changes) = has_changes {
            self.mailbox_query(has_changes)?.into()
        } else {
            None
        })
    }
}
