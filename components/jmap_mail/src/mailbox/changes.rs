use jmap::{
    jmap_store::{
        changes::{ChangesObject, JMAPChanges},
        query_changes::QueryChangesHelper,
    },
    request::{
        changes::{ChangesRequest, ChangesResponse},
        query_changes::{QueryChangesRequest, QueryChangesResponse},
    },
    types::json_pointer::{JSONPointer, JSONPointerEval},
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

impl JSONPointerEval for ChangesResponseArguments {
    fn eval_json_pointer(&self, ptr: &JSONPointer) -> Option<Vec<u64>> {
        if ptr.is_item_query("updatedProperties") {
            Some(if let Some(updated_properties) = &self.updated_properties {
                updated_properties.iter().map(|p| *p as u64).collect()
            } else {
                Vec::with_capacity(0)
            })
        } else {
            None
        }
    }
}
