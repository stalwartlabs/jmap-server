use jmap::jmap_store::changes::JMAPChanges;
use jmap::protocol::json::JSONValue;
use jmap::request::changes::ChangesRequest;
use store::Store;
use store::{Collection, JMAPStore};

use super::MailboxProperties;

pub trait JMAPMailMailboxChanges {
    fn mailbox_changes(&self, request: ChangesRequest) -> jmap::Result<JSONValue>;
}

impl<T> JMAPMailMailboxChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mailbox_changes(&self, request: ChangesRequest) -> jmap::Result<JSONValue> {
        let mut changes = self.get_jmap_changes(
            request.account_id,
            Collection::Mailbox,
            request.since_state.clone(),
            request.max_changes,
        )?;

        if changes.has_children_changes {
            changes.result.as_object_mut().insert(
                "updatedProperties".to_string(),
                vec![
                    MailboxProperties::TotalEmails.into(),
                    MailboxProperties::UnreadEmails.into(),
                    MailboxProperties::TotalThreads.into(),
                    MailboxProperties::UnreadThreads.into(),
                ]
                .into(),
            );
        }

        Ok(changes.result)
    }
}
