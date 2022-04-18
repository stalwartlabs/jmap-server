use jmap::changes::JMAPChanges;
use jmap::json::JSONValue;
use jmap::request::ChangesRequest;
use jmap::JMAPError;
use store::{Collection, JMAPStore, Store};

pub trait JMAPMailChanges {
    fn mail_changes(&self, request: ChangesRequest) -> jmap::Result<JSONValue>;
}

impl<T> JMAPMailChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_changes(&self, request: ChangesRequest) -> jmap::Result<JSONValue> {
        self.get_jmap_changes(
            request.account_id,
            Collection::Mail,
            request.since_state,
            request.max_changes,
        )
        .map(|r| r.result)
        .map_err(JMAPError::ServerFail)
    }
}
