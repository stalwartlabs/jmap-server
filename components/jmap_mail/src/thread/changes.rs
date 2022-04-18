use jmap::{changes::JMAPChanges, json::JSONValue, request::ChangesRequest, JMAPError};
use store::{Collection, JMAPStore, Store};

pub trait JMAPMailThreadChanges {
    fn thread_changes(&self, request: ChangesRequest) -> jmap::Result<JSONValue>;
}

impl<T> JMAPMailThreadChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn thread_changes(&self, request: ChangesRequest) -> jmap::Result<JSONValue> {
        Ok(self
            .get_jmap_changes(
                request.account_id,
                Collection::Thread,
                request.since_state,
                request.max_changes,
            )
            .map_err(JMAPError::ServerFail)?
            .result)
    }
}
