use jmap::{
    error::method::MethodError, jmap_store::changes::JMAPChanges, protocol::json::JSONValue,
    request::changes::ChangesRequest,
};
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
            .map_err(MethodError::ServerFail)?
            .result)
    }
}
