use jmap::{
    jmap_store::changes::{ChangesObject, ChangesResult, JMAPChanges},
    request::changes::ChangesRequest,
};
use store::{Collection, JMAPStore, Store};

pub struct ChangesThread {}

impl ChangesObject for ChangesThread {
    fn collection() -> Collection {
        Collection::Thread
    }

    fn handle_result(_result: &mut ChangesResult) {}
}

pub trait JMAPThreadChanges {
    fn thread_changes(&self, request: ChangesRequest) -> jmap::Result<ChangesResult>;
}

impl<T> JMAPThreadChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn thread_changes(&self, request: ChangesRequest) -> jmap::Result<ChangesResult> {
        self.changes::<ChangesThread>(request)
    }
}
