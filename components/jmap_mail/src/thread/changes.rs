use jmap::{
    jmap_store::changes::{ChangesObject, JMAPChanges},
    request::changes::{ChangesRequest, ChangesResponse},
};
use store::{JMAPStore, Store};

use super::schema::Thread;

impl ChangesObject for Thread {
    type ChangesResponse = ();
}

pub trait JMAPThreadChanges {
    fn thread_changes(&self, request: ChangesRequest) -> jmap::Result<ChangesResponse<Thread>>;
}

impl<T> JMAPThreadChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn thread_changes(&self, request: ChangesRequest) -> jmap::Result<ChangesResponse<Thread>> {
        self.changes(request)
    }
}
