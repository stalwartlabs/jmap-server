use jmap::{
    jmap_store::changes::{ChangesObject, JMAPChanges},
    request::changes::{ChangesRequest, ChangesResponse},
};
use store::{JMAPStore, Store};

use super::schema::Identity;

impl ChangesObject for Identity {
    type ChangesResponse = ();
}

pub trait JMAPIdentityChanges {
    fn identity_changes(&self, request: ChangesRequest) -> jmap::Result<ChangesResponse<Identity>>;
}

impl<T> JMAPIdentityChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn identity_changes(&self, request: ChangesRequest) -> jmap::Result<ChangesResponse<Identity>> {
        self.changes(request)
    }
}
