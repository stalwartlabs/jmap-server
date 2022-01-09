use jmap_store::{
    changes::{JMAPLocalChanges, JMAPState},
    JMAPChangesResponse, JMAP_MAIL,
};

pub trait JMAPMailLocalStoreChanges<'x>: JMAPLocalChanges<'x> {
    fn mail_changes(
        &'x self,
        account: store::AccountId,
        since_state: JMAPState,
        max_changes: usize,
    ) -> jmap_store::Result<JMAPChangesResponse> {
        self.get_jmap_changes(account, JMAP_MAIL, since_state, max_changes)
            .map_err(|e| e.into())
    }
}
