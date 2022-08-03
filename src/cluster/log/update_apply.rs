use jmap::jmap_store::raft::{JMAPRaftStore, RaftUpdate};
use jmap::principal::schema::Principal;
use jmap::push_subscription::schema::PushSubscription;
use jmap_mail::email_submission::schema::EmailSubmission;
use jmap_mail::identity::schema::Identity;
use jmap_mail::mail::schema::Email;
use jmap_mail::mailbox::schema::Mailbox;
use jmap_mail::vacation_response::schema::VacationResponse;
use store::core::collection::Collection;
use store::write::batch::WriteBatch;
use store::{JMAPStore, Store};

pub trait RaftStoreApplyUpdate {
    fn apply_update(
        &self,
        write_batch: &mut WriteBatch,
        collection: Collection,
        update: RaftUpdate,
    ) -> store::Result<()>;
}

impl<T> RaftStoreApplyUpdate for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn apply_update(
        &self,
        write_batch: &mut WriteBatch,
        collection: Collection,
        update: RaftUpdate,
    ) -> store::Result<()> {
        match collection {
            Collection::Mail => self.raft_apply_update::<Email>(write_batch, update),
            Collection::Mailbox => self.raft_apply_update::<Mailbox>(write_batch, update),
            Collection::Principal => self.raft_apply_update::<Principal>(write_batch, update),
            Collection::PushSubscription => {
                self.raft_apply_update::<PushSubscription>(write_batch, update)
            }
            Collection::Identity => self.raft_apply_update::<Identity>(write_batch, update),
            Collection::EmailSubmission => {
                self.raft_apply_update::<EmailSubmission>(write_batch, update)
            }
            Collection::VacationResponse => {
                self.raft_apply_update::<VacationResponse>(write_batch, update)
            }
            Collection::Thread | Collection::None => {
                panic!("Unsupported update for {:?}", collection)
            }
        }
    }
}
