use jmap::push_subscription::set::JMAPSetPushSubscription;
use jmap_mail::email_submission::set::JMAPSetEmailSubmission;
use jmap_mail::identity::set::JMAPSetIdentity;
use jmap_mail::mail::set::JMAPSetMail;
use jmap_mail::mailbox::set::JMAPSetMailbox;
use jmap_mail::vacation_response::set::JMAPSetVacationResponse;
use jmap_sharing::principal::set::JMAPSetPrincipal;
use store::core::collection::Collection;
use store::core::document::Document;
use store::write::batch::WriteBatch;
use store::{DocumentId, JMAPStore, Store};

pub trait RaftStoreDelete {
    fn delete_document(
        &self,
        write_batch: &mut WriteBatch,
        collection: Collection,
        document_id: DocumentId,
    ) -> store::Result<()>;
}

impl<T> RaftStoreDelete for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn delete_document(
        &self,
        write_batch: &mut WriteBatch,
        collection: Collection,
        document_id: DocumentId,
    ) -> store::Result<()> {
        let mut document = Document::new(collection, document_id);
        match collection {
            Collection::Mail => {
                self.mail_delete(write_batch.account_id, None, &mut document)?;
            }
            Collection::Mailbox => {
                self.mailbox_delete(write_batch.account_id, &mut document)?;
            }
            Collection::Principal => {
                self.principal_delete(write_batch.account_id, &mut document)?
            }
            Collection::PushSubscription => {
                self.push_subscription_delete(write_batch.account_id, &mut document)?
            }
            Collection::Identity => self.identity_delete(write_batch.account_id, &mut document)?,
            Collection::EmailSubmission => {
                self.email_submission_delete(write_batch.account_id, &mut document)?
            }
            Collection::VacationResponse => {
                self.vacation_response_delete(write_batch.account_id, &mut document)?
            }
            Collection::Thread | Collection::None => unreachable!(),
        }
        write_batch.delete_document(document);
        Ok(())
    }
}
