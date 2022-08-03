use super::BATCH_MAX_SIZE;
use crate::cluster::log::changes_merge::MergedChanges;
use crate::cluster::log::Update;
use crate::JMAPServer;
use jmap::jmap_store::raft::JMAPRaftStore;
use jmap::principal::schema::Principal;
use jmap::push_subscription::schema::PushSubscription;
use jmap_mail::email_submission::schema::EmailSubmission;
use jmap_mail::identity::schema::Identity;
use jmap_mail::mail::schema::Email;
use jmap_mail::mailbox::schema::Mailbox;
use jmap_mail::vacation_response::schema::VacationResponse;
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::tracing::debug;
use store::{AccountId, Store};

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn prepare_changes(
        &self,
        account_id: AccountId,
        collection: Collection,
        changes: &mut MergedChanges,
    ) -> store::Result<Vec<Update>> {
        let mut batch_size = 0;
        let mut updates = Vec::new();

        loop {
            let (document_id, is_insert) = if let Some(document_id) = changes.inserts.min() {
                changes.inserts.remove(document_id);
                (document_id, true)
            } else if let Some(document_id) = changes.updates.min() {
                changes.updates.remove(document_id);
                (document_id, false)
            } else {
                break;
            };

            let store = self.store.clone();
            let item = self
                .spawn_worker(move || match collection {
                    Collection::Mail => {
                        store.raft_prepare_update::<Email>(account_id, document_id, is_insert)
                    }
                    Collection::Mailbox => {
                        store.raft_prepare_update::<Mailbox>(account_id, document_id, is_insert)
                    }
                    Collection::Principal => {
                        store.raft_prepare_update::<Principal>(account_id, document_id, is_insert)
                    }
                    Collection::PushSubscription => store.raft_prepare_update::<PushSubscription>(
                        account_id,
                        document_id,
                        is_insert,
                    ),
                    Collection::Identity => {
                        store.raft_prepare_update::<Identity>(account_id, document_id, is_insert)
                    }
                    Collection::EmailSubmission => store.raft_prepare_update::<EmailSubmission>(
                        account_id,
                        document_id,
                        is_insert,
                    ),
                    Collection::VacationResponse => store.raft_prepare_update::<VacationResponse>(
                        account_id,
                        document_id,
                        is_insert,
                    ),
                    Collection::Thread | Collection::None => Err(StoreError::InternalError(
                        "Unsupported collection for changes".into(),
                    )),
                })
                .await?;

            if let Some(item) = item {
                if updates.is_empty() {
                    updates.push(Update::Begin {
                        account_id,
                        collection,
                    });
                }
                batch_size += item.size();
                updates.push(Update::Document { update: item });
            } else {
                debug!(
                    "Warning: Failed to fetch item in collection {:?}",
                    collection,
                );
            }

            if batch_size >= BATCH_MAX_SIZE {
                break;
            }
        }

        if !updates.is_empty() {
            updates.push(Update::Eof);
        }

        Ok(updates)
    }
}
