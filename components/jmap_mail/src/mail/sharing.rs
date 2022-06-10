use std::sync::Arc;

use store::{
    core::{acl::ACL, collection::Collection, error::StoreError, tag::Tag},
    roaring::RoaringBitmap,
    AccountId, JMAPStore, SharedResource, Store,
};

use super::MessageField;

pub trait JMAPShareMail<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_shared_folders(
        &self,
        owner_id: AccountId,
        shared_to: &[AccountId],
    ) -> store::Result<Arc<Option<RoaringBitmap>>>;
    fn mail_shared_messages(
        &self,
        owner_id: AccountId,
        shared_to: &[AccountId],
    ) -> store::Result<Arc<Option<RoaringBitmap>>>;
}

impl<T> JMAPShareMail<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_shared_folders(
        &self,
        owner_id: AccountId,
        shared_to: &[AccountId],
    ) -> store::Result<Arc<Option<RoaringBitmap>>> {
        self.shared_documents
            .try_get_with::<_, StoreError>(
                SharedResource::new(
                    owner_id,
                    shared_to.first().copied().unwrap(),
                    Collection::Mail,
                ),
                || {
                    Ok(Arc::new(self.shared_documents(
                        shared_to,
                        owner_id,
                        Collection::Mailbox,
                        ACL::ReadItems.into(),
                    )?))
                },
            )
            .map_err(|e| e.as_ref().clone())
    }

    fn mail_shared_messages(
        &self,
        owner_id: AccountId,
        shared_to: &[AccountId],
    ) -> store::Result<Arc<Option<RoaringBitmap>>> {
        self.shared_documents
            .try_get_with::<_, StoreError>(
                SharedResource::new(
                    owner_id,
                    shared_to.first().copied().unwrap(),
                    Collection::Mail,
                ),
                || {
                    Ok(Arc::new(
                        if let Some(shared_folders) =
                            self.mail_shared_folders(owner_id, shared_to)?.as_ref()
                        {
                            let mut shared_messages = RoaringBitmap::new();
                            for mailbox_id in shared_folders {
                                if let Some(message_ids) = self.get_tag(
                                    owner_id,
                                    Collection::Mail,
                                    MessageField::Mailbox.into(),
                                    Tag::Id(mailbox_id),
                                )? {
                                    shared_messages |= message_ids;
                                }
                            }
                            if !shared_messages.is_empty() {
                                shared_messages.into()
                            } else {
                                None
                            }
                        } else {
                            None
                        },
                    ))
                },
            )
            .map_err(|e| e.as_ref().clone())
    }
}
