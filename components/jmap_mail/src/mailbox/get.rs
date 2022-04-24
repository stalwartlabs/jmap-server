use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use jmap::id::JMAPIdSerialize;
use jmap::jmap_store::get::GetObject;
use jmap::jmap_store::orm::JMAPOrm;
use jmap::protocol::json::JSONValue;
use jmap::request::get::GetRequest;
use store::roaring::RoaringBitmap;

use store::{AccountId, Collection, JMAPId, JMAPIdPrefix, JMAPStore, StoreError, Tag};
use store::{DocumentId, Store};

use crate::mail::{Keyword, MessageField};

use super::MailboxProperty;

pub struct GetMailbox<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    store: &'y JMAPStore<T>,
    account_id: AccountId,
    mail_document_ids: Option<RoaringBitmap>,
    fetch_mailbox: bool,
}

impl<'y, T> GetObject<'y, T> for GetMailbox<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    type Property = MailboxProperty;

    fn new(
        store: &'y JMAPStore<T>,
        request: &mut GetRequest,
        properties: &[Self::Property],
    ) -> jmap::Result<Self> {
        Ok(GetMailbox {
            store,
            account_id: request.account_id,
            mail_document_ids: store.get_document_ids(request.account_id, Collection::Mail)?,
            fetch_mailbox: properties.iter().any(|p| {
                matches!(
                    p,
                    MailboxProperty::Name
                        | MailboxProperty::ParentId
                        | MailboxProperty::Role
                        | MailboxProperty::SortOrder
                )
            }),
        })
    }

    fn get_item(
        &self,
        jmap_id: JMAPId,
        properties: &[Self::Property],
    ) -> jmap::Result<Option<JSONValue>> {
        let document_id = jmap_id.get_document_id();
        let mut mailbox = if self.fetch_mailbox {
            Some(
                self.store
                    .get_orm::<MailboxProperty>(self.account_id, document_id)?
                    .ok_or_else(|| {
                        StoreError::InternalError("Mailbox data not found".to_string())
                    })?,
            )
        } else {
            None
        };

        let mut result: HashMap<String, JSONValue> = HashMap::new();

        for property in properties {
            if let Entry::Vacant(entry) = result.entry(property.to_string()) {
                let value = match property {
                    MailboxProperty::Id => jmap_id.to_jmap_string().into(),
                    MailboxProperty::Name | MailboxProperty::Role | MailboxProperty::SortOrder => {
                        mailbox
                            .as_mut()
                            .unwrap()
                            .remove(property)
                            .unwrap_or_default()
                    }
                    MailboxProperty::ParentId => {
                        if let Some(parent_id) =
                            mailbox.as_ref().unwrap().get_unsigned_int(property)
                        {
                            if parent_id > 0 {
                                (parent_id - 1).to_jmap_string().into()
                            } else {
                                JSONValue::Null
                            }
                        } else {
                            JSONValue::Null
                        }
                    }
                    MailboxProperty::IsSubscribed => true.into(), //TODO implement
                    MailboxProperty::MyRights => JSONValue::Object(HashMap::new()), //TODO implement
                    MailboxProperty::TotalEmails => self
                        .get_mailbox_tag(document_id)?
                        .map(|v| v.len())
                        .unwrap_or(0)
                        .into(),
                    MailboxProperty::UnreadEmails => {
                        self //TODO check unread counts everywhere
                            .get_mailbox_unread_tag(document_id)?
                            .map(|v| v.len())
                            .unwrap_or(0)
                            .into()
                    }
                    MailboxProperty::TotalThreads => self
                        .count_threads(self.get_mailbox_tag(document_id)?)?
                        .into(),
                    MailboxProperty::UnreadThreads => self
                        .count_threads(self.get_mailbox_unread_tag(document_id)?)?
                        .into(),
                };

                entry.insert(value);
            }
        }

        Ok(Some(result.into()))
    }

    fn map_ids<W>(&self, document_ids: W) -> jmap::Result<Vec<JMAPId>>
    where
        W: Iterator<Item = DocumentId>,
    {
        Ok(document_ids.map(|id| id as JMAPId).collect())
    }

    fn is_virtual() -> bool {
        false
    }

    fn default_properties() -> Vec<Self::Property> {
        vec![
            MailboxProperty::Id,
            MailboxProperty::Name,
            MailboxProperty::ParentId,
            MailboxProperty::Role,
            MailboxProperty::SortOrder,
            MailboxProperty::IsSubscribed,
            MailboxProperty::TotalEmails,
            MailboxProperty::UnreadEmails,
            MailboxProperty::TotalThreads,
            MailboxProperty::UnreadThreads,
            MailboxProperty::MyRights,
        ]
    }
}

impl<'y, T> GetMailbox<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn count_threads(&self, document_ids: Option<RoaringBitmap>) -> store::Result<usize> {
        Ok(if let Some(document_ids) = document_ids {
            let mut thread_ids = HashSet::new();
            self.store
                .get_multi_document_tag_id(
                    self.account_id,
                    Collection::Mail,
                    document_ids.into_iter(),
                    MessageField::ThreadId.into(),
                )?
                .into_iter()
                .for_each(|thread_id| {
                    if let Some(thread_id) = thread_id {
                        thread_ids.insert(*thread_id);
                    }
                });
            thread_ids.len()
        } else {
            0
        })
    }

    fn get_mailbox_tag(&self, document_id: DocumentId) -> store::Result<Option<RoaringBitmap>> {
        self.store.get_tag(
            self.account_id,
            Collection::Mail,
            MessageField::Mailbox.into(),
            Tag::Id(document_id),
        )
    }

    fn get_mailbox_unread_tag(
        &self,
        document_id: DocumentId,
    ) -> store::Result<Option<RoaringBitmap>> {
        if let Some(mail_document_ids) = &self.mail_document_ids {
            match self.get_mailbox_tag(document_id) {
                Ok(Some(mailbox)) => {
                    match self.store.get_tag(
                        self.account_id,
                        Collection::Mail,
                        MessageField::Keyword.into(),
                        Tag::Static(Keyword::SEEN),
                    ) {
                        Ok(Some(mut seen)) => {
                            seen ^= mail_document_ids;
                            seen &= &mailbox;
                            if !seen.is_empty() {
                                Ok(Some(seen))
                            } else {
                                Ok(None)
                            }
                        }
                        Ok(None) => Ok(None),
                        Err(e) => Err(e),
                    }
                }
                other => other,
            }
        } else {
            Ok(None)
        }
    }
}
