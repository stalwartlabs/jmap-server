use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use jmap::id::JMAPIdSerialize;
use jmap::jmap_store::get::GetObject;
use jmap::protocol::json::JSONValue;
use jmap::request::get::GetRequest;
use store::roaring::RoaringBitmap;

use store::{AccountId, Collection, JMAPId, JMAPIdPrefix, JMAPStore, StoreError, Tag};
use store::{DocumentId, Store};

use crate::mail::{Keyword, MessageField};

use super::{Mailbox, MailboxProperties};

pub struct GetMailbox<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    store: &'y JMAPStore<T>,
    account_id: AccountId,
    properties: Vec<MailboxProperties>,
    mail_document_ids: Option<RoaringBitmap>,
}

impl<'y, T> GetObject<'y, T> for GetMailbox<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn init(store: &'y JMAPStore<T>, request: &mut GetRequest) -> jmap::Result<Self> {
        Ok(GetMailbox {
            store,
            account_id: request.account_id,
            properties: std::mem::take(&mut request.properties)
                .parse_array_items(true)?
                .unwrap_or_else(|| {
                    vec![
                        MailboxProperties::Id,
                        MailboxProperties::Name,
                        MailboxProperties::ParentId,
                        MailboxProperties::Role,
                        MailboxProperties::SortOrder,
                        MailboxProperties::IsSubscribed,
                        MailboxProperties::TotalEmails,
                        MailboxProperties::UnreadEmails,
                        MailboxProperties::TotalThreads,
                        MailboxProperties::UnreadThreads,
                        MailboxProperties::MyRights,
                    ]
                }),
            mail_document_ids: store.get_document_ids(request.account_id, Collection::Mail)?,
        })
    }

    fn get_item(&self, jmap_id: JMAPId) -> jmap::Result<Option<JSONValue>> {
        let document_id = jmap_id.get_document_id();
        let mut mailbox = if self.properties.iter().any(|p| {
            matches!(
                p,
                MailboxProperties::Name
                    | MailboxProperties::ParentId
                    | MailboxProperties::Role
                    | MailboxProperties::SortOrder
            )
        }) {
            Some(
                self.store
                    .get_document_value::<Mailbox>(
                        self.account_id,
                        Collection::Mailbox,
                        document_id,
                        MailboxProperties::Id.into(),
                    )?
                    .ok_or_else(|| {
                        StoreError::InternalError("Mailbox data not found".to_string())
                    })?,
            )
        } else {
            None
        };

        let mut result: HashMap<String, JSONValue> = HashMap::new();

        for property in &self.properties {
            if let Entry::Vacant(entry) = result.entry(property.to_string()) {
                let value = match property {
                    MailboxProperties::Id => jmap_id.to_jmap_string().into(),
                    MailboxProperties::Name => {
                        std::mem::take(&mut mailbox.as_mut().unwrap().name).into()
                    }
                    MailboxProperties::ParentId => {
                        if mailbox.as_ref().unwrap().parent_id > 0 {
                            (mailbox.as_ref().unwrap().parent_id - 1)
                                .to_jmap_string()
                                .into()
                        } else {
                            JSONValue::Null
                        }
                    }
                    MailboxProperties::Role => std::mem::take(&mut mailbox.as_mut().unwrap().role)
                        .map(|v| v.into())
                        .unwrap_or_default(),
                    MailboxProperties::SortOrder => mailbox.as_ref().unwrap().sort_order.into(),
                    MailboxProperties::IsSubscribed => true.into(), //TODO implement
                    MailboxProperties::MyRights => JSONValue::Object(HashMap::new()), //TODO implement
                    MailboxProperties::TotalEmails => self
                        .get_mailbox_tag(document_id)?
                        .map(|v| v.len())
                        .unwrap_or(0)
                        .into(),
                    MailboxProperties::UnreadEmails => {
                        self //TODO check unread counts everywhere
                            .get_mailbox_unread_tag(document_id)?
                            .map(|v| v.len())
                            .unwrap_or(0)
                            .into()
                    }
                    MailboxProperties::TotalThreads => self
                        .count_threads(self.get_mailbox_tag(document_id)?)?
                        .into(),
                    MailboxProperties::UnreadThreads => self
                        .count_threads(self.get_mailbox_unread_tag(document_id)?)?
                        .into(),
                    MailboxProperties::HasRole => JSONValue::Null,
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

    fn collection() -> Collection {
        Collection::Mailbox
    }

    fn has_virtual_ids() -> bool {
        false
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
