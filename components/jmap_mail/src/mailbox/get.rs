use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use jmap::changes::JMAPChanges;

use jmap::id::JMAPIdSerialize;

use jmap::request::GetRequest;
use jmap::{json::JSONValue, JMAPError};

use store::roaring::RoaringBitmap;

use store::{AccountId, Collection, JMAPId, JMAPIdPrefix, JMAPStore, StoreError, Tag};
use store::{DocumentId, Store};

use crate::mail::{Keyword, MessageField};

use super::{Mailbox, MailboxProperties};

pub trait JMAPMailMailboxGet {
    fn mailbox_get(&self, request: GetRequest) -> jmap::Result<JSONValue>;

    fn count_threads(
        &self,
        account_id: AccountId,
        document_ids: Option<RoaringBitmap>,
    ) -> store::Result<usize>;

    fn get_mailbox_tag(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Option<RoaringBitmap>>;

    fn get_mailbox_unread_tag(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
        mail_document_ids: &Option<RoaringBitmap>,
    ) -> store::Result<Option<RoaringBitmap>>;
}

impl<T> JMAPMailMailboxGet for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mailbox_get(&self, request: GetRequest) -> jmap::Result<JSONValue> {
        let properties = request
            .properties
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
            });

        let request_ids = if let Some(request_ids) = request.ids {
            if request_ids.len() > self.config.max_objects_in_get {
                return Err(JMAPError::RequestTooLarge);
            } else {
                request_ids
            }
        } else {
            self.get_document_ids(request.account_id, Collection::Mailbox)?
                .unwrap_or_default()
                .into_iter()
                .take(self.config.max_objects_in_get)
                .map(|id| id as JMAPId)
                .collect::<Vec<JMAPId>>()
        };

        let document_ids = self
            .get_document_ids(request.account_id, Collection::Mailbox)?
            .unwrap_or_default();
        let mail_document_ids = self.get_document_ids(request.account_id, Collection::Mail)?;
        let mut not_found = Vec::new();
        let mut results = Vec::with_capacity(request_ids.len());

        for jmap_id in request_ids {
            let document_id = jmap_id.get_document_id();
            if !document_ids.contains(document_id) {
                not_found.push(jmap_id.to_jmap_string().into());
                continue;
            }
            let mut mailbox = if properties.iter().any(|p| {
                matches!(
                    p,
                    MailboxProperties::Name
                        | MailboxProperties::ParentId
                        | MailboxProperties::Role
                        | MailboxProperties::SortOrder
                )
            }) {
                Some(
                    self.get_document_value::<Mailbox>(
                        request.account_id,
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

            for property in &properties {
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
                        MailboxProperties::Role => {
                            std::mem::take(&mut mailbox.as_mut().unwrap().role)
                                .map(|v| v.into())
                                .unwrap_or_default()
                        }
                        MailboxProperties::SortOrder => mailbox.as_ref().unwrap().sort_order.into(),
                        MailboxProperties::IsSubscribed => true.into(), //TODO implement
                        MailboxProperties::MyRights => JSONValue::Object(HashMap::new()), //TODO implement
                        MailboxProperties::TotalEmails => self
                            .get_mailbox_tag(request.account_id, document_id)?
                            .map(|v| v.len())
                            .unwrap_or(0)
                            .into(),
                        MailboxProperties::UnreadEmails => self
                            .get_mailbox_unread_tag(
                                request.account_id,
                                document_id,
                                &mail_document_ids,
                            )?
                            .map(|v| v.len())
                            .unwrap_or(0)
                            .into(),
                        MailboxProperties::TotalThreads => self
                            .count_threads(
                                request.account_id,
                                self.get_mailbox_tag(request.account_id, document_id)?,
                            )?
                            .into(),
                        MailboxProperties::UnreadThreads => self
                            .count_threads(
                                request.account_id,
                                self.get_mailbox_unread_tag(
                                    request.account_id,
                                    document_id,
                                    &mail_document_ids,
                                )?,
                            )?
                            .into(),
                        MailboxProperties::HasRole => JSONValue::Null,
                    };

                    entry.insert(value);
                }
            }

            results.push(result.into());
        }

        let mut obj = HashMap::new();
        obj.insert(
            "state".to_string(),
            self.get_state(request.account_id, Collection::Mailbox)?
                .into(),
        );
        obj.insert("list".to_string(), results.into());
        obj.insert("notFound".to_string(), not_found.into());
        Ok(obj.into())
    }

    fn count_threads(
        &self,
        account_id: AccountId,
        document_ids: Option<RoaringBitmap>,
    ) -> store::Result<usize> {
        Ok(if let Some(document_ids) = document_ids {
            let mut thread_ids = HashSet::new();
            self.get_multi_document_tag_id(
                account_id,
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

    fn get_mailbox_tag(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Option<RoaringBitmap>> {
        self.get_tag(
            account_id,
            Collection::Mail,
            MessageField::Mailbox.into(),
            Tag::Id(document_id),
        )
    }

    fn get_mailbox_unread_tag(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
        mail_document_ids: &Option<RoaringBitmap>,
    ) -> store::Result<Option<RoaringBitmap>> {
        if let Some(mail_document_ids) = mail_document_ids {
            match self.get_mailbox_tag(account_id, document_id) {
                Ok(Some(mailbox)) => {
                    match self.get_tag(
                        account_id,
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
