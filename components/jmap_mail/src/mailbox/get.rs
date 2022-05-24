use std::collections::{HashMap, HashSet};

use jmap::jmap_store::get::{default_mapper, GetHelper, GetObject};
use jmap::jmap_store::orm::JMAPOrm;
use jmap::request::get::{GetRequest, GetResponse};
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::core::tag::Tag;
use store::roaring::RoaringBitmap;

use store::{AccountId, JMAPStore};
use store::{DocumentId, Store};

use crate::mail::schema::Keyword;
use crate::mail::MessageField;

use super::schema::{Mailbox, MailboxRights, Property, Value};

impl GetObject for Mailbox {
    type GetArguments = ();

    fn default_properties() -> Vec<Self::Property> {
        vec![
            Property::Id,
            Property::Name,
            Property::ParentId,
            Property::Role,
            Property::SortOrder,
            Property::IsSubscribed,
            Property::TotalEmails,
            Property::UnreadEmails,
            Property::TotalThreads,
            Property::UnreadThreads,
            Property::MyRights,
        ]
    }
}

pub trait JMAPGetMailbox<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mailbox_get(&self, request: GetRequest<Mailbox>) -> jmap::Result<GetResponse<Mailbox>>;
    fn mailbox_count_threads(
        &self,
        account_id: AccountId,
        document_ids: Option<RoaringBitmap>,
    ) -> store::Result<usize>;
    fn mailbox_tags(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Option<RoaringBitmap>>;
    fn mailbox_unread_tags(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
        mail_document_ids: Option<&RoaringBitmap>,
    ) -> store::Result<Option<RoaringBitmap>>;
}

impl<T> JMAPGetMailbox<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mailbox_get(&self, request: GetRequest<Mailbox>) -> jmap::Result<GetResponse<Mailbox>> {
        let helper = GetHelper::new(self, request, default_mapper.into())?;
        let fetch_fields = helper.properties.iter().any(|p| {
            matches!(
                p,
                Property::Name | Property::ParentId | Property::Role | Property::SortOrder
            )
        });
        let account_id = helper.account_id;
        let mail_document_ids = self.get_document_ids(account_id, Collection::Mail)?;

        helper.get(|id, properties| {
            let document_id = id.get_document_id();
            let mut fields = if fetch_fields {
                Some(
                    self.get_orm::<Mailbox>(account_id, document_id)?
                        .ok_or_else(|| {
                            StoreError::InternalError("Mailbox data not found".to_string())
                        })?,
                )
            } else {
                None
            };
            let mut mailbox = HashMap::with_capacity(properties.len());

            for property in properties {
                let value = match property {
                    Property::Id => Value::Id { value: id },
                    Property::Name | Property::Role | Property::SortOrder => fields
                        .as_mut()
                        .unwrap()
                        .remove(property)
                        .unwrap_or_default(),
                    Property::ParentId => fields
                        .as_ref()
                        .unwrap()
                        .get(property)
                        .map(|parent_id| match parent_id {
                            Value::Id { value } if value.get_document_id() > 0 => Value::Id {
                                value: (value.get_document_id() - 1).into(),
                            },
                            _ => Value::Null,
                        })
                        .unwrap_or_default(),
                    Property::TotalEmails => Value::Number {
                        value: self
                            .mailbox_tags(account_id, document_id)?
                            .map(|v| v.len() as u32)
                            .unwrap_or(0),
                    },
                    Property::UnreadEmails => Value::Number {
                        value: self //TODO check unread counts everywhere
                            .mailbox_unread_tags(
                                account_id,
                                document_id,
                                mail_document_ids.as_ref(),
                            )?
                            .map(|v| v.len() as u32)
                            .unwrap_or(0),
                    },
                    Property::TotalThreads => Value::Number {
                        value: self.mailbox_count_threads(
                            account_id,
                            self.mailbox_tags(account_id, document_id)?,
                        )? as u32,
                    },
                    Property::UnreadThreads => Value::Number {
                        value: self.mailbox_count_threads(
                            account_id,
                            self.mailbox_unread_tags(
                                account_id,
                                document_id,
                                mail_document_ids.as_ref(),
                            )?,
                        )? as u32,
                    },
                    Property::MyRights => Value::MailboxRights {
                        value: MailboxRights::default(),
                    },
                    Property::IsSubscribed => Value::Bool { value: true }, //TODO implement
                    _ => Value::Null,
                };

                mailbox.insert(*property, value);
            }
            Ok(Some(Mailbox {
                properties: mailbox,
            }))
        })
    }

    fn mailbox_count_threads(
        &self,
        account_id: AccountId,
        document_ids: Option<RoaringBitmap>,
    ) -> store::Result<usize> {
        if let Some(document_ids) = document_ids {
            let mut thread_ids = HashSet::new();
            self.get_multi_document_value(
                account_id,
                Collection::Mail,
                document_ids.into_iter(),
                MessageField::ThreadId.into(),
            )?
            .into_iter()
            .for_each(|thread_id: Option<DocumentId>| {
                if let Some(thread_id) = thread_id {
                    thread_ids.insert(thread_id);
                }
            });
            Ok(thread_ids.len())
        } else {
            Ok(0)
        }
    }

    fn mailbox_tags(
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

    fn mailbox_unread_tags(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
        mail_document_ids: Option<&RoaringBitmap>,
    ) -> store::Result<Option<RoaringBitmap>> {
        if let Some(mail_document_ids) = mail_document_ids {
            match self.mailbox_tags(account_id, document_id) {
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
