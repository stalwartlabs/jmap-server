/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use super::schema::{Mailbox, MailboxRights, Property, Value};
use crate::mail::schema::Keyword;
use crate::mail::sharing::JMAPShareMail;
use crate::mail::MessageField;
use jmap::jmap_store::get::{default_mapper, GetHelper, GetObject};
use jmap::orm::serialize::JMAPOrm;
use jmap::principal::store::JMAPPrincipals;
use jmap::request::get::{GetRequest, GetResponse};
use jmap::request::ACLEnforce;
use jmap::types::jmap::JMAPId;
use store::ahash::AHashSet;
use store::core::acl::ACL;
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::core::tag::Tag;
use store::core::vec_map::VecMap;
use store::read::comparator::Comparator;
use store::read::filter::{ComparisonOperator, Filter, Query};
use store::read::FilterMapper;
use store::roaring::RoaringBitmap;
use store::{AccountId, JMAPStore, SharedBitmap};
use store::{DocumentId, Store};

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

    fn get_as_id(&self, property: &Self::Property) -> Option<Vec<JMAPId>> {
        match self.properties.get(property)? {
            Value::Id { value } => Some(vec![*value]),
            _ => None,
        }
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
    fn mailbox_get_by_name(
        &self,
        account_id: AccountId,
        name: &str,
    ) -> store::Result<Option<DocumentId>>;
    fn mailbox_get_by_role(
        &self,
        account_id: AccountId,
        role: &str,
    ) -> store::Result<Option<DocumentId>>;
}

impl<T> JMAPGetMailbox<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mailbox_get(&self, request: GetRequest<Mailbox>) -> jmap::Result<GetResponse<Mailbox>> {
        let mut helper = GetHelper::new(
            self,
            request,
            default_mapper.into(),
            (|account_id: AccountId, member_of: &[AccountId]| {
                self.mail_shared_folders(account_id, member_of, ACL::ReadItems)
            })
            .into(),
        )?;
        let fetch_fields = helper.properties.iter().any(|p| {
            matches!(
                p,
                Property::Name
                    | Property::ParentId
                    | Property::Role
                    | Property::SortOrder
                    | Property::ACL
            )
        });
        let account_id = helper.account_id;
        let acl = helper.acl.clone();
        let mail_document_ids = self.get_document_ids(account_id, Collection::Mail)?;

        // Add Id Property
        if !helper.properties.contains(&Property::Id) {
            helper.properties.push(Property::Id);
        }

        helper.get(|id, properties| {
            let document_id = id.get_document_id();
            let mut fields = if fetch_fields {
                Some(
                    self.get_orm::<Mailbox>(account_id, document_id)?
                        .ok_or_else(|| {
                            StoreError::NotFound("Mailbox data not found".to_string())
                        })?,
                )
            } else {
                None
            };
            let mut mailbox = VecMap::with_capacity(properties.len());

            for property in properties {
                let value = match property {
                    Property::Id => Value::Id { value: id },
                    Property::Name | Property::Role => fields
                        .as_mut()
                        .unwrap()
                        .remove(property)
                        .unwrap_or_default(),
                    Property::SortOrder => fields
                        .as_mut()
                        .unwrap()
                        .remove(property)
                        .unwrap_or(Value::Number { value: 0 }),
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
                        value: self
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
                        value: if acl.is_shared(account_id) {
                            MailboxRights::shared(self.get_acl(
                                &acl.member_of,
                                account_id,
                                Collection::Mailbox,
                                document_id,
                            )?)
                        } else {
                            MailboxRights::owner()
                        },
                    },
                    Property::IsSubscribed => fields
                        .as_ref()
                        .unwrap()
                        .get(property)
                        .map(|parent_id| match parent_id {
                            Value::Subscriptions { value } if value.contains(&acl.primary_id()) => {
                                Value::Bool { value: true }
                            }
                            _ => Value::Bool { value: false },
                        })
                        .unwrap_or(Value::Bool { value: false }),
                    Property::ACL
                        if acl.is_member(account_id)
                            || self
                                .mail_shared_folders(account_id, &acl.member_of, ACL::Administer)?
                                .has_access(document_id) =>
                    {
                        let mut acl_get = VecMap::new();
                        for (account_id, acls) in fields.as_ref().unwrap().get_acls() {
                            if let Some(email) = self.principal_to_email(account_id)? {
                                acl_get.append(email, acls);
                            }
                        }
                        Value::ACLGet(acl_get)
                    }
                    _ => Value::Null,
                };

                mailbox.append(*property, value);
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
            let mut thread_ids = AHashSet::default();
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
                        Ok(None) => Ok(mailbox.into()),
                        Err(e) => Err(e),
                    }
                }
                other => other,
            }
        } else {
            Ok(None)
        }
    }

    fn mailbox_get_by_name(
        &self,
        account_id: AccountId,
        path: &str,
    ) -> store::Result<Option<DocumentId>> {
        let path = path
            .split('/')
            .filter_map(|p| {
                let p = p.trim();
                if !p.is_empty() {
                    p.into()
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if path.is_empty() || path.len() > self.config.mailbox_max_depth {
            return Ok(None);
        }
        let document_ids = self
            .query_store::<FilterMapper>(
                account_id,
                Collection::Mailbox,
                Filter::or(
                    path.iter()
                        .map(|n| {
                            Filter::new_condition(
                                Property::Name.into(),
                                ComparisonOperator::Equal,
                                Query::Index(n.to_string()),
                            )
                        })
                        .collect(),
                ),
                Comparator::None,
            )?
            .into_bitmap();

        if (document_ids.len() as usize) < path.len() {
            return Ok(None);
        }

        let mut found_names = Vec::new();
        for document_id in document_ids {
            if let Some(mut orm) = self.get_orm::<Mailbox>(account_id, document_id)? {
                if let Some(Value::Text { value }) = orm.remove(&Property::Name) {
                    found_names.push((
                        value,
                        if let Some(Value::Id { value }) = orm.remove(&Property::ParentId) {
                            value.get_document_id()
                        } else {
                            0
                        },
                        document_id + 1,
                    ));
                } else {
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        }

        let mut next_parent_id = 0;
        'outer: for name in path {
            for (part, parent_id, document_id) in &found_names {
                if part.eq(name) && *parent_id == next_parent_id {
                    next_parent_id = *document_id;
                    continue 'outer;
                }
            }
            return Ok(None);
        }

        Ok(Some(next_parent_id - 1))
    }

    fn mailbox_get_by_role(
        &self,
        account_id: AccountId,
        role: &str,
    ) -> store::Result<Option<DocumentId>> {
        self.query_store::<FilterMapper>(
            account_id,
            Collection::Mailbox,
            Filter::new_condition(
                Property::Role.into(),
                ComparisonOperator::Equal,
                Query::Keyword(role.to_string()),
            ),
            Comparator::None,
        )
        .map(|r| r.into_bitmap().min())
    }
}
