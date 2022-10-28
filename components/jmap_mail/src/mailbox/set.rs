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

use std::time::Duration;

use super::is_valid_role;
use super::schema::{Mailbox, Property, Value};
use crate::mail::schema::Email;
use crate::mail::set::JMAPSetMail;
use crate::mail::sharing::JMAPShareMail;
use crate::mail::{self, MessageField};
use crate::{INBOX_ID, TRASH_ID};
use jmap::error::set::{SetError, SetErrorType};
use jmap::jmap_store::set::{SetHelper, SetObject};
use jmap::jmap_store::Object;
use jmap::orm::acl::ACLUpdate;
use jmap::orm::{serialize::JMAPOrm, TinyORM};
use jmap::principal::store::JMAPPrincipals;
use jmap::request::set::{SetRequest, SetResponse};
use jmap::request::{ACLEnforce, ResultReference};
use jmap::types::jmap::JMAPId;
use jmap::SUPERUSER_ID;
use store::core::acl::ACL;
use store::core::collection::Collection;
use store::core::document::Document;
use store::core::error::StoreError;
use store::core::tag::Tag;
use store::core::JMAPIdPrefix;
use store::read::comparator::Comparator;
use store::read::filter::{ComparisonOperator, Filter, Query};
use store::read::FilterMapper;
use store::tracing::debug;
use store::write::batch::WriteBatch;
use store::write::update::Changes;
use store::{AccountId, DocumentId, JMAPStore, LongInteger, SharedResource};
use store::{SharedBitmap, Store};

#[derive(Debug, Clone, Default)]
pub struct SetArguments {
    pub on_destroy_remove_emails: Option<bool>,
}

impl SetObject for Mailbox {
    type SetArguments = SetArguments;

    type NextCall = ();

    fn eval_id_references(&mut self, mut fnc: impl FnMut(&str) -> Option<JMAPId>) {
        for (_, entry) in self.properties.iter_mut() {
            if let Value::IdReference { value } = entry {
                if let Some(value) = fnc(value) {
                    *entry = Value::Id { value };
                }
            }
        }
    }

    fn eval_result_references(
        &mut self,
        mut fnc: impl FnMut(&ResultReference) -> Option<Vec<u64>>,
    ) {
        for (_, entry) in self.properties.iter_mut() {
            if let Value::ResultReference { value } = entry {
                if let Some(value) = fnc(value).and_then(|mut v| v.pop()) {
                    *entry = Value::Id {
                        value: value.into(),
                    };
                }
            }
        }
    }

    fn set_property(&mut self, property: Self::Property, value: Self::Value) {
        self.properties.set(property, value);
    }
}

pub trait JMAPSetMailbox<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mailbox_set(&self, request: SetRequest<Mailbox>) -> jmap::Result<SetResponse<Mailbox>>;
    fn mailbox_delete(&self, account_id: AccountId, document: &mut Document) -> store::Result<()>;
    fn mailbox_create_path(
        &self,
        account_id: AccountId,
        path: &str,
    ) -> store::Result<Option<(DocumentId, Option<Changes>)>>;
}

impl<T> JMAPSetMailbox<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mailbox_set(&self, request: SetRequest<Mailbox>) -> jmap::Result<SetResponse<Mailbox>> {
        let mut helper = SetHelper::new(self, request)?;
        let on_destroy_remove_emails = helper
            .request
            .arguments
            .on_destroy_remove_emails
            .unwrap_or(false);

        helper.create(|_create_id, mailbox, helper, document| {
            // Set values
            let mut mailbox = TinyORM::<Mailbox>::new().mailbox_set(helper, mailbox, None, None)?;

            // Check ACLs
            if helper.acl.is_shared(helper.account_id) {
                match mailbox.get(&Property::ParentId) {
                    Some(Value::Id { value }) => {
                        if !helper
                            .store
                            .mail_shared_folders(
                                helper.account_id,
                                &helper.acl.member_of,
                                ACL::CreateChild,
                            )?
                            .has_access(value.get_document_id())
                        {
                            return Err(SetError::forbidden().with_description(
                                "You are not allowed to create sub folders under this folder.",
                            ));
                        }
                    }
                    _ => {
                        return Err(SetError::forbidden()
                            .with_description("You are not allowed to create root folders."));
                    }
                }
            }

            // Set parentId if the field is missing
            if !mailbox.has_property(&Property::ParentId) {
                mailbox.set(Property::ParentId, Value::Id { value: 0u64.into() });
            }
            mailbox.insert_validate(document)?;

            Ok(Mailbox::new(document.document_id.into()))
        })?;

        helper.update(|id, mailbox, helper, document| {
            let document_id = id.get_document_id();
            let current_fields = self
                .get_orm::<Mailbox>(helper.account_id, document_id)?
                .ok_or_else(|| SetError::new(SetErrorType::NotFound))?;

            let fields = TinyORM::track_changes(&current_fields).mailbox_set(
                helper,
                mailbox,
                document_id.into(),
                Some(&current_fields),
            )?;

            // Role of internal folders cannot be modified
            if (document_id == INBOX_ID || document_id == TRASH_ID)
                && fields.has_property(&Property::Role)
                && !helper.acl.is_member(SUPERUSER_ID)
            {
                return Err(SetError::forbidden().with_description(
                    "You are not allowed to change the role of Inbox or Trash folders.",
                ));
            }

            // Check ACLs
            if helper.acl.is_shared(helper.account_id) {
                if !helper
                    .store
                    .mail_shared_folders(helper.account_id, &helper.acl.member_of, ACL::Modify)?
                    .has_access(document_id)
                {
                    return Err(SetError::forbidden()
                        .with_description("You are not allowed to modify this folder."));
                }

                if fields.has_property(&Property::ACL)
                    && !helper
                        .store
                        .mail_shared_folders(
                            helper.account_id,
                            &helper.acl.member_of,
                            ACL::Administer,
                        )?
                        .has_access(document_id)
                {
                    return Err(SetError::forbidden().with_description(
                        "You are not allowed to change the permissions of this folder.",
                    ));
                }
            }

            // Merge changes
            current_fields.merge_validate(document, fields)?;

            Ok(None)
        })?;

        helper.destroy(|id, helper, document| {
            let document_id = id.get_document_id();

            // Internal folders cannot be deleted
            if (document_id == INBOX_ID || document_id == TRASH_ID)
                && !helper.acl.is_member(SUPERUSER_ID)
            {
                return Err(SetError::forbidden()
                    .with_description("You are not allowed to delete Inbox or Trash folders."));
            }

            // Check ACLs
            if helper.acl.is_shared(helper.account_id) {
                if !helper
                    .store
                    .mail_shared_folders(helper.account_id, &helper.acl.member_of, ACL::Delete)?
                    .has_access(document_id)
                {
                    return Err(SetError::forbidden()
                        .with_description("You are not allowed to delete this folder."));
                }
                if on_destroy_remove_emails
                    && !helper
                        .store
                        .mail_shared_folders(
                            helper.account_id,
                            &helper.acl.member_of,
                            ACL::RemoveItems,
                        )?
                        .has_access(document_id)
                {
                    return Err(SetError::forbidden().with_description(
                        "You are not allowed to delete emails from this folder.",
                    ));
                }
            }

            // Verify that this mailbox does not have sub-mailboxes
            if !self
                .query_store::<FilterMapper>(
                    helper.account_id,
                    Collection::Mailbox,
                    Filter::new_condition(
                        Property::ParentId.into(),
                        ComparisonOperator::Equal,
                        Query::LongInteger((document_id + 1) as LongInteger),
                    ),
                    Comparator::None,
                )?
                .is_empty()
            {
                return Err(SetError::new(SetErrorType::MailboxHasChild)
                    .with_description("Mailbox has at least one children."));
            }

            // Verify that the mailbox is empty
            if let Some(message_doc_ids) = self.get_tag(
                helper.account_id,
                Collection::Mail,
                MessageField::Mailbox.into(),
                Tag::Id(document_id),
            )? {
                if on_destroy_remove_emails {
                    // Try locking the collection before deleting the messages
                    let _lock = match self.try_lock_collection(
                        helper.account_id,
                        Collection::Mail,
                        Duration::from_secs(1),
                    ) {
                        Some(lock) => lock,
                        None => {
                            return Err(SetError::new(SetErrorType::RateLimit).with_description(
                                "Resource busy, please try again in a few momentss.",
                            ));
                        }
                    };

                    for message_document_id in message_doc_ids {
                        let mut document = Document::new(Collection::Mail, message_document_id);
                        // Fetch Email's ORM
                        let current_fields = if let Some(current_fields) =
                            self.get_orm::<Email>(helper.account_id, message_document_id)?
                        {
                            current_fields
                        } else {
                            debug!(
                                "Email ORM for {}:{} not found",
                                helper.account_id, message_document_id
                            );
                            continue;
                        };

                        // If the message is in multiple mailboxes, untag it from the current mailbox,
                        // otherwise delete it.
                        match current_fields.get_tags(&mail::schema::Property::MailboxIds) {
                            Some(tags) if tags.len() > 1 => {
                                let thread_id = self
                                    .get_document_value::<DocumentId>(
                                        helper.account_id,
                                        Collection::Mail,
                                        message_document_id,
                                        MessageField::ThreadId.into(),
                                    )?
                                    .ok_or_else(|| {
                                        StoreError::DataCorruption(format!(
                                            "Failed to fetch threadId for {}:{}.",
                                            helper.account_id, message_document_id
                                        ))
                                    })?;
                                let mut fields = TinyORM::track_changes(&current_fields);
                                fields.untag(
                                    &mail::schema::Property::MailboxIds,
                                    &Tag::Id(document_id),
                                );
                                current_fields.merge(&mut document, fields)?;
                                helper.changes.update_document(document);
                                helper.changes.log_update(
                                    Collection::Mail,
                                    JMAPId::from_parts(thread_id, message_document_id),
                                )
                            }
                            _ => {
                                if let Some(id) = self.mail_delete(
                                    helper.account_id,
                                    Some(&mut helper.changes),
                                    &mut document,
                                )? {
                                    helper.changes.delete_document(document);
                                    helper.changes.log_delete(Collection::Mail, id);
                                }
                            }
                        }
                    }
                } else {
                    return Err(SetError::new(SetErrorType::MailboxHasEmail)
                        .with_description("Mailbox is not empty."));
                }
            }

            // Delete ORM and index
            if let Some(orm) = helper
                .store
                .get_orm::<Mailbox>(helper.account_id, document_id)?
            {
                orm.delete(document);
            }

            Ok(())
        })?;

        helper.into_response()
    }

    fn mailbox_delete(&self, account_id: AccountId, document: &mut Document) -> store::Result<()> {
        // Delete ORM
        self.get_orm::<Mailbox>(account_id, document.document_id)?
            .ok_or_else(|| {
                StoreError::NotFound(format!(
                    "Failed to fetch Mailbox ORM for {}:{}.",
                    account_id, document.document_id
                ))
            })?
            .delete(document);

        Ok(())
    }

    fn mailbox_create_path(
        &self,
        account_id: AccountId,
        path: &str,
    ) -> store::Result<Option<(DocumentId, Option<Changes>)>> {
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

        // Lock collection
        let _lock = self
            .try_lock_collection(account_id, Collection::Mailbox, Duration::from_millis(200))
            .ok_or_else(|| StoreError::InternalError("Failed to obtain lock".to_string()))?;

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
        let mut path = path.into_iter().peekable();
        'outer: while let Some(name) = path.peek() {
            for (part, parent_id, document_id) in &found_names {
                if part.eq(name) && *parent_id == next_parent_id {
                    next_parent_id = *document_id;
                    path.next();
                    continue 'outer;
                }
            }
            break;
        }

        // Create missing folders
        if path.peek().is_some() {
            let mut batch = WriteBatch::new(account_id);

            for name in path {
                if name.len() > self.config.mailbox_name_max_len {
                    return Ok(None);
                }

                let document_id = self.assign_document_id(account_id, Collection::Mailbox)?;
                let mut document = Document::new(Collection::Mailbox, document_id);
                let mut orm = TinyORM::<Mailbox>::new();
                orm.set(
                    Property::Name,
                    Value::Text {
                        value: name.to_string(),
                    },
                );
                orm.set(
                    Property::ParentId,
                    Value::Id {
                        value: next_parent_id.into(),
                    },
                );
                next_parent_id = document_id + 1;
                orm.insert(&mut document)?;
                batch.insert_document(document);
                batch.log_insert(Collection::Mailbox, document_id);
            }

            Ok(Some((next_parent_id - 1, self.write(batch)?)))
        } else {
            Ok(Some((next_parent_id - 1, None)))
        }
    }
}

trait MailboxSet<T>: Sized
where
    T: for<'x> Store<'x> + 'static,
{
    fn mailbox_set(
        self,
        helper: &mut SetHelper<Mailbox, T>,
        mailbox: Mailbox,
        mailbox_id: Option<DocumentId>,
        fields: Option<&TinyORM<Mailbox>>,
    ) -> jmap::error::set::Result<Self, Property>;
}

impl<T> MailboxSet<T> for TinyORM<Mailbox>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mailbox_set(
        mut self,
        helper: &mut SetHelper<Mailbox, T>,
        mailbox: Mailbox,
        mailbox_id: Option<DocumentId>,
        current_fields: Option<&TinyORM<Mailbox>>,
    ) -> jmap::error::set::Result<Self, Property> {
        // Set properties
        for (property, value) in mailbox.properties {
            let value = match (property, value) {
                (Property::Name, Value::Text { value }) => {
                    if value.len() < helper.store.config.mailbox_name_max_len {
                        Value::Text { value }
                    } else {
                        return Err(SetError::invalid_properties()
                            .with_property(property)
                            .with_description("Mailbox name is too long.".to_string()));
                    }
                }
                (Property::ParentId, Value::Id { value }) => {
                    let parent_id = value.get_document_id();
                    if helper.will_destroy.contains(&value) {
                        return Err(SetError::new(SetErrorType::WillDestroy)
                            .with_description("Parent ID will be destroyed."));
                    } else if !helper.document_ids.contains(parent_id) {
                        return Err(SetError::invalid_properties()
                            .with_description("Parent ID does not exist."));
                    }

                    Value::Id {
                        value: (parent_id + 1).into(),
                    }
                }
                (Property::ParentId, Value::IdReference { value }) => Value::Id {
                    value: (u64::from(helper.get_id_reference(Property::ParentId, &value)?) + 1)
                        .into(),
                },
                (Property::IsSubscribed, Value::Bool { value: subscribe }) => {
                    let account_id = helper.acl.primary_id();
                    let mut new_value = None;
                    if let Some(current_fields) = current_fields.as_ref() {
                        if let Some(Value::Subscriptions { value }) =
                            current_fields.get(&Property::IsSubscribed)
                        {
                            if subscribe {
                                if !value.contains(&account_id) {
                                    let mut current_subscriptions = value.clone();
                                    current_subscriptions.push(account_id);
                                    new_value = Value::Subscriptions {
                                        value: current_subscriptions,
                                    }
                                    .into();
                                } else {
                                    continue;
                                }
                            } else if value.contains(&account_id) {
                                if value.len() > 1 {
                                    new_value = Value::Subscriptions {
                                        value: value
                                            .iter()
                                            .filter(|&&id| id != account_id)
                                            .cloned()
                                            .collect(),
                                    }
                                    .into();
                                } else {
                                    new_value = Value::Null.into();
                                }
                            } else {
                                continue;
                            }
                        }
                    }
                    if let Some(new_value) = new_value {
                        new_value
                    } else if subscribe {
                        Value::Subscriptions {
                            value: vec![account_id],
                        }
                    } else {
                        continue;
                    }
                }
                (Property::ParentId, Value::Null) => Value::Id { value: 0u64.into() },
                (Property::Role, Value::Text { value }) => {
                    let role = value.to_lowercase();
                    if is_valid_role(&role) {
                        self.tag(property, Tag::Default);
                        Value::Text { value: role }
                    } else {
                        return Err(SetError::invalid_properties()
                            .with_property(property)
                            .with_description("Invalid role."));
                    }
                }
                (Property::Role, Value::Null) => {
                    self.untag(&property, &Tag::Default);
                    Value::Null
                }
                (Property::SortOrder, value @ Value::Number { .. }) => value,
                (Property::ACL, Value::ACLSet(value)) => {
                    for acl_update in &value {
                        match acl_update {
                            ACLUpdate::Replace { acls } => {
                                self.acl_clear();
                                for (account_id, acls) in acls {
                                    self.acl_update(
                                        helper.store.principal_to_id(account_id)?,
                                        acls,
                                    );
                                }
                            }
                            ACLUpdate::Update { account_id, acls } => {
                                self.acl_update(helper.store.principal_to_id(account_id)?, acls);
                            }
                            ACLUpdate::Set {
                                account_id,
                                acl,
                                is_set,
                            } => {
                                self.acl_set(
                                    helper.store.principal_to_id(account_id)?,
                                    *acl,
                                    *is_set,
                                );
                            }
                        }
                    }
                    self.acl_finish();
                    continue;
                }
                (_, _) => {
                    return Err(SetError::invalid_properties()
                        .with_property(property)
                        .with_description("Unexpected value."));
                }
            };

            self.set(property, value);
        }

        if let (Some(mailbox_id), Some(mut mailbox_parent_id)) = (
            mailbox_id,
            self.get(&Property::ParentId).and_then(|v| v.as_id()),
        ) {
            // Validate circular parent-child relationship
            let mut success = false;
            for _ in 0..helper.store.config.mailbox_max_depth {
                if mailbox_parent_id == (mailbox_id as store::JMAPId) + 1 {
                    return Err(SetError::invalid_properties()
                        .with_description("Mailbox cannot be a parent of itself."));
                } else if mailbox_parent_id == 0 {
                    success = true;
                    break;
                }
                let parent_document_id = (mailbox_parent_id - 1).get_document_id();

                if let Some(fields) = helper
                    .store
                    .get_orm::<Mailbox>(helper.account_id, parent_document_id)?
                {
                    mailbox_parent_id = fields
                        .get(&Property::ParentId)
                        .and_then(|v| v.as_id())
                        .unwrap_or(0);
                } else if helper.document_ids.contains(parent_document_id) {
                    // Parent mailbox is probably created within the same request
                    success = true;
                    break;
                } else {
                    return Err(SetError::invalid_properties()
                        .with_description("Mailbox parent does not exist."));
                }
            }

            if !success {
                return Err(SetError::invalid_properties()
                    .with_description("Mailbox parent-child relationship is too deep."));
            }
        }

        // Verify that the mailbox role is unique.
        if let Some(Value::Text {
            value: mailbox_role,
        }) = self.get(&Property::Role)
        {
            if !helper
                .store
                .query_store::<FilterMapper>(
                    helper.account_id,
                    Collection::Mailbox,
                    Filter::new_condition(
                        Property::Role.into(),
                        ComparisonOperator::Equal,
                        Query::Keyword(mailbox_role.into()),
                    ),
                    Comparator::None,
                )?
                .is_empty()
            {
                return Err(SetError::invalid_properties().with_description(format!(
                    "A mailbox with role '{}' already exists.",
                    mailbox_role
                )));
            }
        }

        // Verify that the mailbox name is unique.
        if let Some(Value::Text {
            value: mailbox_name,
        }) = self.get(&Property::Name)
        {
            // Obtain parent mailbox id
            if let Some(parent_mailbox_id) = if let Some(mailbox_parent_id) =
                &self.get(&Property::ParentId).and_then(|id| id.as_id())
            {
                (*mailbox_parent_id).into()
            } else if let Some(current_fields) = current_fields {
                if current_fields
                    .get(&Property::Name)
                    .and_then(|n| n.as_text())
                    != Some(mailbox_name)
                {
                    current_fields
                        .get(&Property::ParentId)
                        .and_then(|id| id.as_id())
                        .unwrap_or_default()
                        .into()
                } else {
                    None
                }
            } else {
                0.into()
            } {
                if !helper
                    .store
                    .query_store::<FilterMapper>(
                        helper.account_id,
                        Collection::Mailbox,
                        Filter::and(vec![
                            Filter::new_condition(
                                Property::ParentId.into(),
                                ComparisonOperator::Equal,
                                Query::LongInteger(parent_mailbox_id),
                            ),
                            Filter::new_condition(
                                Property::Name.into(),
                                ComparisonOperator::Equal,
                                Query::Index(mailbox_name.to_string()),
                            ),
                        ]),
                        Comparator::None,
                    )?
                    .into_bitmap()
                    .is_empty()
                {
                    return Err(SetError::invalid_properties().with_description(format!(
                        "A mailbox with name '{}' already exists.",
                        mailbox_name
                    )));
                }

                /*for jmap_id in helper.store.query_store::<FilterMapper>(
                    helper.account_id,
                    Collection::Mailbox,
                    Filter::new_condition(
                        Property::ParentId.into(),
                        ComparisonOperator::Equal,
                        Query::LongInteger(parent_mailbox_id),
                    ),
                    Comparator::None,
                )? {
                    if helper
                        .store
                        .get_orm::<Mailbox>(helper.account_id, jmap_id.get_document_id())?
                        .unwrap_or_default()
                        .get(&Property::Name)
                        .and_then(|n| n.as_text())
                        == Some(mailbox_name)
                    {
                        return Err(SetError::invalid_properties().with_description(format!(
                            "A mailbox with name '{}' already exists.",
                            mailbox_name
                        )));
                    }
                }*/
            }
        }

        // Invalidate cache for changed ACLs
        if let Some(permissions) = self.get_changed_acls(current_fields) {
            for permission in permissions {
                helper.store.acl_tokens.invalidate(&permission.id);
                for acl in permission.acl {
                    for collection in [Collection::Mail, Collection::Mailbox] {
                        let key =
                            SharedResource::new(helper.account_id, permission.id, collection, acl);
                        helper.store.shared_documents.invalidate(&key);
                    }
                }
            }
        }

        Ok(self)
    }
}
