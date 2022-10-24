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

use super::{
    import::JMAPMailImport,
    schema::{Email, Property, Value},
    sharing::JMAPShareMail,
    MessageData, MessageField,
};
use jmap::{
    error::set::SetError,
    jmap_store::copy::CopyHelper,
    orm::TinyORM,
    request::{
        copy::{CopyRequest, CopyResponse},
        set::SetRequest,
        ACLEnforce, MaybeResultReference,
    },
    types::{blob::JMAPBlob, jmap::JMAPId},
};
use store::core::acl::ACL;
use store::{
    blob::BlobId,
    core::{collection::Collection, error::StoreError, tag::Tag},
    serialize::{StoreDeserialize, StoreSerialize},
    write::options::IndexOptions,
    JMAPStore, SharedBitmap, Store,
};

pub trait JMAPCopyMail<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_copy(&self, request: CopyRequest<Email>) -> jmap::Result<CopyResponse<Email>>;
}

impl<T> JMAPCopyMail<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_copy(&self, request: CopyRequest<Email>) -> jmap::Result<CopyResponse<Email>> {
        let mut helper = CopyHelper::new(self, request)?;
        let mailbox_ids = self
            .get_document_ids(helper.account_id, Collection::Mailbox)?
            .unwrap_or_default();
        let on_success_delete = helper
            .request
            .on_success_destroy_original
            .as_ref()
            .copied()
            .unwrap_or(false);
        let destroy_from_if_in_state = helper.request.destroy_from_if_in_state.take();
        let mut destroy_ids = Vec::new();

        let is_shared_source = helper.acl.is_shared(helper.from_account_id);
        let is_shared_target = helper.acl.is_shared(helper.account_id);

        helper.create(|copy_id, item, helper, document| {
            // Check ACL on source account
            let document_id = copy_id.get_document_id();
            if is_shared_source
                && !helper
                    .store
                    .mail_shared_messages(
                        helper.from_account_id,
                        &helper.acl.member_of,
                        ACL::ReadItems,
                    )?
                    .has_access(document_id)
            {
                return Err(SetError::forbidden()
                    .with_description("You do not have access to this message."));
            }

            // Update properties
            let mut fields = TinyORM::<Email>::new();
            let mut received_at = None;
            for (property, value) in item.properties {
                match (property, value) {
                    (Property::MailboxIds, Value::MailboxIds { value, set }) => {
                        if set {
                            for (mailbox_id, set) in value {
                                let mailbox_id = mailbox_id.unwrap_value().ok_or_else(|| {
                                    SetError::invalid_properties()
                                        .with_property(Property::MailboxIds)
                                        .with_description("Invalid reference used on mailboxIds.")
                                })?;

                                if mailbox_ids.contains(mailbox_id.into()) {
                                    if set {
                                        fields
                                            .tag(Property::MailboxIds, Tag::Id(mailbox_id.into()));
                                    }
                                } else {
                                    return Err(SetError::invalid_properties()
                                        .with_property(Property::MailboxIds)
                                        .with_description(format!(
                                            "mailboxId {} does not exist.",
                                            mailbox_id
                                        )));
                                }
                            }
                        } else {
                            for (mailbox_id, set) in value {
                                let mailbox_id = mailbox_id.unwrap_value().ok_or_else(|| {
                                    SetError::invalid_properties()
                                        .with_property(Property::MailboxIds)
                                        .with_description("Invalid reference used on mailboxIds.")
                                })?;

                                if mailbox_ids.contains(mailbox_id.into()) {
                                    if set {
                                        fields
                                            .tag(Property::MailboxIds, Tag::Id(mailbox_id.into()));
                                    }
                                } else {
                                    return Err(SetError::invalid_properties()
                                        .with_property(Property::MailboxIds)
                                        .with_description(format!(
                                            "mailboxId {} does not exist.",
                                            mailbox_id
                                        )));
                                }
                            }
                        }
                    }
                    (Property::Keywords, Value::Keywords { value, set }) => {
                        if set {
                            fields.untag_all(&Property::Keywords);

                            for (keyword, set) in value {
                                if set {
                                    fields.tag(Property::Keywords, keyword.tag);
                                }
                            }
                        } else {
                            for (keyword, set) in value {
                                if set {
                                    fields.tag(Property::Keywords, keyword.tag);
                                }
                            }
                        }
                    }
                    (Property::ReceivedAt, Value::Date { value }) => {
                        received_at = value.timestamp().into();
                    }
                    _ => (),
                }
            }

            // Make sure the message is at least in one mailbox
            if !fields.has_tags(&Property::MailboxIds) {
                return Err(SetError::invalid_properties()
                    .with_property(Property::MailboxIds)
                    .with_description("Message has to belong to at least one mailbox."));
            }

            // Check ACL on target account
            if is_shared_target {
                let allowed_folders = helper.store.mail_shared_folders(
                    helper.account_id,
                    &helper.acl.member_of,
                    ACL::AddItems,
                )?;

                for mailbox in fields.get_tags(&Property::MailboxIds).unwrap() {
                    let mailbox_id = mailbox.as_id();
                    if !allowed_folders.has_access(mailbox_id) {
                        return Err(SetError::forbidden().with_description(format!(
                            "You are not allowed to add messages to folder {}.",
                            JMAPId::from(mailbox_id)
                        )));
                    }
                }
            }

            // Fetch metadata
            let mut metadata_blob_id = self
                .get_document_value::<BlobId>(
                    helper.from_account_id,
                    Collection::Mail,
                    document_id,
                    MessageField::Metadata.into(),
                )?
                .ok_or_else(|| {
                    StoreError::NotFound(format!(
                        "Message data for {}:{} not found.",
                        helper.account_id, document_id
                    ))
                })?;
            let mut message_data = MessageData::deserialize(
                &helper.store.blob_get(&metadata_blob_id)?.ok_or_else(|| {
                    StoreError::NotFound(format!(
                        "Could not find message metadata blob for {}.",
                        document.document_id
                    ))
                })?,
            )
            .ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to get deserialize message data for {}.",
                    document.document_id
                ))
            })?;

            // Set receivedAt
            if let Some(received_at) = received_at {
                // Serialize message data and outline
                message_data.received_at = received_at;

                // Link blob and set message data field
                let metadata_bytes = message_data.serialize().ok_or_else(|| {
                    StoreError::SerializeError("Failed to serialize message data".into())
                })?;
                metadata_blob_id = BlobId::new_local(&metadata_bytes);

                self.blob_store(&metadata_blob_id, metadata_bytes)?;
            }

            // Copy properties and build index
            let raw_blob = JMAPBlob::from(&message_data.raw_message);
            let size = message_data.size;
            message_data.build_index(document, true)?;

            // Link metadata blob
            document.binary(
                MessageField::Metadata,
                metadata_blob_id.serialize().unwrap(),
                IndexOptions::new(),
            );
            document.blob(metadata_blob_id, IndexOptions::new());

            // Add fields
            fields.insert(document)?;

            // Lock collection
            let lock = self.lock_collection(helper.account_id, Collection::Mail);

            // Obtain thread Id
            let thread_id = self.mail_set_thread(&mut helper.changes, document)?;

            // Build email result
            let mut email = Email::default();
            email.insert(
                Property::Id,
                JMAPId::from_parts(thread_id, document.document_id),
            );
            email.insert(Property::BlobId, raw_blob);
            email.insert(Property::ThreadId, JMAPId::from(thread_id));
            email.insert(Property::Size, size);

            // Add to destroy list
            if on_success_delete {
                destroy_ids.push(*copy_id);
            }

            Ok((email, lock.into()))
        })?;

        let acl = helper.acl.clone();
        helper.into_response().map(|mut r| {
            if on_success_delete && !destroy_ids.is_empty() {
                r.next_call = SetRequest {
                    acl: acl.into(),
                    account_id: r.from_account_id,
                    if_in_state: destroy_from_if_in_state,
                    create: None,
                    update: None,
                    destroy: Some(MaybeResultReference::Value(destroy_ids)),
                    arguments: (),
                }
                .into()
            }
            r
        })
    }
}
