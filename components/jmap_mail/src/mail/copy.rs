use super::{
    import::JMAPMailImport,
    schema::{Email, Property, Value},
    MessageData, MessageField,
};
use jmap::{
    error::set::{SetError, SetErrorType},
    jmap_store::copy::CopyHelper,
    orm::TinyORM,
    request::{
        copy::{CopyRequest, CopyResponse},
        set::SetRequest,
        MaybeResultReference,
    },
    types::{blob::JMAPBlob, jmap::JMAPId},
};
use store::serialize::leb128::Leb128;
use store::{
    blob::BlobId,
    core::{collection::Collection, error::StoreError, tag::Tag},
    serialize::{StoreDeserialize, StoreSerialize},
    write::options::IndexOptions,
    JMAPStore, Store,
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

        //TODO validate ACLs

        helper.create(|copy_id, item, helper, document| {
            // Update properties
            let mut fields = TinyORM::<Email>::new();
            let mut received_at = None;
            for (property, value) in item.properties {
                match (property, value) {
                    (Property::MailboxIds, Value::MailboxIds { value, set }) => {
                        if set {
                            for (mailbox_id, set) in value {
                                let mailbox_id = mailbox_id.unwrap_value().ok_or_else(|| {
                                    SetError::new(
                                        SetErrorType::InvalidProperties,
                                        "Invalid reference used on mailboxIds.",
                                    )
                                })?;

                                if mailbox_ids.contains(mailbox_id.into()) {
                                    if set {
                                        fields
                                            .tag(Property::MailboxIds, Tag::Id(mailbox_id.into()));
                                    }
                                } else {
                                    return Err(SetError::invalid_property(
                                        Property::MailboxIds,
                                        format!("mailboxId {} does not exist.", mailbox_id),
                                    ));
                                }
                            }
                        } else {
                            for (mailbox_id, set) in value {
                                let mailbox_id = mailbox_id.unwrap_value().ok_or_else(|| {
                                    SetError::new(
                                        SetErrorType::InvalidProperties,
                                        "Invalid reference used on mailboxIds.",
                                    )
                                })?;

                                if mailbox_ids.contains(mailbox_id.into()) {
                                    if set {
                                        fields
                                            .tag(Property::MailboxIds, Tag::Id(mailbox_id.into()));
                                    }
                                } else {
                                    return Err(SetError::invalid_property(
                                        Property::MailboxIds,
                                        format!("mailboxId {} does not exist.", mailbox_id),
                                    ));
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
                return Err(SetError::new(
                    SetErrorType::InvalidProperties,
                    "Message has to belong to at least one mailbox.",
                ));
            }

            // Fetch metadata
            let document_id = copy_id.get_document_id();
            let mut metadata_blob_id = self
                .get_document_value::<BlobId>(
                    helper.from_account_id,
                    Collection::Mail,
                    document_id,
                    MessageField::Metadata.into(),
                )?
                .ok_or(StoreError::DataCorruption)?;
            let metadata_bytes = helper.store.blob_get(&metadata_blob_id)?.ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Could not find message metadata blob for {}.",
                    document.document_id
                ))
            })?;
            let (message_data_len, read_bytes) =
                usize::from_leb128_bytes(&metadata_bytes[..]).ok_or(StoreError::DataCorruption)?;
            let mut message_data = MessageData::deserialize(
                &metadata_bytes[read_bytes..read_bytes + message_data_len],
            )
            .ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to get deserialize message data for {}.",
                    document.document_id
                ))
            })?;

            // Set receivedAt
            if let Some(received_at) = received_at {
                message_data.received_at = received_at;

                // Serialize message data and outline
                let mut message_data_bytes = message_data.serialize().ok_or_else(|| {
                    StoreError::SerializeError("Failed to serialize message data".into())
                })?;
                let mut metadata = Vec::with_capacity(
                    message_data_bytes.len()
                        + (metadata_bytes.len() - (read_bytes + message_data_len))
                        + std::mem::size_of::<usize>(),
                );
                message_data_bytes.len().to_leb128_bytes(&mut metadata);
                metadata.append(&mut message_data_bytes);
                metadata.extend_from_slice(&metadata_bytes[read_bytes + message_data_len..]);

                // Link blob and set message data field
                metadata_blob_id = self.blob_store(&metadata)?;
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
            let lock = self.lock_account(helper.account_id, Collection::Mail);

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
