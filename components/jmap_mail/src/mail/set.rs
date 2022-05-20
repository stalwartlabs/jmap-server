use crate::mail::import::JMAPMailImport;
use jmap::error::set::{SetError, SetErrorType};
use jmap::id::blob::JMAPBlob;
use jmap::id::jmap::JMAPId;
use jmap::jmap_store::blob::JMAPBlobStore;
use jmap::jmap_store::orm::{JMAPOrm, TinyORM};
use jmap::jmap_store::set::{SetHelper, SetObject};

use jmap::request::set::{SetRequest, SetResponse};
use mail_builder::headers::address::Address;
use mail_builder::headers::content_type::ContentType;
use mail_builder::headers::date::Date;
use mail_builder::headers::message_id::MessageId;
use mail_builder::headers::raw::Raw;
use mail_builder::headers::text::Text;
use mail_builder::headers::url::URL;
use mail_builder::mime::{BodyPart, MimePart};
use mail_builder::MessageBuilder;
use mail_parser::RfcHeader;
use std::collections::{BTreeMap, HashMap, HashSet};
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::core::tag::Tag;
use store::write::options::{IndexOptions, Options};

use store::blob::BlobId;
use store::{AccountId, DocumentId, JMAPStore, Store};

use super::import::get_message_part;
use super::schema::{Email, EmailBodyPart, EmailBodyValue, HeaderValue, Keyword, Property};
use super::{MessageData, MessageField};

impl SetObject for Email {
    type SetArguments = ();

    type NextInvocation = ();

    fn map_references(&self, fnc: impl FnMut(&str) -> Option<jmap::id::jmap::JMAPId>) {
        todo!()
    }
}

pub trait JMAPSetMail<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_set(&self, request: SetRequest<Email>) -> jmap::Result<SetResponse<Email>>;
}

impl<T> JMAPSetMail<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_set(&self, request: SetRequest<Email>) -> jmap::Result<SetResponse<Email>> {
        let mut helper = SetHelper::new(self, request)?;
        let mailbox_ids = self
            .get_document_ids(helper.account_id, Collection::Mailbox)?
            .unwrap_or_default();
        let account_id = helper.account_id;

        helper.create(|_create_id, item, batch, document| {
            let mut builder = MessageBuilder::new();
            let mut fields = TinyORM::<Email>::new();

            // Set mailboxes
            if let Some(mailboxes) = item.mailbox_ids {
                fields.untag_all(&Property::MailboxIds);

                for (mailbox_id, set) in mailboxes {
                    if mailbox_ids.contains(mailbox_id.into()) {
                        if set {
                            fields.tag(Property::MailboxIds, Tag::Id(mailbox_id.into()));
                        }
                    } else {
                        return Err(SetError::invalid_property(
                            Property::MailboxIds,
                            format!("mailboxId {} does not exist.", mailbox_id),
                        ));
                    }
                }
            } else if !item.params.patch_mailbox.is_empty() {
                for (mailbox_id, set) in item.params.patch_mailbox {
                    if mailbox_ids.contains(mailbox_id.into()) {
                        if set {
                            fields.tag(Property::MailboxIds, Tag::Id(mailbox_id.into()));
                        }
                    } else {
                        return Err(SetError::invalid_property(
                            Property::MailboxIds,
                            format!("mailboxId {} does not exist.", mailbox_id),
                        ));
                    }
                }
            }
            if !fields.has_tags(&Property::MailboxIds) {
                return Err(SetError::new(
                    SetErrorType::InvalidProperties,
                    "Message has to belong to at least one mailbox.",
                ));
            }

            // Set keywords
            if let Some(keywords) = item.keywords {
                fields.untag_all(&Property::Keywords);
                for (keyword, set) in keywords {
                    if set {
                        fields.tag(Property::Keywords, keyword.tag);
                    }
                }
            } else if !item.params.patch_keyword.is_empty() {
                for (keyword, set) in item.params.patch_keyword {
                    if set {
                        fields.tag(Property::Keywords, keyword.tag);
                    }
                }
            }

            // Add messageIds
            for (header, value) in [
                (RfcHeader::MessageId, item.message_id),
                (RfcHeader::InReplyTo, item.in_reply_to),
                (RfcHeader::References, item.references),
            ] {
                if let Some(value) = value {
                    builder = builder.header(header, MessageId::from(value));
                }
            }

            // Add Addresses
            for (header, value) in [
                (RfcHeader::From, item.from),
                (RfcHeader::Sender, item.sender),
                (RfcHeader::ReplyTo, item.reply_to),
                (RfcHeader::To, item.to),
                (RfcHeader::Cc, item.cc),
                (RfcHeader::Bcc, item.bcc),
            ] {
                if let Some(value) = value {
                    builder = builder.header(header, Address::from(value));
                }
            }

            // Add Subject
            if let Some(subject) = item.subject {
                builder = builder.subject(subject);
            }

            // Add Date
            if let Some(sent_at) = item.sent_at {
                builder = builder.date(sent_at);
            }

            // Add other headers
            for header in item.params.headers {
                match header {
                    HeaderValue::AsRaw { name, value } => {
                        builder = builder.header(name, Raw::from(value));
                    }
                    HeaderValue::AsRawAll { name, value } => {
                        builder = builder.headers(name, value.into_iter().map(Raw::from));
                    }
                    HeaderValue::AsDate { name, value } => {
                        builder = builder.header(name, Date::from(value));
                    }
                    HeaderValue::AsDateAll { name, value } => {
                        builder = builder.headers(name, value.into_iter().map(Date::from));
                    }
                    HeaderValue::AsText { name, value } => {
                        builder = builder.header(name, Text::from(value));
                    }
                    HeaderValue::AsTextAll { name, value } => {
                        builder = builder.headers(name, value.into_iter().map(Text::from));
                    }
                    HeaderValue::AsURLs { name, value } => {
                        builder = builder.header(name, URL::from(value));
                    }
                    HeaderValue::AsURLsAll { name, value } => {
                        builder = builder.headers(name, value.into_iter().map(URL::from));
                    }
                    HeaderValue::AsMessageIds { name, value } => {
                        builder = builder.header(name, MessageId::from(value));
                    }
                    HeaderValue::AsMessageIdsAll { name, value } => {
                        builder = builder.headers(name, value.into_iter().map(MessageId::from));
                    }
                    HeaderValue::AsAddresses { name, value } => {
                        builder = builder.header(name, Address::from(value));
                    }
                    HeaderValue::AsAddressesAll { name, value } => {
                        builder = builder.headers(name, value.into_iter().map(Address::from));
                    }
                    HeaderValue::AsGroupedAddresses { name, value } => {
                        builder = builder.header(name, Address::from(value));
                    }
                    HeaderValue::AsGroupedAddressesAll { name, value } => {
                        builder = builder.headers(name, value.into_iter().map(Address::from));
                    }
                }
            }

            // Add text and html parts
            for (target, body_parts, ct) in [
                (&mut builder.text_body, &item.text_body, "text/plain"),
                (&mut builder.html_body, &item.html_body, "text/html"),
            ] {
                if let Some(body_part) = body_parts.as_ref().and_then(|b| b.first()) {
                    *target = body_part
                        .parse(self, account_id, item.body_values.as_ref(), ct.into())?
                        .0
                        .into();
                }
            }

            // Add attachments
            if let Some(item_attachments) = &item.attachments {
                let mut attachments = Vec::with_capacity(item_attachments.len());
                for attachment in item_attachments {
                    attachments.push(
                        attachment
                            .parse(self, account_id, item.body_values.as_ref(), None)?
                            .0,
                    );
                }
                builder.attachments = attachments.into();
            }

            // Add Body structure
            if let Some(body_structure) = &item.body_structure {
                let (mut mime_part, sub_parts) =
                    body_structure.parse(self, account_id, item.body_values.as_ref(), None)?;

                if let Some(sub_parts) = sub_parts {
                    let mut stack = Vec::new();
                    let mut it = sub_parts.iter();

                    loop {
                        while let Some(part) = it.next() {
                            let (sub_mime_part, sub_parts) =
                                part.parse(self, account_id, item.body_values.as_ref(), None)?;
                            if let Some(sub_parts) = sub_parts {
                                stack.push((mime_part, it));
                                mime_part = sub_mime_part;
                                it = sub_parts.iter();
                            } else {
                                mime_part.add_part(sub_mime_part);
                            }
                        }
                        if let Some((mut prev_mime_part, prev_it)) = stack.pop() {
                            prev_mime_part.add_part(mime_part);
                            mime_part = prev_mime_part;
                            it = prev_it;
                        } else {
                            break;
                        }
                    }
                }

                builder.body = mime_part.into();
            }

            // Make sure the message is not empty
            if builder.headers.is_empty()
                && builder.body.is_none()
                && builder.html_body.is_none()
                && builder.text_body.is_none()
                && builder.attachments.is_none()
            {
                return Err(SetError::new(
                    SetErrorType::InvalidProperties,
                    "Message has to have at least one header or body part.",
                ));
            }

            // Store blob
            let mut blob = Vec::with_capacity(1024);
            builder.write_to(&mut blob).map_err(|_| {
                StoreError::SerializeError("Failed to write to memory.".to_string())
            })?;
            let blob_id = self.blob_store(&blob)?;
            let raw_blob: JMAPBlob = (&blob_id).into();

            // Add mailbox tags
            for mailbox_tag in fields.get_tags(&Property::MailboxIds).unwrap() {
                batch.log_child_update(Collection::Mailbox, mailbox_tag.as_id() as store::JMAPId);
            }

            // Parse message
            // TODO: write parsed message directly to store, avoid parsing it again.
            let size = blob.len();
            self.mail_parse(
                document,
                blob_id,
                &blob,
                item.received_at.map(|t| t.timestamp()),
            )?;
            fields.insert(document)?;

            // Lock collection
            let lock = self.lock_account(account_id, Collection::Mail);

            // Obtain thread Id
            let thread_id = self.mail_set_thread(batch, document)?;

            Ok((
                Email {
                    id: JMAPId::from_parts(thread_id, document.document_id).into(),
                    blob_id: raw_blob.into(),
                    thread_id: JMAPId::from(thread_id).into(),
                    size: size.into(),
                    ..Default::default()
                },
                lock.into(),
            ))
        })?;

        helper.update(|id, item, batch, document| {
            let current_fields = self
                .get_orm::<Email>(account_id, id.get_document_id())?
                .ok_or_else(|| SetError::new_err(SetErrorType::NotFound))?;
            let mut fields = TinyORM::track_changes(&current_fields);

            // Set mailboxes
            if let Some(mailboxes) = item.mailbox_ids {
                fields.untag_all(&Property::MailboxIds);

                for (mailbox_id, set) in mailboxes {
                    if mailbox_ids.contains(mailbox_id.into()) {
                        if set {
                            fields.tag(Property::MailboxIds, Tag::Id(mailbox_id.into()));
                        }
                    } else {
                        return Err(SetError::invalid_property(
                            Property::MailboxIds,
                            format!("mailboxId {} does not exist.", mailbox_id),
                        ));
                    }
                }
            } else if !item.params.patch_mailbox.is_empty() {
                for (mailbox_id, set) in item.params.patch_mailbox {
                    if mailbox_ids.contains(mailbox_id.into()) {
                        if set {
                            fields.tag(Property::MailboxIds, Tag::Id(mailbox_id.into()));
                        } else {
                            fields.untag(&Property::MailboxIds, &Tag::Id(mailbox_id.into()));
                        }
                    } else {
                        return Err(SetError::invalid_property(
                            Property::MailboxIds,
                            format!("mailboxId {} does not exist.", mailbox_id),
                        ));
                    }
                }
            }

            // Set keywords
            if let Some(keywords) = item.keywords {
                fields.untag_all(&Property::Keywords);
                for (keyword, set) in keywords {
                    if set {
                        fields.tag(Property::Keywords, keyword.tag);
                    }
                }
            } else if !item.params.patch_keyword.is_empty() {
                for (keyword, set) in item.params.patch_keyword {
                    if set {
                        fields.tag(Property::Keywords, keyword.tag);
                    } else {
                        fields.untag(&Property::Keywords, &keyword.tag);
                    }
                }
            }

            // Make sure the message is at least in one mailbox
            if !fields.has_tags(&Property::MailboxIds) {
                return Err(SetError::new(
                    SetErrorType::InvalidProperties,
                    "Message has to belong to at least one mailbox.",
                ));
            }

            // Set all current mailboxes as changed if the Seen tag changed
            let mut changed_mailboxes = HashSet::new();
            if current_fields
                .get_changed_tags(&fields, &Property::Keywords)
                .iter()
                .any(|keyword| matches!(keyword, Tag::Static(k_id) if k_id == &Keyword::SEEN))
            {
                for mailbox_tag in fields.get_tags(&Property::MailboxIds).unwrap() {
                    changed_mailboxes.insert(mailbox_tag.as_id());
                }
            }

            // Add all new or removed mailboxes
            for changed_mailbox_tag in
                current_fields.get_changed_tags(&fields, &Property::MailboxIds)
            {
                changed_mailboxes.insert(changed_mailbox_tag.as_id());
            }

            // Log mailbox changes
            if !changed_mailboxes.is_empty() {
                for changed_mailbox_id in changed_mailboxes {
                    batch.log_child_update(Collection::Mailbox, changed_mailbox_id);
                }
            }

            // Merge changes
            current_fields.merge_validate(document, fields)?;

            Ok(None)
        })?;

        helper.destroy(|id, _batch, document| {
            let document_id = id.get_document_id();
            let metadata_blob_id = if let Some(metadata_blob_id) = self
                .get_document_value::<BlobId>(
                    account_id,
                    Collection::Mail,
                    document_id,
                    MessageField::Metadata.into(),
                )? {
                metadata_blob_id
            } else {
                return Ok(());
            };

            // Remove index entries
            MessageData::from_metadata(
                &self
                    .blob_get(&metadata_blob_id)?
                    .ok_or(StoreError::DataCorruption)?,
            )
            .ok_or(StoreError::DataCorruption)?
            .build_index(document, false)?;

            // Remove thread related data
            let thread_id = self
                .get_document_value::<DocumentId>(
                    account_id,
                    Collection::Mail,
                    document_id,
                    MessageField::ThreadId.into(),
                )?
                .ok_or(StoreError::DataCorruption)?;
            document.tag(
                MessageField::ThreadId,
                Tag::Id(thread_id),
                IndexOptions::new().clear(),
            );
            document.number(
                MessageField::ThreadId,
                thread_id,
                IndexOptions::new().store().clear(),
            );

            // Unlink metadata
            document.blob(metadata_blob_id, IndexOptions::new().clear());
            document.binary(
                MessageField::Metadata,
                Vec::with_capacity(0),
                IndexOptions::new().clear(),
            );

            // Delete ORM
            let fields = self
                .get_orm::<Email>(account_id, document_id)?
                .ok_or(StoreError::DataCorruption)?;
            fields.delete(document);

            Ok(())
        })?;

        helper.into_response()
    }
}

impl EmailBodyPart {
    fn parse<'y, T>(
        &'y self,
        store: &JMAPStore<T>,
        account_id: AccountId,
        body_values: Option<&'y HashMap<String, EmailBodyValue>>,
        strict_type: Option<&'static str>,
    ) -> jmap::error::set::Result<(MimePart<'y>, Option<&'y Vec<EmailBodyPart>>), Property>
    where
        T: for<'x> Store<'x> + 'static,
    {
        let content_type = self
            .type_
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "text/plain".to_string());

        if matches!(strict_type, Some(strict_type) if strict_type != content_type) {
            return Err(SetError::new(
                SetErrorType::InvalidProperties,
                format!(
                    "Expected one body part of type \"{}\"",
                    strict_type.unwrap()
                ),
            ));
        }

        let is_multipart = content_type.starts_with("multipart/");
        let mut mime_part = MimePart {
            headers: BTreeMap::new(),
            contents: if is_multipart {
                BodyPart::Multipart(vec![])
            } else if let Some(part_id) = &self.part_id {
                BodyPart::Text(
                    body_values
                        .as_ref()
                        .ok_or_else(|| {
                            SetError::new(
                                SetErrorType::InvalidProperties,
                                "Missing \"bodyValues\" object containing partId.".to_string(),
                            )
                        })?
                        .get(part_id.as_str())
                        .ok_or_else(|| {
                            SetError::new(
                                SetErrorType::InvalidProperties,
                                format!("Missing body value for partId \"{}\"", part_id),
                            )
                        })?
                        .value
                        .as_str()
                        .into(),
                )
            } else if let Some(blob_id) = &self.blob_id {
                BodyPart::Binary(
                    store
                        .blob_jmap_get(account_id, blob_id, get_message_part)
                        .map_err(|_| {
                            SetError::new(SetErrorType::BlobNotFound, "Failed to fetch blob.")
                        })?
                        .ok_or_else(|| {
                            SetError::new(
                                SetErrorType::BlobNotFound,
                                "blobId does not exist on this server.",
                            )
                        })?
                        .into(),
                )
            } else {
                return Err(SetError::new(
                    SetErrorType::InvalidProperties,
                    "Expected a \"partId\" or \"blobId\" field in body part.".to_string(),
                ));
            },
        };

        let mut content_type = ContentType::new(content_type);
        if !is_multipart {
            if content_type.c_type.starts_with("text/") {
                if matches!(mime_part.contents, BodyPart::Text(_)) {
                    content_type
                        .attributes
                        .insert("charset".into(), "utf-8".into());
                } else if let Some(charset) = &self.charset {
                    content_type
                        .attributes
                        .insert("charset".into(), charset.into());
                };
            }

            match (&self.disposition, &self.name) {
                (Some(disposition), Some(filename)) => {
                    mime_part.headers.insert(
                        "Content-Disposition".into(),
                        ContentType::new(disposition)
                            .attribute("filename", filename)
                            .into(),
                    );
                }
                (Some(disposition), None) => {
                    mime_part.headers.insert(
                        "Content-Disposition".into(),
                        ContentType::new(disposition).into(),
                    );
                }
                (None, Some(filename)) => {
                    content_type
                        .attributes
                        .insert("name".into(), filename.into());
                }
                (None, None) => (),
            };

            if let Some(languages) = self.language.as_ref() {
                mime_part.headers.insert(
                    "Content-Language".into(),
                    Text::new(languages.join(",")).into(),
                );
            }

            if let Some(cid) = &self.cid {
                mime_part
                    .headers
                    .insert("Content-ID".into(), MessageId::new(cid).into());
            }

            if let Some(location) = &self.location {
                mime_part
                    .headers
                    .insert("Content-Location".into(), Text::new(location).into());
            }
        }

        mime_part
            .headers
            .insert("Content-Type".into(), content_type.into());

        if let Some(headers) = self.headers.as_ref() {
            for header in headers {
                mime_part
                    .headers
                    .insert(header.name.as_str().into(), Raw::from(&header.value).into());
            }
        }

        for header in self.params.headers.iter() {
            match header {
                HeaderValue::AsRaw { name, value } => {
                    mime_part
                        .headers
                        .insert(name.as_str().into(), Raw::from(value).into());
                }
                HeaderValue::AsRawAll { name, value } => {
                    for value in value {
                        mime_part
                            .headers
                            .insert(name.as_str().into(), Raw::from(value).into());
                    }
                }
                _ => (),
            }
        }

        Ok((
            mime_part,
            if is_multipart {
                self.sub_parts.as_ref()
            } else {
                None
            },
        ))
    }
}
