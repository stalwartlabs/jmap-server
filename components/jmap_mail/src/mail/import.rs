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

use std::sync::Arc;
use std::time::SystemTime;

use jmap::error::method::MethodError;
use jmap::error::set::{SetError, SetErrorType};
use jmap::jmap_store::changes::JMAPChanges;
use jmap::jmap_store::Object;
use jmap::orm::serialize::JMAPOrm;
use jmap::orm::TinyORM;
use jmap::request::{ACLEnforce, MaybeIdReference, MaybeResultReference, ResultReference};
use jmap::types::blob::JMAPBlob;
use jmap::types::date::JMAPDate;
use jmap::types::jmap::JMAPId;
use jmap::types::state::JMAPState;
use mail_parser::decoders::html::html_to_text;
use mail_parser::parsers::fields::thread::thread_name;
use mail_parser::{GetHeader, HeaderName, HeaderValue, Message, PartType, RfcHeader};
use store::ahash::AHashMap;
use store::ahash::AHashSet;
use store::blob::BlobId;
use store::core::acl::{ACLToken, ACL};
use store::core::collection::Collection;
use store::core::document::{Document, MAX_ID_LENGTH, MAX_SORT_FIELD_LENGTH};
use store::core::error::StoreError;
use store::core::tag::Tag;
use store::core::vec_map::VecMap;
use store::core::JMAPIdPrefix;
use store::log::changes::ChangeId;
use store::nlp::Language;
use store::read::comparator::Comparator;
use store::read::filter::{Filter, Query};
use store::read::FilterMapper;
use store::serialize::StoreSerialize;

use store::tracing::error;
use store::write::batch::WriteBatch;
use store::write::options::{IndexOptions, Options};
use store::{AccountId, JMAPStore, SharedBitmap, Store, ThreadId};
use store::{DocumentId, Integer, LongInteger};

use crate::mail::MessageField;

use super::conv::HeaderValueInto;
use super::get::{BlobResult, JMAPGetMail};
use super::schema::{Email, Keyword, Property};
use super::sharing::JMAPShareMail;
use super::{MessageData, MessagePart, MimePart, MimePartType, MAX_MESSAGE_PARTS};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct EmailImportRequest {
    #[serde(skip)]
    pub acl: Option<Arc<ACLToken>>,

    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "ifInState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub if_in_state: Option<JMAPState>,

    pub emails: VecMap<String, EmailImport>,
}

#[derive(Debug, Clone)]
pub struct EmailImport {
    pub blob_id: JMAPBlob,
    pub mailbox_ids: Option<MaybeResultReference<VecMap<MaybeIdReference, bool>>>,
    pub keywords: Option<VecMap<Keyword, bool>>,
    pub received_at: Option<JMAPDate>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct EmailImportResponse {
    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "oldState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_state: Option<JMAPState>,

    #[serde(rename = "newState")]
    pub new_state: JMAPState,

    #[serde(rename = "created")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created: Option<VecMap<String, Email>>,

    #[serde(rename = "notCreated")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub not_created: Option<VecMap<String, SetError<Property>>>,
}

pub trait JMAPMailImport {
    fn mail_import(&self, request: EmailImportRequest) -> jmap::Result<EmailImportResponse>;

    fn mail_import_item(
        &self,
        account_id: AccountId,
        blob_id: BlobId,
        blob: &[u8],
        mailbox_ids: Vec<DocumentId>,
        keywords: Vec<Tag>,
        received_at: Option<i64>,
    ) -> jmap::Result<Email>;

    fn mail_parse_item(
        &self,
        document: &mut Document,
        blob_id: BlobId,
        message: Message,
        received_at: Option<i64>,
    ) -> store::Result<()>;

    fn mail_set_thread(
        &self,
        batch: &mut WriteBatch,
        document: &mut Document,
    ) -> store::Result<DocumentId>;

    fn mail_merge_threads(
        &self,
        documents: &mut WriteBatch,
        thread_ids: Vec<ThreadId>,
    ) -> store::Result<ThreadId>;
}

impl<T> JMAPMailImport for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_import(&self, request: EmailImportRequest) -> jmap::Result<EmailImportResponse> {
        let account_id = request.account_id.get_document_id();
        let mailbox_document_ids = self
            .get_document_ids(account_id, Collection::Mailbox)?
            .unwrap_or_default();
        let acl = request.acl.unwrap();
        let is_shared_account = acl.is_shared(account_id);

        let old_state = self.get_state(account_id, Collection::Mail)?;
        if let Some(if_in_state) = request.if_in_state {
            if old_state != if_in_state {
                return Err(MethodError::StateMismatch);
            }
        }

        let mut created = VecMap::with_capacity(request.emails.len());
        let mut not_created = VecMap::with_capacity(request.emails.len());

        'outer: for (id, item) in request.emails {
            if let Some(mailbox_ids) = item.mailbox_ids {
                let mailbox_ids = mailbox_ids
                    .unwrap_value()
                    .ok_or_else(|| {
                        MethodError::InvalidArguments(
                            "Invalid mailboxIds result reference.".to_string(),
                        )
                    })?
                    .into_iter()
                    .filter_map(|(id, value)| (id.unwrap_value()?, value).into())
                    .collect::<AHashMap<JMAPId, bool>>();

                for mailbox_id in mailbox_ids.keys() {
                    let document_id = mailbox_id.get_document_id();
                    if !mailbox_document_ids.contains(document_id) {
                        not_created.append(
                            id,
                            SetError::invalid_properties()
                                .with_property(Property::MailboxIds)
                                .with_description(format!(
                                    "Mailbox {} does not exist.",
                                    mailbox_id
                                )),
                        );
                        continue 'outer;
                    } else if is_shared_account
                        && !self
                            .mail_shared_folders(account_id, &acl.member_of, ACL::AddItems)?
                            .has_access(document_id)
                    {
                        not_created.append(
                            id,
                            SetError::forbidden().with_description(format!(
                                "You are not allowed to import messages into mailbox {}.",
                                mailbox_id
                            )),
                        );
                        continue 'outer;
                    }
                }

                // Make sure the message does not exist already
                {
                    let _lock = self.lock_collection(account_id, Collection::Mail);
                    if let Some(document_id) = self.blob_any_linked_document(
                        &item.blob_id.id,
                        account_id,
                        Collection::Mail,
                    )? {
                        if let (Some(thread_id), Some(current_fields)) = (
                            self.get_document_value::<DocumentId>(
                                account_id,
                                Collection::Mail,
                                document_id,
                                MessageField::ThreadId.into(),
                            )?,
                            self.get_orm::<Email>(account_id, document_id)?,
                        ) {
                            let email_id = JMAPId::from_parts(thread_id, document_id);
                            let mut fields = TinyORM::track_changes(&current_fields);
                            for mailbox_id in mailbox_ids.keys() {
                                fields.tag(
                                    Property::MailboxIds,
                                    Tag::Id(mailbox_id.get_document_id()),
                                );
                            }
                            let added_mailboxes =
                                current_fields.get_added_tags(&fields, &Property::MailboxIds);
                            if !added_mailboxes.is_empty() {
                                let mut batch = WriteBatch::new(account_id);
                                let mut document = Document::new(Collection::Mail, document_id);

                                for added_mailbox in added_mailboxes {
                                    batch.log_child_update(
                                        Collection::Mailbox,
                                        added_mailbox.as_id(),
                                    );
                                }
                                current_fields.merge(&mut document, fields)?;
                                debug_assert!(!document.is_empty());
                                batch.update_document(document);
                                batch.log_update(Collection::Mail, email_id);
                                self.write(batch)?;
                            }

                            let mut email = Email::default();
                            email.insert(Property::Id, email_id);
                            email.insert(Property::BlobId, JMAPBlob::new(item.blob_id.id));
                            email.insert(Property::ThreadId, JMAPId::from(thread_id));

                            created.append(id, email);
                            continue 'outer;
                        }
                    }
                }

                match self.mail_blob_get(account_id, &acl, &item.blob_id)? {
                    BlobResult::Blob(blob) => {
                        created.append(
                            id,
                            self.mail_import_item(
                                account_id,
                                item.blob_id.id,
                                &blob,
                                mailbox_ids
                                    .into_iter()
                                    .filter_map(|(id, set)| {
                                        if set {
                                            id.get_document_id().into()
                                        } else {
                                            None
                                        }
                                    })
                                    .collect(),
                                item.keywords
                                    .map(|keywords| {
                                        keywords
                                            .into_iter()
                                            .filter_map(
                                                |(k, set)| if set { k.tag.into() } else { None },
                                            )
                                            .collect()
                                    })
                                    .unwrap_or_default(),
                                item.received_at.map(|t| t.timestamp()),
                            )?,
                        );
                    }
                    BlobResult::Unauthorized => {
                        not_created.append(
                            id,
                            SetError::new(SetErrorType::Forbidden).with_description(format!(
                                "You do not have access to blobId {}.",
                                item.blob_id
                            )),
                        );
                    }
                    BlobResult::NotFound => {
                        not_created.append(
                            id,
                            SetError::new(SetErrorType::BlobNotFound)
                                .with_description(format!("BlobId {} not found.", item.blob_id)),
                        );
                    }
                }
            } else {
                not_created.append(
                    id,
                    SetError::invalid_properties()
                        .with_property(Property::MailboxIds)
                        .with_description("Message must belong to at least one mailbox."),
                );
            }
        }

        Ok(EmailImportResponse {
            account_id: request.account_id,
            new_state: if !created.is_empty() {
                self.get_state(account_id, Collection::Mail)?
            } else {
                old_state.clone()
            },
            old_state: old_state.into(),
            created: if !created.is_empty() {
                created.into()
            } else {
                None
            },
            not_created: if !not_created.is_empty() {
                not_created.into()
            } else {
                None
            },
        })
    }

    fn mail_import_item(
        &self,
        account_id: AccountId,
        blob_id: BlobId,
        blob: &[u8],
        mailbox_ids: Vec<DocumentId>,
        keywords: Vec<Tag>,
        received_at: Option<i64>,
    ) -> jmap::Result<Email> {
        let document_id = self.assign_document_id(account_id, Collection::Mail)?;
        let mut batch = WriteBatch::new(account_id);
        let mut document = Document::new(Collection::Mail, document_id);
        let size = blob.len();

        // Parse message
        let raw_blob: JMAPBlob = (&blob_id).into();
        self.mail_parse_item(
            &mut document,
            blob_id,
            Message::parse(blob).ok_or_else(|| {
                MethodError::InvalidArguments("Failed to parse e-mail message.".to_string())
            })?,
            received_at,
        )?;

        // Add keyword tags
        let mut orm = TinyORM::<Email>::new();
        for keyword in keywords {
            orm.tag(Property::Keywords, keyword);
        }

        // Add mailbox tags
        for mailbox_id in mailbox_ids {
            batch.log_child_update(Collection::Mailbox, mailbox_id);
            orm.tag(Property::MailboxIds, Tag::Id(mailbox_id));
        }

        // Serialize ORM
        orm.insert(&mut document)?;

        // Lock account while threads are merged
        let _lock = self.lock_collection(batch.account_id, Collection::Mail);

        // Obtain thread Id
        let thread_id = self.mail_set_thread(&mut batch, &mut document)?;

        // Write document to store
        let id = JMAPId::from_parts(thread_id, document_id);
        batch.log_insert(Collection::Mail, id);
        batch.insert_document(document);
        self.write(batch)?;

        // Build email result
        let mut email = Email::default();
        email.insert(Property::Id, id);
        email.insert(Property::BlobId, raw_blob);
        email.insert(Property::ThreadId, JMAPId::from(thread_id));
        email.insert(Property::Size, size);

        Ok(email)
    }

    fn mail_parse_item(
        &self,
        document: &mut Document,
        blob_id: BlobId,
        mut message: Message,
        received_at: Option<i64>,
    ) -> store::Result<()> {
        let root_part = message.get_root_part();
        let mut message_data = MessageData {
            headers: VecMap::with_capacity(root_part.headers.len()),
            body_offset: root_part.offset_body,
            mime_parts: Vec::with_capacity(message.parts.len()),
            html_body: message.html_body,
            text_body: message.text_body,
            attachments: message.attachments,
            raw_message: blob_id,
            size: message.raw_message.len(),
            received_at: received_at.unwrap_or_else(|| {
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0) as i64
            }),
            has_attachments: false,
        };
        let mut has_attachments = false;

        if message.parts.len() > MAX_MESSAGE_PARTS {
            return Err(StoreError::InvalidArguments(
                "Message has too many parts.".to_string(),
            ));
        }

        // Build JMAP headers
        let root_part = &mut message.parts[0];
        let message_language = root_part.get_language().unwrap_or(Language::Unknown);
        for header in root_part.headers.iter_mut() {
            let header_name = if let HeaderName::Rfc(header_name) = &header.name {
                *header_name
            } else {
                continue;
            };

            let header_value = match header_name {
                RfcHeader::MessageId
                | RfcHeader::InReplyTo
                | RfcHeader::References
                | RfcHeader::ResentMessageId => std::mem::take(&mut header.value).into_keyword(),
                RfcHeader::From
                | RfcHeader::To
                | RfcHeader::Cc
                | RfcHeader::Bcc
                | RfcHeader::ReplyTo
                | RfcHeader::Sender
                | RfcHeader::ResentTo
                | RfcHeader::ResentFrom
                | RfcHeader::ResentBcc
                | RfcHeader::ResentCc
                | RfcHeader::ResentSender => std::mem::take(&mut header.value).into_address(),
                RfcHeader::Date | RfcHeader::ResentDate => {
                    std::mem::take(&mut header.value).into_date()
                }
                RfcHeader::ListArchive
                | RfcHeader::ListHelp
                | RfcHeader::ListOwner
                | RfcHeader::ListPost
                | RfcHeader::ListSubscribe
                | RfcHeader::ListUnsubscribe => std::mem::take(&mut header.value).into_url(),
                RfcHeader::Subject
                | RfcHeader::Comments
                | RfcHeader::Keywords
                | RfcHeader::ListId => {
                    // Add Subject to index
                    if header_name == RfcHeader::Subject {
                        match &header.value {
                            HeaderValue::Text(text) => {
                                document.text(
                                    RfcHeader::Subject,
                                    text.to_string(),
                                    message_language,
                                    IndexOptions::new().full_text(0),
                                );
                            }
                            HeaderValue::TextList(list) if !list.is_empty() => {
                                document.text(
                                    RfcHeader::Subject,
                                    list.first().unwrap().to_string(),
                                    message_language,
                                    IndexOptions::new().full_text(0),
                                );
                            }
                            _ => (),
                        }
                    }

                    std::mem::take(&mut header.value).into_text()
                }
                _ => None,
            };

            if let Some(header_value) = header_value {
                message_data
                    .headers
                    .get_mut_or_insert(header_name)
                    .push(header_value);
            }
        }

        for (part_id, message_part) in message.parts.into_iter().enumerate() {
            let part = MessagePart {
                offset_start: message_part.offset_body,
                offset_end: message_part.offset_end,
                encoding: message_part.encoding,
            };
            let part_language = message_part.get_language().unwrap_or(message_language);
            let (mime_type, part_size) = match message_part.body {
                PartType::Html(html) => {
                    let field = if message_data.text_body.contains(&part_id)
                        || message_data.html_body.contains(&part_id)
                    {
                        MessageField::Body
                    } else {
                        has_attachments = true;
                        MessageField::Attachment
                    };

                    document.text(
                        field,
                        html_to_text(html.as_ref()),
                        part_language,
                        IndexOptions::new().full_text((part_id + 1) as u32),
                    );

                    (MimePartType::Html { part }, html.len())
                }
                PartType::Text(text) => {
                    let field = if message_data.text_body.contains(&part_id)
                        || message_data.html_body.contains(&part_id)
                    {
                        MessageField::Body
                    } else {
                        has_attachments = true;
                        MessageField::Attachment
                    };

                    let text_len = text.len();
                    document.text(
                        field,
                        text.into_owned(),
                        part_language,
                        IndexOptions::new().full_text((part_id + 1) as u32),
                    );
                    (MimePartType::Text { part }, text_len)
                }
                PartType::Binary(binary) => {
                    if !has_attachments {
                        has_attachments = true;
                    }
                    (MimePartType::Other { part }, binary.len())
                }
                PartType::InlineBinary(binary) => (MimePartType::Other { part }, binary.len()),
                PartType::Message(mut nested_message) => {
                    if !has_attachments {
                        has_attachments = true;
                    }
                    let size = nested_message.parts[0].raw_len();
                    document.add_message(&mut nested_message, (part_id) as u32);

                    (MimePartType::Other { part }, size)
                }
                PartType::Multipart(subparts) => (MimePartType::MultiPart { subparts }, 0),
            };

            message_data.mime_parts.push(MimePart::from_headers(
                message_part.headers,
                mime_type,
                message_part.is_encoding_problem,
                part_size,
            ));
        }

        // Set attachment properties
        if has_attachments {
            message_data.has_attachments = true;
        }

        // Link blob and set message data field
        let metadata_bytes = message_data
            .serialize()
            .ok_or_else(|| StoreError::SerializeError("Failed to serialize message data".into()))?;
        let metadata_blob_id = BlobId::new_local(&metadata_bytes);

        self.blob_store(&metadata_blob_id, metadata_bytes)?;
        document.binary(
            MessageField::Metadata,
            metadata_blob_id.serialize().unwrap(),
            IndexOptions::new(),
        );
        document.blob(metadata_blob_id, IndexOptions::new());

        // Build index
        message_data.build_index(document, true)
    }

    fn mail_set_thread(
        &self,
        batch: &mut WriteBatch,
        document: &mut Document,
    ) -> store::Result<DocumentId> {
        // Obtain thread name and reference ids
        let mut reference_ids = Vec::new();
        let mut thread_name = None;
        for field in &document.text_fields {
            if field.field == MessageField::ThreadName as u8 {
                thread_name = field.value.text.as_str().into();
            } else if field.field == MessageField::MessageIdRef as u8 {
                reference_ids.push(field.value.text.as_str());
            }
        }

        // Obtain thread id
        let thread_id = if !reference_ids.is_empty() {
            // Obtain thread ids for all matching document ids
            let thread_ids = self
                .get_multi_document_value(
                    batch.account_id,
                    Collection::Mail,
                    self.query_store::<FilterMapper>(
                        batch.account_id,
                        Collection::Mail,
                        Filter::and(vec![
                            Filter::eq(
                                MessageField::ThreadName.into(),
                                Query::Keyword(thread_name.unwrap_or("!").to_string()),
                            ),
                            Filter::or(
                                reference_ids
                                    .iter()
                                    .map(|id| {
                                        Filter::eq(
                                            MessageField::MessageIdRef.into(),
                                            Query::Keyword(id.to_string()),
                                        )
                                    })
                                    .collect(),
                            ),
                        ]),
                        Comparator::None,
                    )?
                    .into_iter()
                    .map(|id| id.get_document_id())
                    .collect::<Vec<DocumentId>>()
                    .into_iter(),
                    MessageField::ThreadId.into(),
                )?
                .into_iter()
                .flatten()
                .collect::<AHashSet<ThreadId>>();

            match thread_ids.len() {
                1 => {
                    // There was just one match, use it as the thread id
                    thread_ids.into_iter().next()
                }
                0 => None,
                _ => {
                    // Merge all matching threads
                    Some(self.mail_merge_threads(batch, thread_ids.into_iter().collect())?)
                }
            }
        } else {
            None
        };

        let thread_id = if let Some(thread_id) = thread_id {
            batch.log_child_update(Collection::Thread, thread_id);
            thread_id
        } else {
            let thread_id = self.assign_document_id(batch.account_id, Collection::Thread)?;
            batch.log_insert(Collection::Thread, thread_id);
            thread_id
        };

        document.tag(
            MessageField::ThreadId,
            Tag::Id(thread_id),
            IndexOptions::new(),
        );

        document.number(
            MessageField::ThreadId,
            thread_id,
            IndexOptions::new().store(),
        );

        Ok(thread_id)
    }

    fn mail_merge_threads(
        &self,
        batch: &mut WriteBatch,
        thread_ids: Vec<ThreadId>,
    ) -> store::Result<ThreadId> {
        // Query tags for all thread ids
        let mut document_sets = Vec::with_capacity(thread_ids.len());

        for (pos, document_set) in self
            .get_tags(
                batch.account_id,
                Collection::Mail,
                MessageField::ThreadId.into(),
                &thread_ids
                    .iter()
                    .map(|id| Tag::Id(*id))
                    .collect::<Vec<Tag>>(),
            )?
            .into_iter()
            .enumerate()
        {
            if let Some(document_set) = document_set {
                debug_assert!(!document_set.is_empty());
                document_sets.push((document_set, thread_ids[pos]));
            } else {
                error!(
                    "No tags found for thread id {}, account: {}.",
                    thread_ids[pos], batch.account_id
                );
            }
        }

        document_sets.sort_unstable_by_key(|i| i.0.len());

        let mut document_sets = document_sets.into_iter().rev();
        let thread_id = document_sets.next().unwrap().1;

        for (document_set, delete_thread_id) in document_sets {
            for document_id in document_set {
                let mut document = Document::new(Collection::Mail, document_id);
                document.tag(
                    MessageField::ThreadId,
                    Tag::Id(thread_id),
                    IndexOptions::new(),
                );
                document.tag(
                    MessageField::ThreadId,
                    Tag::Id(delete_thread_id),
                    IndexOptions::new().clear(),
                );
                document.number(
                    MessageField::ThreadId,
                    thread_id,
                    IndexOptions::new().store(),
                );
                batch.log_move(
                    Collection::Mail,
                    JMAPId::from_parts(delete_thread_id, document_id),
                    JMAPId::from_parts(thread_id, document_id),
                );
                batch.update_document(document);
            }

            batch.log_delete(Collection::Thread, delete_thread_id);
        }

        Ok(thread_id)
    }
}

impl EmailImport {
    fn eval_id_references(
        &mut self,
        mut fnc: impl FnMut(&str) -> Option<JMAPId>,
    ) -> jmap::Result<()> {
        if let Some(MaybeResultReference::Value(value)) = self.mailbox_ids.as_mut() {
            if value
                .keys()
                .any(|k| matches!(k, MaybeIdReference::Reference(_)))
            {
                let mut new_values = VecMap::with_capacity(value.len());

                for (id, value) in std::mem::take(value).into_iter() {
                    if let MaybeIdReference::Reference(id) = &id {
                        if let Some(id) = fnc(id) {
                            new_values.append(MaybeIdReference::Value(id), value);
                            continue;
                        }
                    }
                    new_values.append(id, value);
                }

                *value = new_values;
            }
        }
        Ok(())
    }

    fn eval_result_references(
        &mut self,
        mut fnc: impl FnMut(&ResultReference) -> Option<Vec<u64>>,
    ) -> jmap::Result<()> {
        if let Some(items) = self.mailbox_ids.as_mut() {
            if let Some(rr) = items.result_reference()? {
                if let Some(ids) = fnc(rr) {
                    *items = MaybeResultReference::Value(
                        ids.into_iter()
                            .map(|id| (MaybeIdReference::Value(id.into()), true))
                            .collect(),
                    );
                } else {
                    return Err(MethodError::InvalidResultReference(
                        "Failed to evaluate result reference.".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

impl EmailImportRequest {
    pub fn eval_references(
        &mut self,
        mut result_map_fnc: impl FnMut(&ResultReference) -> Option<Vec<u64>>,
        created_ids: &AHashMap<String, JMAPId>,
    ) -> jmap::Result<()> {
        for email in self.emails.values_mut() {
            email.eval_result_references(&mut result_map_fnc)?;
            email.eval_id_references(|id| created_ids.get(id).copied())?;
        }
        Ok(())
    }
}

trait AddMessage {
    fn add_message(&mut self, message: &mut Message, part_id: u32);
}

impl AddMessage for Document {
    fn add_message(&mut self, message: &mut Message, part_id: u32) {
        let message_language = message.parts[0].get_language().unwrap_or(Language::Unknown);

        if let Some(HeaderValue::Text(subject)) = message.remove_header_rfc(RfcHeader::Subject) {
            self.text(
                MessageField::Attachment,
                subject.into_owned(),
                message_language,
                IndexOptions::new().full_text(part_id << 16),
            );
        }
        for (sub_part_id, sub_part) in message.parts.drain(..).take(MAX_MESSAGE_PARTS).enumerate() {
            let sub_part_language = sub_part.get_language().unwrap_or(message_language);
            match sub_part.body {
                PartType::Text(text) => {
                    self.text(
                        MessageField::Attachment,
                        text.into_owned(),
                        sub_part_language,
                        IndexOptions::new().full_text(part_id << 16 | (sub_part_id + 1) as u32),
                    );
                }
                PartType::Html(html) => {
                    self.text(
                        MessageField::Attachment,
                        html_to_text(&html),
                        sub_part_language,
                        IndexOptions::new().full_text(part_id << 16 | (sub_part_id + 1) as u32),
                    );
                }
                _ => (),
            }
        }
    }
}

impl MessageData {
    pub fn build_index(self, document: &mut Document, is_insert: bool) -> store::Result<()> {
        let options = if is_insert {
            IndexOptions::new()
        } else {
            IndexOptions::new().clear()
        };

        document.number(
            MessageField::Size,
            self.size as Integer,
            IndexOptions::new().index() | options,
        );

        document.number(
            MessageField::ReceivedAt,
            self.received_at as LongInteger,
            IndexOptions::new().index() | options,
        );

        if self.has_attachments {
            document.tag(
                MessageField::Attachment,
                Tag::Default,
                IndexOptions::new() | options,
            );
        }

        for (header_name, mut values) in self.headers {
            document.tag(
                MessageField::HasHeader,
                Tag::Static(header_name.into()),
                IndexOptions::new() | options,
            );

            match header_name {
                RfcHeader::MessageId
                | RfcHeader::InReplyTo
                | RfcHeader::References
                | RfcHeader::ResentMessageId => {
                    for value in values {
                        if let Some(ids) = value.unwrap_textlist() {
                            for id in ids {
                                if id.len() <= MAX_ID_LENGTH {
                                    if header_name == RfcHeader::MessageId {
                                        document.text(
                                            header_name,
                                            id.to_string(),
                                            Language::Unknown,
                                            IndexOptions::new().keyword() | options,
                                        );
                                    }
                                    document.text(
                                        MessageField::MessageIdRef,
                                        id,
                                        Language::Unknown,
                                        IndexOptions::new().keyword() | options,
                                    );
                                }
                            }
                        }
                    }
                }

                RfcHeader::From | RfcHeader::To | RfcHeader::Cc | RfcHeader::Bcc => {
                    let mut sort_text = String::with_capacity(MAX_SORT_FIELD_LENGTH);
                    let mut found_addr = false;
                    let mut last_is_space = true;

                    for value in values {
                        value.visit_addresses(|value, is_addr| {
                            if !found_addr {
                                if !sort_text.is_empty() {
                                    sort_text.push(' ');
                                    last_is_space = true;
                                }
                                found_addr = is_addr;
                                'outer: for ch in value.chars() {
                                    for ch in ch.to_lowercase() {
                                        if sort_text.len() < MAX_SORT_FIELD_LENGTH {
                                            let is_space = ch.is_whitespace();
                                            if !is_space || !last_is_space {
                                                sort_text.push(ch);
                                                last_is_space = is_space;
                                            }
                                        } else {
                                            found_addr = true;
                                            break 'outer;
                                        }
                                    }
                                }
                            }

                            document.text(
                                header_name,
                                value,
                                Language::Unknown,
                                IndexOptions::new().tokenize() | options,
                            );

                            true
                        });
                    }

                    document.text(
                        header_name,
                        if !sort_text.is_empty() {
                            sort_text
                        } else {
                            "!".to_string()
                        },
                        Language::Unknown,
                        IndexOptions::new().index() | options,
                    );
                }
                RfcHeader::Date => {
                    if let Some(timestamp) = values.pop().and_then(|t| t.unwrap_timestamp()) {
                        document.number(
                            header_name,
                            timestamp as LongInteger,
                            IndexOptions::new().index() | options,
                        );
                    }
                }
                RfcHeader::Subject => {
                    if let Some(subject) = values.pop().and_then(|t| t.unwrap_text()) {
                        // Obtain thread name
                        let thread_name = thread_name(&subject);
                        document.text(
                            MessageField::ThreadName,
                            if !thread_name.is_empty() {
                                thread_name.to_string()
                            } else {
                                "!".to_string()
                            },
                            Language::Unknown,
                            IndexOptions::new().keyword().index() | options,
                        );
                    }
                }
                RfcHeader::Keywords => {
                    for value in values {
                        if let Some(keywords) = value.unwrap_textlist() {
                            for keyword in keywords {
                                if keyword.len() <= MAX_ID_LENGTH {
                                    document.text(
                                        MessageField::MessageIdRef,
                                        keyword,
                                        Language::Unknown,
                                        IndexOptions::new().keyword() | options,
                                    );
                                }
                            }
                        }
                    }
                }
                RfcHeader::Comments => {
                    for value in values {
                        if let Some(comments) = value.unwrap_textlist() {
                            for comment in comments {
                                document.text(
                                    header_name,
                                    comment,
                                    Language::Unknown,
                                    IndexOptions::new().tokenize() | options,
                                );
                            }
                        }
                    }
                }
                _ => (),
            }
        }

        // Link/Unlink raw message
        document.blob(self.raw_message, IndexOptions::new() | options);

        Ok(())
    }
}

impl EmailImportResponse {
    pub fn account_id(&self) -> AccountId {
        self.account_id.get_document_id()
    }

    pub fn has_changes(&self) -> Option<ChangeId> {
        if self.old_state.as_ref().unwrap_or(&JMAPState::Initial) != &self.new_state {
            self.new_state.get_change_id().into()
        } else {
            None
        }
    }

    pub fn created_ids(&self) -> Option<AHashMap<String, JMAPId>> {
        if let Some(created) = &self.created {
            let mut created_ids = AHashMap::with_capacity(created.len());
            for (create_id, item) in created {
                created_ids.insert(create_id.to_string(), *item.id().unwrap());
            }
            created_ids.into()
        } else {
            None
        }
    }
}

trait GetContentLanguage {
    fn get_language(&self) -> Option<Language>;
}

impl GetContentLanguage for mail_parser::MessagePart<'_> {
    fn get_language(&self) -> Option<Language> {
        self.headers
            .get_rfc(&RfcHeader::ContentLanguage)
            .and_then(|v| {
                Language::from_iso_639(match v {
                    HeaderValue::Text(v) => v,
                    HeaderValue::TextList(v) => v.first()?,
                    _ => {
                        return None;
                    }
                })
                .unwrap_or(Language::Unknown)
                .into()
            })
    }
}
