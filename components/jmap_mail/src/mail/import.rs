use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

use crate::mail::parse::get_message_part;
use crate::mail::Keyword;

use jmap::error::method::MethodError;
use jmap::error::set::{SetError, SetErrorType};
use jmap::id::blob::JMAPBlob;
use jmap::id::JMAPIdSerialize;
use jmap::jmap_store::blob::JMAPBlobStore;
use jmap::jmap_store::import::ImportObject;
use jmap::jmap_store::orm::TinyORM;
use jmap::jmap_store::set::CreateItemResult;
use jmap::protocol::json::JSONValue;
use jmap::request::import::ImportRequest;
use mail_parser::decoders::html::{html_to_text, text_to_html};
use mail_parser::parsers::fields::thread::thread_name;
use mail_parser::{HeaderValue, Message, MessageAttachment, MessagePart, RfcHeader};
use store::blob::BlobId;
use store::core::collection::Collection;
use store::core::document::{Document, MAX_ID_LENGTH, MAX_SORT_FIELD_LENGTH};
use store::core::error::StoreError;
use store::core::tag::Tag;
use store::core::JMAPIdPrefix;
use store::nlp::Language;
use store::read::comparator::Comparator;
use store::read::filter::{FieldValue, Filter};
use store::read::DefaultIdMapper;
use store::serialize::leb128::Leb128;
use store::serialize::StoreSerialize;

use store::tracing::log::error;
use store::write::batch::WriteBatch;
use store::write::options::{IndexOptions, Options};
use store::{roaring::RoaringBitmap, AccountId, JMAPId, JMAPStore, Store, ThreadId};
use store::{DocumentId, Integer};

use crate::mail::MessageField;

use super::parse::{
    empty_text_mime_headers, header_to_jmap_address, header_to_jmap_date, header_to_jmap_id,
    header_to_jmap_text, header_to_jmap_url, mime_header_to_jmap, mime_parts_to_jmap,
};
use super::{
    HeaderName, MailHeaderForm, MailHeaderProperty, MailProperty, MessageData, MessageOutline,
    MimePart, MAX_MESSAGE_PARTS,
};

pub struct ImportMail<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub store: &'y JMAPStore<T>,
    pub account_id: AccountId,
    pub mailbox_ids: RoaringBitmap,
}
pub struct ImportItem {
    pub blob_id: JMAPBlob,
    pub mailbox_ids: Vec<DocumentId>,
    pub keywords: Vec<Keyword>,
    pub received_at: Option<u64>,
}

impl<'y, T> ImportObject<'y, T> for ImportMail<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    type Item = ImportItem;

    fn new(store: &'y JMAPStore<T>, request: &mut ImportRequest) -> jmap::Result<Self> {
        Ok(ImportMail {
            store,
            account_id: request.account_id,
            mailbox_ids: store
                .get_document_ids(request.account_id, Collection::Mailbox)?
                .unwrap_or_default(),
        })
    }

    fn parse_items(
        &self,
        request: &mut ImportRequest,
    ) -> jmap::Result<HashMap<String, Self::Item>> {
        let arguments = request
            .arguments
            .remove("emails")
            .ok_or_else(|| MethodError::InvalidArguments("Missing emails property.".to_string()))?
            .unwrap_object()
            .ok_or_else(|| MethodError::InvalidArguments("Expected email object.".to_string()))?;

        if self.store.config.mail_import_max_items > 0
            && arguments.len() > self.store.config.mail_import_max_items
        {
            return Err(MethodError::RequestTooLarge);
        }

        let mut emails = HashMap::with_capacity(arguments.len());
        for (id, item_value) in arguments {
            let mut item_value = item_value.unwrap_object().ok_or_else(|| {
                MethodError::InvalidArguments(format!("Expected mailImport object for {}.", id))
            })?;
            let item = ImportItem {
                blob_id: item_value
                    .remove("blobId")
                    .ok_or_else(|| {
                        MethodError::InvalidArguments(format!("Missing blobId for {}.", id))
                    })?
                    .parse_blob(false)?
                    .unwrap(),
                mailbox_ids: item_value
                    .remove("mailboxIds")
                    .ok_or_else(|| {
                        MethodError::InvalidArguments(format!("Missing mailboxIds for {}.", id))
                    })?
                    .unwrap_object()
                    .ok_or_else(|| {
                        MethodError::InvalidArguments(format!(
                            "Expected mailboxIds object for {}.",
                            id
                        ))
                    })?
                    .into_iter()
                    .filter_map(|(k, v)| {
                        if v.to_bool()? {
                            JMAPId::from_jmap_string(&k).map(|id| id as DocumentId)
                        } else {
                            None
                        }
                    })
                    .collect(),
                keywords: if let Some(keywords) = item_value.remove("keywords") {
                    keywords
                        .parse_array_items::<Keyword>(true)?
                        .unwrap_or_default()
                } else {
                    vec![]
                },
                received_at: if let Some(received_at) = item_value.remove("receivedAt") {
                    received_at.parse_utc_date(true)?
                } else {
                    None
                },
            };
            emails.insert(id, item);
        }

        Ok(emails)
    }

    fn import_item(&self, item: Self::Item) -> jmap::error::set::Result<JSONValue> {
        if item.mailbox_ids.is_empty() {
            return Err(SetError::invalid_property(
                "mailboxIds",
                "Message must belong to at least one mailbox.",
            ));
        }

        for &mailbox_id in &item.mailbox_ids {
            if !self.mailbox_ids.contains(mailbox_id) {
                return Err(SetError::invalid_property(
                    "mailboxIds",
                    format!(
                        "Mailbox {} does not exist.",
                        (mailbox_id as JMAPId).to_jmap_string()
                    ),
                ));
            }
        }

        if let Some(blob) =
            self.store
                .blob_jmap_get(self.account_id, &item.blob_id, get_message_part)?
        {
            Ok(self
                .store
                .mail_import(
                    self.account_id,
                    item.blob_id.id,
                    &blob,
                    item.mailbox_ids,
                    item.keywords.into_iter().map(|k| k.tag).collect(),
                    item.received_at,
                )
                .map_err(|_| {
                    SetError::new(
                        SetErrorType::Forbidden,
                        "Failed to insert message, please try again later.",
                    )
                })?
                .into())
        } else {
            Err(SetError::new(
                SetErrorType::BlobNotFound,
                format!("BlobId {} not found.", item.blob_id.to_jmap_string()),
            ))
        }
    }

    fn collection() -> Collection {
        Collection::Mail
    }
}

pub trait JMAPMailImport {
    fn mail_import(
        &self,
        account_id: AccountId,
        blob_id: BlobId,
        blob: &[u8],
        mailbox_ids: Vec<DocumentId>,
        keywords: Vec<Tag>,
        received_at: Option<u64>,
    ) -> jmap::Result<MailImportResult>;

    fn mail_parse(
        &self,
        document: &mut Document,
        blob_id: BlobId,
        raw_message: &[u8],
        received_at: Option<u64>,
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

pub struct MailImportResult {
    pub id: JMAPId,
    pub blob_id: JMAPBlob,
    pub thread_id: DocumentId,
    pub size: usize,
}

impl CreateItemResult for MailImportResult {
    fn get_id(&self) -> JMAPId {
        self.id
    }
}

impl From<MailImportResult> for JSONValue {
    fn from(import_result: MailImportResult) -> Self {
        // Generate JSON object
        let mut result = HashMap::with_capacity(4);
        result.insert("id".to_string(), import_result.id.to_jmap_string().into());
        result.insert(
            "blobId".to_string(),
            import_result.blob_id.to_jmap_string().into(),
        );
        result.insert(
            "threadId".to_string(),
            (import_result.thread_id as JMAPId).to_jmap_string().into(),
        );
        result.insert("size".to_string(), import_result.size.into());
        result.into()
    }
}

impl<T> JMAPMailImport for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_import(
        &self,
        account_id: AccountId,
        blob_id: BlobId,
        blob: &[u8],
        mailbox_ids: Vec<DocumentId>,
        keywords: Vec<Tag>,
        received_at: Option<u64>,
    ) -> jmap::Result<MailImportResult> {
        let document_id = self.assign_document_id(account_id, Collection::Mail)?;
        let mut batch = WriteBatch::new(account_id, self.config.is_in_cluster);
        let mut document = Document::new(Collection::Mail, document_id);
        let size = blob.len();

        // Parse message
        let raw_blob: JMAPBlob = (&blob_id).into();
        self.mail_parse(&mut document, blob_id, blob, received_at)?;

        // Add keyword tags
        let mut orm = TinyORM::<MessageField>::new();
        for keyword in keywords {
            orm.tag(MessageField::Keyword, keyword);
        }

        // Add mailbox tags
        for mailbox_id in mailbox_ids {
            batch.log_child_update(Collection::Mailbox, mailbox_id);
            orm.tag(MessageField::Mailbox, Tag::Id(mailbox_id));
        }

        // Serialize ORM
        orm.insert(&mut document)?;

        // Lock account while threads are merged
        let _lock = self.lock_account(batch.account_id, Collection::Mail);

        // Obtain thread Id
        let thread_id = self.mail_set_thread(&mut batch, &mut document)?;

        // Write document to store
        let result = MailImportResult {
            id: JMAPId::from_parts(thread_id, document_id),
            blob_id: raw_blob,
            thread_id,
            size,
        };
        batch.log_insert(Collection::Mail, result.id);
        batch.insert_document(document);
        self.write(batch)?;

        Ok(result)
    }

    fn mail_parse(
        &self,
        document: &mut Document,
        blob_id: BlobId,
        raw_message: &[u8],
        received_at: Option<u64>,
    ) -> store::Result<()> {
        let message = Message::parse(raw_message).ok_or_else(|| {
            StoreError::InvalidArguments("Failed to parse e-mail message.".to_string())
        })?;
        let mut total_parts = message.parts.len();
        let mut message_data = MessageData {
            properties: HashMap::with_capacity(message.headers_rfc.len() + 3),
            mime_parts: Vec::with_capacity(total_parts + 1),
            html_body: message.html_body,
            text_body: message.text_body,
            attachments: message.attachments,
            raw_message: blob_id,
        };
        let mut message_outline = MessageOutline {
            body_offset: message.offset_body,
            body_structure: message.structure,
            headers: Vec::with_capacity(total_parts + 1),
        };
        let mut has_attachments = false;

        if message.parts.len() > MAX_MESSAGE_PARTS {
            return Err(StoreError::InvalidArguments(
                "Message has too many parts.".to_string(),
            ));
        }

        // Add size
        message_data
            .properties
            .insert(MailProperty::Size, message.raw_message.len().into());

        // Add received at
        message_data.properties.insert(
            MailProperty::ReceivedAt,
            received_at
                .unwrap_or_else(|| {
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                })
                .into(),
        );

        let mut mime_parts = HashMap::with_capacity(5);

        // Build JMAP headers
        for (header_name, header_value) in message.headers_rfc {
            match header_name {
                RfcHeader::MessageId
                | RfcHeader::InReplyTo
                | RfcHeader::References
                | RfcHeader::ResentMessageId => {
                    let (value, is_collection) = header_to_jmap_id(header_value);
                    message_data.properties.insert(
                        MailProperty::Header(MailHeaderProperty::new_rfc(
                            header_name,
                            MailHeaderForm::MessageIds,
                            is_collection,
                        )),
                        value,
                    );
                }
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
                | RfcHeader::ResentSender => {
                    let (value, is_grouped, is_collection) =
                        header_to_jmap_address(header_value, false);
                    message_data.properties.insert(
                        MailProperty::Header(MailHeaderProperty::new_rfc(
                            header_name,
                            if is_grouped {
                                MailHeaderForm::GroupedAddresses
                            } else {
                                MailHeaderForm::Addresses
                            },
                            is_collection,
                        )),
                        value,
                    );
                }
                RfcHeader::Date | RfcHeader::ResentDate => {
                    let (value, is_collection) = header_to_jmap_date(header_value);
                    message_data.properties.insert(
                        MailProperty::Header(MailHeaderProperty::new_rfc(
                            header_name,
                            MailHeaderForm::Date,
                            is_collection,
                        )),
                        value,
                    );
                }
                RfcHeader::ListArchive
                | RfcHeader::ListHelp
                | RfcHeader::ListOwner
                | RfcHeader::ListPost
                | RfcHeader::ListSubscribe
                | RfcHeader::ListUnsubscribe => {
                    let (value, is_collection) = header_to_jmap_url(header_value);
                    message_data.properties.insert(
                        MailProperty::Header(MailHeaderProperty::new_rfc(
                            header_name,
                            MailHeaderForm::URLs,
                            is_collection,
                        )),
                        value,
                    );
                }
                RfcHeader::Subject
                | RfcHeader::Comments
                | RfcHeader::Keywords
                | RfcHeader::ListId => {
                    // Add Subject to index
                    if header_name == RfcHeader::Subject {
                        match &header_value {
                            HeaderValue::Text(text) => {
                                document.text(
                                    RfcHeader::Subject,
                                    text.to_string(),
                                    Language::Unknown,
                                    IndexOptions::new().full_text(0),
                                );
                            }
                            HeaderValue::TextList(list) if !list.is_empty() => {
                                document.text(
                                    RfcHeader::Subject,
                                    list.first().unwrap().to_string(),
                                    Language::Unknown,
                                    IndexOptions::new().full_text(0),
                                );
                            }
                            _ => (),
                        }
                    }

                    let (value, is_collection) = header_to_jmap_text(header_value);
                    message_data.properties.insert(
                        MailProperty::Header(MailHeaderProperty::new_rfc(
                            header_name,
                            MailHeaderForm::Text,
                            is_collection,
                        )),
                        value,
                    );
                }
                RfcHeader::ContentType
                | RfcHeader::ContentDisposition
                | RfcHeader::ContentId
                | RfcHeader::ContentLanguage
                | RfcHeader::ContentLocation => {
                    mime_header_to_jmap(&mut mime_parts, header_name, header_value);
                }
                RfcHeader::ContentTransferEncoding
                | RfcHeader::ContentDescription
                | RfcHeader::MimeVersion
                | RfcHeader::Received
                | RfcHeader::ReturnPath => (),
            }
        }

        // Add main headers as a MimePart
        message_data.mime_parts.push(MimePart::new_part(mime_parts));
        message_outline.headers.push(
            message
                .headers_raw
                .into_iter()
                .map(|(k, v)| (k.into(), v))
                .collect(),
        );

        let mut extra_mime_parts = Vec::new();

        for (part_id, part) in message.parts.into_iter().enumerate() {
            match part {
                MessagePart::Html(html) => {
                    let text = html_to_text(html.body.as_ref());
                    let text_len = text.len();
                    let html_len = html.body.len();
                    let (text_part_id, field) = if let Some(pos) =
                        message_data.text_body.iter().position(|&p| p == part_id)
                    {
                        message_data.text_body[pos] = total_parts;
                        extra_mime_parts.push(MimePart::new_text(
                            empty_text_mime_headers(false, text_len),
                            self.blob_store(text.as_bytes())?,
                            false,
                        ));
                        total_parts += 1;
                        (total_parts - 1, MessageField::Body)
                    } else if message_data.html_body.contains(&part_id) {
                        (part_id, MessageField::Body)
                    } else {
                        has_attachments = true;
                        (part_id, MessageField::Attachment)
                    };

                    document.text(
                        field,
                        text,
                        Language::Unknown,
                        IndexOptions::new().full_text(text_part_id as u32),
                    );

                    message_data.mime_parts.push(MimePart::new_html(
                        mime_parts_to_jmap(html.headers_rfc, html_len),
                        self.blob_store(html.body.as_bytes())?,
                        html.is_encoding_problem,
                    ));
                    message_outline.headers.push(
                        html.headers_raw
                            .into_iter()
                            .map(|(k, v)| (k.into(), v))
                            .collect(),
                    );
                }
                MessagePart::Text(text) => {
                    let text_len = text.body.len();
                    let field = if let Some(pos) =
                        message_data.html_body.iter().position(|&p| p == part_id)
                    {
                        let html = text_to_html(text.body.as_ref());
                        let html_len = html.len();

                        extra_mime_parts.push(MimePart::new_html(
                            empty_text_mime_headers(true, html_len),
                            self.blob_store(html.as_bytes())?,
                            false,
                        ));
                        message_data.html_body[pos] = total_parts;
                        total_parts += 1;
                        MessageField::Body
                    } else if message_data.text_body.contains(&part_id) {
                        MessageField::Body
                    } else {
                        has_attachments = true;
                        MessageField::Attachment
                    };

                    message_data.mime_parts.push(MimePart::new_text(
                        mime_parts_to_jmap(text.headers_rfc, text_len),
                        self.blob_store(text.body.as_bytes())?,
                        text.is_encoding_problem,
                    ));
                    message_outline.headers.push(
                        text.headers_raw
                            .into_iter()
                            .map(|(k, v)| (k.into(), v))
                            .collect(),
                    );

                    document.text(
                        field,
                        text.body.into_owned(),
                        Language::Unknown,
                        IndexOptions::new().full_text(part_id as u32),
                    );
                }
                MessagePart::Binary(binary) => {
                    if !has_attachments {
                        has_attachments = true;
                    }
                    message_data.mime_parts.push(MimePart::new_binary(
                        mime_parts_to_jmap(binary.headers_rfc, binary.body.len()),
                        self.blob_store(binary.body.as_ref())?,
                        binary.is_encoding_problem,
                    ));
                    message_outline.headers.push(
                        binary
                            .headers_raw
                            .into_iter()
                            .map(|(k, v)| (k.into(), v))
                            .collect(),
                    );
                }
                MessagePart::InlineBinary(binary) => {
                    message_data.mime_parts.push(MimePart::new_binary(
                        mime_parts_to_jmap(binary.headers_rfc, binary.body.len()),
                        self.blob_store(binary.body.as_ref())?,
                        binary.is_encoding_problem,
                    ));
                    message_outline.headers.push(
                        binary
                            .headers_raw
                            .into_iter()
                            .map(|(k, v)| (k.into(), v))
                            .collect(),
                    );
                }
                MessagePart::Message(nested_message) => {
                    if !has_attachments {
                        has_attachments = true;
                    }

                    let (blob_id, part_len) = match nested_message.body {
                        MessageAttachment::Parsed(mut message) => {
                            add_attached_message(document, &mut message, part_id as u32);
                            (
                                self.blob_store(message.raw_message.as_ref())?,
                                message.raw_message.len(),
                            )
                        }
                        MessageAttachment::Raw(raw_message) => {
                            if let Some(message) = &mut Message::parse(raw_message.as_ref()) {
                                add_attached_message(document, message, part_id as u32);
                            }

                            (self.blob_store(raw_message.as_ref())?, raw_message.len())
                        }
                    };

                    message_data.mime_parts.push(MimePart::new_binary(
                        mime_parts_to_jmap(nested_message.headers_rfc, part_len),
                        blob_id,
                        false,
                    ));

                    message_outline.headers.push(
                        nested_message
                            .headers_raw
                            .into_iter()
                            .map(|(k, v)| (k.into(), v))
                            .collect(),
                    );
                }
                MessagePart::Multipart(part) => {
                    message_data
                        .mime_parts
                        .push(MimePart::new_part(mime_parts_to_jmap(part.headers_rfc, 0)));
                    message_outline.headers.push(
                        part.headers_raw
                            .into_iter()
                            .map(|(k, v)| (k.into(), v))
                            .collect(),
                    );
                }
            };
        }

        // Add any HTML/text part conversions
        if !extra_mime_parts.is_empty() {
            message_data.mime_parts.append(&mut extra_mime_parts);
        }

        // Set attachment properties
        message_data
            .properties
            .insert(MailProperty::HasAttachment, has_attachments.into());

        // Serialize message data and outline
        let mut message_data_bytes = message_data
            .serialize()
            .ok_or_else(|| StoreError::SerializeError("Failed to serialize message data".into()))?;
        let mut message_outline_bytes = message_outline.serialize().ok_or_else(|| {
            StoreError::SerializeError("Failed to serialize message outline".into())
        })?;
        let mut metadata = Vec::with_capacity(
            message_data_bytes.len() + message_outline_bytes.len() + std::mem::size_of::<usize>(),
        );
        message_data_bytes.len().to_leb128_bytes(&mut metadata);
        metadata.append(&mut message_data_bytes);
        metadata.append(&mut message_outline_bytes);

        // Link blob and set message data tield
        let metadata_blob_id = self.blob_store(&metadata)?;
        document.blob(metadata_blob_id.clone(), IndexOptions::new());
        document.binary(
            MessageField::Metadata,
            metadata_blob_id.serialize().unwrap(),
            IndexOptions::new(),
        );

        // TODO search by "header exists"
        // TODO use content language when available
        // TODO index PDF, Doc, Excel, etc.

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
                    self.query_store::<DefaultIdMapper>(
                        batch.account_id,
                        Collection::Mail,
                        Filter::and(vec![
                            Filter::eq(
                                MessageField::ThreadName.into(),
                                FieldValue::Keyword(thread_name.unwrap_or("!").to_string()),
                            ),
                            Filter::or(
                                reference_ids
                                    .iter()
                                    .map(|id| {
                                        Filter::eq(
                                            MessageField::MessageIdRef.into(),
                                            FieldValue::Keyword(id.to_string()),
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
                .collect::<HashSet<ThreadId>>();

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

fn add_attached_message(document: &mut Document, message: &mut Message, part_id: u32) {
    if let Some(HeaderValue::Text(subject)) = message.headers_rfc.remove(&RfcHeader::Subject) {
        document.text(
            MessageField::Attachment,
            subject.into_owned(),
            Language::Unknown,
            IndexOptions::new().full_text(part_id << 16),
        );
    }
    for (sub_part_id, sub_part) in message.parts.drain(..).take(MAX_MESSAGE_PARTS).enumerate() {
        match sub_part {
            MessagePart::Text(text) => {
                document.text(
                    MessageField::Attachment,
                    text.body.into_owned(),
                    Language::Unknown,
                    IndexOptions::new().full_text(part_id << 16 | (sub_part_id + 1) as u32),
                );
            }
            MessagePart::Html(html) => {
                document.text(
                    MessageField::Attachment,
                    html_to_text(&html.body),
                    Language::Unknown,
                    IndexOptions::new().full_text(part_id << 16 | (sub_part_id + 1) as u32),
                );
            }
            _ => (),
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

        for (property, value) in self.properties {
            match property {
                MailProperty::Size => {
                    document.number(
                        MessageField::Size,
                        value.to_unsigned_int().ok_or(StoreError::DataCorruption)? as Integer,
                        IndexOptions::new().sort() | options,
                    );
                }
                MailProperty::ReceivedAt => {
                    document.number(
                        MessageField::ReceivedAt,
                        value.to_unsigned_int().ok_or(StoreError::DataCorruption)?,
                        IndexOptions::new().sort() | options,
                    );
                }
                MailProperty::HasAttachment => {
                    if value.unwrap_bool().ok_or(StoreError::DataCorruption)? {
                        document.tag(
                            MessageField::Attachment,
                            Tag::Default,
                            IndexOptions::new() | options,
                        );
                    }
                }
                MailProperty::Header(MailHeaderProperty {
                    header: HeaderName::Rfc(header_name),
                    ..
                }) => {
                    document.tag(
                        MessageField::HasHeader,
                        Tag::Static(header_name.into()),
                        IndexOptions::new() | options,
                    );

                    match header_name {
                        RfcHeader::MessageId
                        | RfcHeader::InReplyTo
                        | RfcHeader::References
                        | RfcHeader::ResentMessageId => value.unwrap_values(|value| {
                            match value {
                                JSONValue::String(id) if id.len() <= MAX_ID_LENGTH => {
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
                                _ => (),
                            }
                            true
                        }),

                        RfcHeader::From | RfcHeader::To | RfcHeader::Cc | RfcHeader::Bcc => {
                            let mut sort_text = String::with_capacity(MAX_SORT_FIELD_LENGTH);
                            let mut found_addr = false;
                            let mut last_is_space = true;

                            value.unwrap_object_properties(
                                &["name", "email"],
                                |property_idx, value| {
                                    if let Some(value) = value.unwrap_string() {
                                        let is_addr = property_idx == 1;

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

                                        if is_addr {
                                            if value.len() <= MAX_ID_LENGTH {
                                                document.text(
                                                    header_name,
                                                    value.to_lowercase(),
                                                    Language::Unknown,
                                                    IndexOptions::new().keyword() | options,
                                                );
                                            }
                                        } else {
                                            document.text(
                                                header_name,
                                                value,
                                                Language::Unknown,
                                                IndexOptions::new().tokenize() | options,
                                            );
                                        }
                                    }

                                    true
                                },
                            );

                            document.text(
                                header_name,
                                if !sort_text.is_empty() {
                                    sort_text
                                } else {
                                    "!".to_string()
                                },
                                Language::Unknown,
                                IndexOptions::new().sort() | options,
                            );
                        }
                        RfcHeader::Date => value.unwrap_values(|value| {
                            if let Some(timestamp) = value.to_unsigned_int() {
                                document.number(
                                    header_name,
                                    timestamp,
                                    IndexOptions::new().sort() | options,
                                );
                            }
                            false
                        }),
                        RfcHeader::Subject => {
                            value.unwrap_values(|value| {
                                if let Some(subject) = value.unwrap_string() {
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
                                        IndexOptions::new().keyword().sort() | options,
                                    );
                                }

                                false
                            });
                        }
                        RfcHeader::Keywords => {
                            value.unwrap_values(|value| {
                                match value {
                                    JSONValue::String(keyword)
                                        if keyword.len() <= MAX_ID_LENGTH =>
                                    {
                                        document.text(
                                            MessageField::MessageIdRef,
                                            keyword,
                                            Language::Unknown,
                                            IndexOptions::new().keyword() | options,
                                        );
                                    }
                                    _ => (),
                                }
                                true
                            });
                        }
                        RfcHeader::Comments => {
                            value.unwrap_values(|value| {
                                if let Some(value) = value.unwrap_string() {
                                    document.text(
                                        header_name,
                                        value,
                                        Language::Unknown,
                                        IndexOptions::new().tokenize() | options,
                                    );
                                }
                                true
                            });
                        }
                        _ => (),
                    }
                }
                _ => (),
            }
        }

        // Link/unlink blobs
        document.blob(self.raw_message, IndexOptions::new() | options);
        for mime_part in self.mime_parts {
            if let Some(blob_id) = mime_part.blob_id {
                document.blob(blob_id, IndexOptions::new() | options);
            }
        }

        Ok(())
    }
}
