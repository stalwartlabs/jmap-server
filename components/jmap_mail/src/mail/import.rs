use std::collections::{HashMap, HashSet};

use crate::mail::parse::get_message_blob;
use crate::mail::Keyword;

use jmap::error::method::MethodError;
use jmap::error::set::{SetError, SetErrorType};
use jmap::id::blob::JMAPBlob;
use jmap::id::JMAPIdSerialize;
use jmap::jmap_store::blob::JMAPBlobStore;
use jmap::jmap_store::import::ImportObject;
use jmap::jmap_store::set::CreateItemResult;
use jmap::protocol::json::JSONValue;
use jmap::request::import::ImportRequest;
use mail_parser::decoders::html::{html_to_text, text_to_html};
use mail_parser::parsers::fields::thread::thread_name;
use mail_parser::{HeaderValue, Message, MessageAttachment, MessagePart, RfcHeader};
use nlp::Language;
use store::batch::{Document, MAX_ID_LENGTH};
use store::blob::BlobId;
use store::chrono::{LocalResult, SecondsFormat, TimeZone, Utc};
use store::field::{IndexOptions, Options};
use store::leb128::Leb128;
use store::query::DefaultIdMapper;
use store::serialize::StoreSerialize;
use store::tracing::debug;
use store::tracing::log::error;
use store::{
    batch::WriteBatch, field::Text, roaring::RoaringBitmap, AccountId, Comparator, FieldValue,
    Filter, JMAPId, JMAPStore, Store, Tag, ThreadId,
};
use store::{Collection, DocumentId, Integer, JMAPIdPrefix, LongInteger, StoreError};

use crate::mail::MessageField;

use super::parse::{
    empty_text_mime_headers, header_to_jmap_address, header_to_jmap_date, header_to_jmap_id,
    header_to_jmap_text, header_to_jmap_url, mime_header_to_jmap, mime_parts_to_jmap,
    MessageParser,
};
use super::{
    MailHeaderForm, MailHeaderProperty, MailProperty, MessageData, MessageOutline, MimePart,
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
    pub received_at: Option<i64>,
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
                .download_blob(self.account_id, &item.blob_id, get_message_blob)?
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
        received_at: Option<i64>,
    ) -> jmap::Result<MailImportResult>;

    fn mail_parse(
        &self,
        document: &mut Document,
        blob_id: BlobId,
        raw_message: &[u8],
        received_at: Option<i64>,
    ) -> store::Result<(Vec<String>, String)>;

    fn mail_set_thread(
        &self,
        batch: &mut WriteBatch,
        document: &mut Document,
        reference_ids: Vec<String>,
        thread_name: String,
    ) -> store::Result<DocumentId>;

    fn mail_merge_threads(
        &self,
        documents: &mut WriteBatch,
        thread_ids: Vec<ThreadId>,
    ) -> store::Result<ThreadId>;

    #[allow(clippy::too_many_arguments)]
    fn raft_update_mail(
        &self,
        batch: &mut WriteBatch,
        account_id: AccountId,
        document_id: DocumentId,
        thread_id: DocumentId,
        mailbox_ids: HashSet<Tag>,
        keywords: HashSet<Tag>,
        insert: Option<(Vec<u8>, i64)>,
    ) -> store::Result<()>;
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
        received_at: Option<i64>,
    ) -> jmap::Result<MailImportResult> {
        let document_id = self.assign_document_id(account_id, Collection::Mail)?;
        let mut batch = WriteBatch::new(account_id, self.config.is_in_cluster);
        let mut document = Document::new(Collection::Mail, document_id);
        let size = blob.len();

        // Parse message
        let jmap_blob: JMAPBlob = (&blob_id).into();
        let (reference_ids, thread_name) =
            self.mail_parse(&mut document, blob_id, blob, received_at)?;

        // Add keyword tags
        for keyword in keywords {
            document.tag(MessageField::Keyword, keyword, IndexOptions::new());
        }

        // Add mailbox tags
        for mailbox_id in mailbox_ids {
            batch.log_child_update(Collection::Mailbox, mailbox_id);
            document.tag(
                MessageField::Mailbox,
                Tag::Id(mailbox_id),
                IndexOptions::new(),
            );
        }

        // Lock account while threads are merged
        let _lock = self.lock_account(batch.account_id, Collection::Mail);

        // Obtain thread Id
        let thread_id =
            self.mail_set_thread(&mut batch, &mut document, reference_ids, thread_name)?;

        // Write document to store
        let result = MailImportResult {
            id: JMAPId::from_parts(thread_id, document_id),
            blob_id: jmap_blob,
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
        received_at: Option<i64>,
    ) -> store::Result<(Vec<String>, String)> {
        let message = Message::parse(&raw_message).ok_or_else(|| {
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
            received_at: received_at.unwrap_or_else(|| Utc::now().timestamp()),
        };
        let mut has_attachments = false;

        message_data
            .properties
            .insert(MailProperty::Size, message.raw_message.len().into());

        document.number(
            MessageField::Size,
            message.raw_message.len() as Integer,
            IndexOptions::new().sort(),
        );

        message_data.properties.insert(
            MailProperty::ReceivedAt,
            if let LocalResult::Single(received_at) =
                Utc.timestamp_opt(message_outline.received_at, 0)
            {
                JSONValue::String(received_at.to_rfc3339_opts(SecondsFormat::Secs, true))
            } else {
                JSONValue::Null
            },
        );

        document.number(
            MessageField::ReceivedAt,
            message_outline.received_at as LongInteger,
            IndexOptions::new().sort(),
        );

        let mut reference_ids = Vec::new();
        let mut mime_parts = HashMap::with_capacity(5);
        let mut base_subject = None;

        for (header_name, header_value) in message.headers_rfc {
            // Add headers to document
            document.parse_header(header_name, &header_value);

            // Tag header
            document.tag(
                MessageField::HasHeader,
                Tag::Static(header_name.into()),
                IndexOptions::new(),
            );

            // Build JMAP headers
            match header_name {
                RfcHeader::MessageId
                | RfcHeader::InReplyTo
                | RfcHeader::References
                | RfcHeader::ResentMessageId => {
                    // Build a list containing all IDs that appear in the header
                    match &header_value {
                        HeaderValue::Text(text) if text.len() <= MAX_ID_LENGTH => {
                            reference_ids.push(text.to_string())
                        }
                        HeaderValue::TextList(list) => {
                            reference_ids.extend(list.iter().filter_map(|text| {
                                if text.len() <= MAX_ID_LENGTH {
                                    Some(text.to_string())
                                } else {
                                    None
                                }
                            }));
                        }
                        HeaderValue::Collection(col) => {
                            for item in col {
                                match item {
                                    HeaderValue::Text(text) if text.len() <= MAX_ID_LENGTH => {
                                        reference_ids.push(text.to_string())
                                    }
                                    HeaderValue::TextList(list) => {
                                        reference_ids.extend(list.iter().filter_map(|text| {
                                            if text.len() <= MAX_ID_LENGTH {
                                                Some(text.to_string())
                                            } else {
                                                None
                                            }
                                        }))
                                    }
                                    _ => (),
                                }
                            }
                        }
                        _ => (),
                    }
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
                RfcHeader::From | RfcHeader::To | RfcHeader::Cc | RfcHeader::Bcc => {
                    // Build sort index
                    document.add_addr_sort(header_name, &header_value);
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
                RfcHeader::ReplyTo
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
                RfcHeader::Subject => {
                    if let HeaderValue::Text(subject) = &header_value {
                        let thread_name = thread_name(subject);

                        document.text(
                            RfcHeader::Subject,
                            subject.to_string(),
                            Language::Unknown,
                            IndexOptions::new().full_text(0),
                        );

                        base_subject = Some(if !thread_name.is_empty() {
                            thread_name.to_string()
                        } else {
                            "!".to_string()
                        });
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
                RfcHeader::Comments | RfcHeader::Keywords | RfcHeader::ListId => {
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
                        field.clone(),
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
                            document.parse_attached_message(&mut message);
                            (
                                self.blob_store(message.raw_message.as_ref())?,
                                message.raw_message.len(),
                            )
                        }
                        MessageAttachment::Raw(raw_message) => {
                            if let Some(message) = &mut Message::parse(raw_message.as_ref()) {
                                document.parse_attached_message(message);
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

        if !extra_mime_parts.is_empty() {
            message_data.mime_parts.append(&mut extra_mime_parts);
        }

        message_data
            .properties
            .insert(MailProperty::HasAttachment, has_attachments.into());

        if has_attachments {
            document.tag(MessageField::Attachment, Tag::Default, IndexOptions::new());
        }

        let mut message_data = message_data
            .serialize()
            .ok_or_else(|| StoreError::SerializeError("Failed to serialize message data".into()))?;
        let mut message_outline = message_outline.serialize().ok_or_else(|| {
            StoreError::SerializeError("Failed to serialize message outline".into())
        })?;
        let mut buf = Vec::with_capacity(
            message_data.len() + message_outline.len() + std::mem::size_of::<usize>(),
        );
        message_data.len().to_leb128_bytes(&mut buf);
        buf.append(&mut message_data);
        buf.append(&mut message_outline);

        let blob_id = self.blob_store(&buf)?;

        // TODO search by "header exists"
        // TODO use content language when available
        // TODO index PDF, Doc, Excel, etc.

        Ok((
            reference_ids,
            base_subject.unwrap_or_else(|| "!".to_string()),
        ))
    }

    fn mail_set_thread(
        &self,
        batch: &mut WriteBatch,
        document: &mut Document,
        reference_ids: Vec<String>,
        thread_name: String,
    ) -> store::Result<DocumentId> {
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
                                FieldValue::Keyword(thread_name.to_string()),
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
                .filter_map(|id: Option<DocumentId>| Some(id?))
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

        for reference_id in reference_ids {
            document.text(
                MessageField::MessageIdRef,
                reference_id,
                Language::Unknown,
                IndexOptions::new().keyword(),
            );
        }

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

        document.text(
            MessageField::ThreadName,
            thread_name,
            Language::Unknown,
            IndexOptions::new().keyword().sort(),
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

    fn raft_update_mail(
        &self,
        batch: &mut WriteBatch,
        account_id: AccountId,
        document_id: DocumentId,
        thread_id: DocumentId,
        mailboxes: HashSet<Tag>,
        keywords: HashSet<Tag>,
        insert: Option<(Vec<u8>, i64)>,
    ) -> store::Result<()> {
        /*if let Some((raw_message, received_at)) = insert {
            let mut document = Document::new(Collection::Mail, document_id);

            // Parse and build message document
            let (reference_ids, thread_name) =
                document.parse_message(raw_message, vec![], vec![], received_at.into())?;

            for mailbox in mailboxes {
                document.tag(MessageField::Mailbox, mailbox, IndexOptions::new());
            }

            for keyword in keywords {
                document.tag(MessageField::Keyword, keyword, IndexOptions::new());
            }

            for reference_id in reference_ids {
                document.text(
                    MessageField::MessageIdRef,
                    Text::keyword(reference_id),
                    IndexOptions::new(),
                );
            }

            // Add thread id and name
            document.tag(
                MessageField::ThreadId,
                Tag::Id(thread_id),
                IndexOptions::new(),
            );
            document.text(
                MessageField::ThreadName,
                Text::keyword(thread_name),
                IndexOptions::new().sort(),
            );

            batch.insert_document(document);
        } else {
            let mut document = Document::new(Collection::Mail, document_id);

            // Process mailbox changes
            if let Some(current_mailboxes) = self.get_document_tags(
                account_id,
                Collection::Mail,
                document_id,
                MessageField::Mailbox.into(),
            )? {
                if current_mailboxes.items != mailboxes {
                    for current_mailbox in &current_mailboxes.items {
                        if !mailboxes.contains(current_mailbox) {
                            document.tag(
                                MessageField::Mailbox,
                                current_mailbox.clone(),
                                IndexOptions::new().clear(),
                            );
                        }
                    }

                    for mailbox in mailboxes {
                        if !current_mailboxes.contains(&mailbox) {
                            document.tag(MessageField::Mailbox, mailbox, IndexOptions::new());
                        }
                    }
                }
            } else {
                debug!(
                    "Raft update failed: No mailbox tags found for message {}.",
                    document_id
                );
                return Ok(());
            };

            // Process keyword changes
            let current_keywords = if let Some(current_keywords) = self.get_document_tags(
                account_id,
                Collection::Mail,
                document_id,
                MessageField::Keyword.into(),
            )? {
                current_keywords.items
            } else {
                HashSet::new()
            };
            if current_keywords != keywords {
                for current_keyword in &current_keywords {
                    if !keywords.contains(current_keyword) {
                        document.tag(
                            MessageField::Keyword,
                            current_keyword.clone(),
                            IndexOptions::new().clear(),
                        );
                    }
                }

                for keyword in keywords {
                    if !current_keywords.contains(&keyword) {
                        document.tag(MessageField::Keyword, keyword, IndexOptions::new());
                    }
                }
            }

            // Handle thread id changes
            if let Some(current_thread_id) = self.get_document_tag_id(
                account_id,
                Collection::Mail,
                document_id,
                MessageField::ThreadId.into(),
            )? {
                if thread_id != current_thread_id {
                    document.tag(
                        MessageField::ThreadId,
                        Tag::Id(thread_id),
                        IndexOptions::new(),
                    );
                    document.tag(
                        MessageField::ThreadId,
                        Tag::Id(current_thread_id),
                        IndexOptions::new().clear(),
                    );
                }
            } else {
                debug!(
                    "Raft update failed: No thread id found for message {}.",
                    document_id
                );
                return Ok(());
            };

            if !document.is_empty() {
                batch.update_document(document);
            }
        }*/
        Ok(())
    }
}
