use std::collections::{HashMap, HashSet};

use crate::mail::Keyword;
use crate::mail::{parse::get_message_blob, MESSAGE_RAW};

use jmap::error::method::MethodError;
use jmap::error::set::{SetError, SetErrorType};
use jmap::id::blob::BlobId;
use jmap::id::JMAPIdSerialize;
use jmap::jmap_store::blob::JMAPBlobStore;
use jmap::jmap_store::import::ImportObject;
use jmap::jmap_store::set::CreateItemResult;
use jmap::protocol::json::JSONValue;
use jmap::request::import::ImportRequest;
use store::batch::Document;
use store::field::{DefaultOptions, Options};
use store::query::DefaultIdMapper;
use store::tracing::debug;
use store::tracing::log::error;
use store::{
    batch::WriteBatch, field::Text, roaring::RoaringBitmap, AccountId, Comparator, FieldValue,
    Filter, JMAPId, JMAPStore, Store, Tag, ThreadId,
};
use store::{Collection, DocumentId, JMAPIdPrefix};

use crate::mail::MessageField;

use super::parse::MessageParser;

pub struct ImportMail<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub store: &'y JMAPStore<T>,
    pub account_id: AccountId,
    pub mailbox_ids: RoaringBitmap,
}
pub struct ImportItem {
    pub blob_id: BlobId,
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
                    .parse_blob_id(false)?
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
                    blob,
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
        blob: Vec<u8>,
        mailbox_ids: Vec<DocumentId>,
        keywords: Vec<Tag>,
        received_at: Option<i64>,
    ) -> jmap::Result<MailImportResult>;

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
    pub blob_id: BlobId,
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
        blob: Vec<u8>,
        mailbox_ids: Vec<DocumentId>,
        keywords: Vec<Tag>,
        received_at: Option<i64>,
    ) -> jmap::Result<MailImportResult> {
        let document_id = self.assign_document_id(account_id, Collection::Mail)?;
        let mut batch = WriteBatch::new(account_id, self.config.is_in_cluster);
        let mut document = Document::new(Collection::Mail, document_id);
        let size = blob.len();

        // Add mailbox tags
        for mailbox_id in &mailbox_ids {
            batch.log_child_update(Collection::Mailbox, *mailbox_id);
        }

        // Parse message
        let (reference_ids, thread_name) =
            document.parse_message(blob, mailbox_ids, keywords, received_at)?;

        // Lock account while threads are merged
        let _lock = self.lock_account(batch.account_id, Collection::Mail);

        // Obtain thread Id
        let thread_id =
            self.mail_set_thread(&mut batch, &mut document, reference_ids, thread_name)?;

        // Write document to store
        let result = MailImportResult {
            id: JMAPId::from_parts(thread_id, document_id),
            blob_id: BlobId::new_owned(
                batch.account_id,
                Collection::Mail,
                document.document_id,
                MESSAGE_RAW,
            ),
            thread_id,
            size,
        };
        batch.log_insert(Collection::Mail, result.id);
        batch.insert_document(document);
        self.write(batch)?;

        Ok(result)
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
                .get_multi_document_tag_id(
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
                .filter_map(|id| Some(*id?))
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
                Text::keyword(reference_id),
                DefaultOptions::new(),
            );
        }

        document.tag(
            MessageField::ThreadId,
            Tag::Id(thread_id),
            DefaultOptions::new(),
        );

        document.text(
            MessageField::ThreadName,
            Text::keyword(thread_name),
            DefaultOptions::new().sort(),
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
                    DefaultOptions::new(),
                );
                document.tag(
                    MessageField::ThreadId,
                    Tag::Id(delete_thread_id),
                    DefaultOptions::new().clear(),
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
        if let Some((raw_message, received_at)) = insert {
            let mut document = Document::new(Collection::Mail, document_id);

            // Parse and build message document
            let (reference_ids, thread_name) =
                document.parse_message(raw_message, vec![], vec![], received_at.into())?;

            for mailbox in mailboxes {
                document.tag(MessageField::Mailbox, mailbox, DefaultOptions::new());
            }

            for keyword in keywords {
                document.tag(MessageField::Keyword, keyword, DefaultOptions::new());
            }

            for reference_id in reference_ids {
                document.text(
                    MessageField::MessageIdRef,
                    Text::keyword(reference_id),
                    DefaultOptions::new(),
                );
            }

            // Add thread id and name
            document.tag(
                MessageField::ThreadId,
                Tag::Id(thread_id),
                DefaultOptions::new(),
            );
            document.text(
                MessageField::ThreadName,
                Text::keyword(thread_name),
                DefaultOptions::new().sort(),
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
                                DefaultOptions::new().clear(),
                            );
                        }
                    }

                    for mailbox in mailboxes {
                        if !current_mailboxes.contains(&mailbox) {
                            document.tag(MessageField::Mailbox, mailbox, DefaultOptions::new());
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
                            DefaultOptions::new().clear(),
                        );
                    }
                }

                for keyword in keywords {
                    if !current_keywords.contains(&keyword) {
                        document.tag(MessageField::Keyword, keyword, DefaultOptions::new());
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
                        DefaultOptions::new(),
                    );
                    document.tag(
                        MessageField::ThreadId,
                        Tag::Id(current_thread_id),
                        DefaultOptions::new().clear(),
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
        }
        Ok(())
    }
}
