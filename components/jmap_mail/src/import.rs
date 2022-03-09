use std::collections::{HashMap, HashSet};

use crate::{parse::get_message_blob, MESSAGE_RAW};
use jmap_store::blob::JMAPBlobStore;
use jmap_store::{
    changes::{JMAPChanges, JMAPState},
    id::{BlobId, JMAPIdSerialize},
    json::JSONValue,
    JMAPError, JMAP_MAIL, JMAP_MAILBOX, JMAP_MAILBOX_CHANGES, JMAP_THREAD,
};
use mail_parser::Message;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use store::query::{JMAPIdMapFnc, JMAPStoreQuery};
use store::serialize::{StoreDeserialize, StoreSerialize};
use store::JMAPIdPrefix;
use store::{
    batch::WriteBatch,
    bincode,
    changelog::RaftId,
    field::{FieldOptions, Text},
    roaring::RoaringBitmap,
    AccountId, Comparator, FieldValue, Filter, JMAPId, JMAPStore, Store, StoreError, Tag, ThreadId,
};

use crate::{parse::build_message_document, query::MailboxId, MessageField};

#[derive(Serialize, Deserialize)]
pub struct Bincoded<T>
where
    T: Send + Sync,
{
    pub items: T,
}

impl<T> Bincoded<T>
where
    T: Send + Sync,
{
    pub fn new(items: T) -> Self {
        Self { items }
    }
}

impl<T> StoreSerialize for Bincoded<T>
where
    T: Serialize + Send + Sync,
{
    fn serialize(&self) -> Option<Vec<u8>> {
        bincode::serialize(&self).ok()
    }
}

impl<T> StoreDeserialize for Bincoded<T>
where
    T: DeserializeOwned + Send + Sync,
{
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize_from(bytes).ok()
    }
}

pub struct JMAPMailImportRequest {
    pub account_id: AccountId,
    pub if_in_state: Option<JMAPState>,
    pub emails: Vec<JMAPMailImportItem>,
}
pub struct JMAPMailImportItem {
    pub id: String,
    pub blob_id: BlobId,
    pub mailbox_ids: Vec<MailboxId>,
    pub keywords: Vec<Tag>,
    pub received_at: Option<i64>,
}

pub struct JMAPMailImportResponse {
    pub old_state: JMAPState,
    pub new_state: JMAPState,
    pub created: JSONValue,
    pub not_created: JSONValue,
}

impl From<JMAPMailImportResponse> for JSONValue {
    fn from(value: JMAPMailImportResponse) -> Self {
        let mut obj = HashMap::new();
        obj.insert("oldState".to_string(), value.old_state.into());
        obj.insert("newState".to_string(), value.new_state.into());
        obj.insert("created".to_string(), value.created);
        obj.insert("notCreated".to_string(), value.not_created);
        obj.into()
    }
}

pub trait JMAPMailImport {
    fn mail_import(
        &self,
        request: JMAPMailImportRequest,
    ) -> jmap_store::Result<JMAPMailImportResponse>;
}

impl<T> JMAPMailImport for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_import(
        &self,
        request: JMAPMailImportRequest,
    ) -> jmap_store::Result<JMAPMailImportResponse> {
        let old_state = self.get_state(request.account_id, JMAP_MAIL)?;
        if let Some(if_in_state) = request.if_in_state {
            if old_state != if_in_state {
                return Err(JMAPError::StateMismatch);
            }
        }

        if request.emails.len() > self.config.mail_import_max_items {
            return Err(JMAPError::RequestTooLarge);
        }

        let mailbox_ids = self
            .get_document_ids(request.account_id, JMAP_MAILBOX)?
            .unwrap_or_else(RoaringBitmap::new);

        let mut created = HashMap::with_capacity(request.emails.len());
        let mut not_created = HashMap::with_capacity(request.emails.len());

        'main: for item in request.emails {
            if item.mailbox_ids.is_empty() {
                not_created.insert(
                    item.id,
                    JSONValue::new_invalid_property(
                        "mailboxIds",
                        "Message must belong to at least one mailbox.",
                    ),
                );
                continue 'main;
            }
            for &mailbox_id in &item.mailbox_ids {
                if !mailbox_ids.contains(mailbox_id) {
                    not_created.insert(
                        item.id,
                        JSONValue::new_invalid_property(
                            "mailboxIds",
                            format!(
                                "Mailbox {} does not exist.",
                                (mailbox_id as JMAPId).to_jmap_string()
                            ),
                        ),
                    );
                    continue 'main;
                }
            }

            if let Some(blob) =
                self.download_blob(request.account_id, &item.blob_id, get_message_blob)?
            {
                created.insert(
                    item.id,
                    self.mail_import_blob(
                        request.account_id,
                        self.next_raft_id(),
                        &blob,
                        item.mailbox_ids,
                        item.keywords,
                        item.received_at,
                    )?,
                );
            } else {
                not_created.insert(
                    item.id,
                    JSONValue::new_invalid_property(
                        "blobId",
                        format!("BlobId {} not found.", item.blob_id.to_jmap_string()),
                    ),
                );
            }
        }

        Ok(JMAPMailImportResponse {
            new_state: if !created.is_empty() {
                self.get_state(request.account_id, JMAP_MAIL)?
            } else {
                old_state.clone()
            },
            old_state,
            created: if !created.is_empty() {
                created.into()
            } else {
                JSONValue::Null
            },
            not_created: if !not_created.is_empty() {
                not_created.into()
            } else {
                JSONValue::Null
            },
        })
    }
}

pub trait JMAPMailLocalStoreImport {
    fn mail_import_blob(
        &self,
        account_id: AccountId,
        raft_id: RaftId,
        blob: &[u8],
        mailbox_ids: Vec<MailboxId>,
        keywords: Vec<Tag>,
        received_at: Option<i64>,
    ) -> jmap_store::Result<JSONValue>;

    fn mail_merge_threads(
        &self,
        account_id: AccountId,
        documents: &mut Vec<WriteBatch>,
        thread_ids: Vec<ThreadId>,
    ) -> store::Result<ThreadId>;
}

impl<T> JMAPMailLocalStoreImport for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_import_blob(
        &self,
        account_id: AccountId,
        raft_id: RaftId,
        blob: &[u8],
        mailbox_ids: Vec<MailboxId>,
        keywords: Vec<Tag>,
        received_at: Option<i64>,
    ) -> jmap_store::Result<JSONValue> {
        // Build message document
        let document_id = self.assign_document_id(account_id, JMAP_MAIL)?;
        let mut document = WriteBatch::insert(JMAP_MAIL, document_id, document_id);
        let (reference_ids, thread_name) = build_message_document(
            &mut document,
            Message::parse(blob).ok_or(StoreError::ParseError)?,
            received_at,
        )?;
        let mut documents = Vec::new();

        // Add mailbox tags
        if !mailbox_ids.is_empty() {
            //TODO validate mailbox ids
            let mailbox_ids = Bincoded::new(mailbox_ids);
            document.binary(
                MessageField::Mailbox,
                store::serialize::StoreSerialize::serialize(&mailbox_ids).ok_or_else(|| {
                    StoreError::SerializeError("Failed to serialize mailbox list.".into())
                })?,
                FieldOptions::Store,
            );
            let change_id = self.assign_change_id(account_id, JMAP_MAILBOX)?;
            for mailbox_id in mailbox_ids.items {
                document.tag(
                    MessageField::Mailbox,
                    Tag::Id(mailbox_id),
                    FieldOptions::None,
                );
                documents.push(
                    WriteBatch::update(JMAP_MAILBOX, mailbox_id, mailbox_id).log_with_id(change_id),
                );
                documents.push(
                    WriteBatch::update(JMAP_MAILBOX_CHANGES, mailbox_id, mailbox_id)
                        .log_with_id(change_id),
                );
            }
        }

        // Add keyword tags
        if !keywords.is_empty() {
            let keywords = Bincoded::new(keywords);
            document.binary(
                MessageField::Keyword,
                store::serialize::StoreSerialize::serialize(&keywords).ok_or_else(|| {
                    StoreError::SerializeError("Failed to serialize keywords.".into())
                })?,
                FieldOptions::Store,
            );
            for keyword in keywords.items {
                document.tag(MessageField::Keyword, keyword, FieldOptions::None);
            }
        }

        // Lock account
        let _lock = self.lock_account(account_id, JMAP_MAIL);

        // Obtain thread id
        let thread_id = if !reference_ids.is_empty() {
            // Obtain thread ids for all matching document ids
            let thread_ids = self
                .get_multi_document_value(
                    account_id,
                    JMAP_MAIL,
                    self.query::<JMAPIdMapFnc>(JMAPStoreQuery::new(
                        account_id,
                        JMAP_MAIL,
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
                    ))?
                    .into_iter()
                    .map(|id| id.get_document_id())
                    .collect::<Vec<u32>>()
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
                    Some(self.mail_merge_threads(
                        account_id,
                        &mut documents,
                        thread_ids.into_iter().collect(),
                    )?)
                }
            }
        } else {
            None
        };

        let thread_id = if let Some(thread_id) = thread_id {
            thread_id
        } else {
            let thread_id = self.assign_document_id(account_id, JMAP_THREAD)?;
            documents.push(WriteBatch::insert(JMAP_THREAD, thread_id, thread_id));
            thread_id
        };

        for reference_id in reference_ids {
            document.text(
                MessageField::MessageIdRef,
                Text::Keyword(reference_id),
                FieldOptions::None,
            );
        }

        document.integer(MessageField::ThreadId, thread_id, FieldOptions::Store);
        document.tag(
            MessageField::ThreadId,
            Tag::Id(thread_id),
            FieldOptions::None,
        );

        document.text(
            MessageField::ThreadName,
            Text::Keyword(thread_name),
            FieldOptions::Sort,
        );

        let jmap_mail_id = JMAPId::from_parts(thread_id, document_id);
        document.update_jmap_id(jmap_mail_id);
        documents.push(document);

        // Write documents to store
        self.update_documents(account_id, raft_id, documents)?;

        // Generate JSON object
        let mut values = HashMap::with_capacity(4);
        values.insert("id".to_string(), jmap_mail_id.to_jmap_string().into());
        values.insert(
            "blobId".to_string(),
            BlobId::new_owned(account_id, JMAP_MAIL, document_id, MESSAGE_RAW)
                .to_jmap_string()
                .into(),
        );
        values.insert(
            "threadId".to_string(),
            (thread_id as JMAPId).to_jmap_string().into(),
        );
        values.insert("size".to_string(), blob.len().into());

        Ok(values.into())
    }

    fn mail_merge_threads(
        &self,
        account_id: AccountId,
        documents: &mut Vec<WriteBatch>,
        thread_ids: Vec<ThreadId>,
    ) -> store::Result<ThreadId> {
        // Query tags for all thread ids
        let mut document_sets = Vec::with_capacity(thread_ids.len());

        for (pos, document_set) in self
            .get_tags(
                account_id,
                JMAP_MAIL,
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
                // TODO log this error instead
                debug_assert!(false, "No tags found for thread id {}.", thread_ids[pos]);
            }
        }

        document_sets.sort_unstable_by_key(|i| i.0.len());

        let mut document_sets = document_sets.into_iter().rev();
        let thread_id = document_sets.next().unwrap().1;

        for (document_set, delete_thread_id) in document_sets {
            for document_id in document_set {
                let mut document = WriteBatch::moved(
                    JMAP_MAIL,
                    document_id,
                    JMAPId::from_parts(delete_thread_id, document_id),
                    JMAPId::from_parts(thread_id, document_id),
                );
                document.integer(MessageField::ThreadId, thread_id, FieldOptions::Store);
                document.tag(
                    MessageField::ThreadId,
                    Tag::Id(thread_id),
                    FieldOptions::None,
                );
                document.tag(
                    MessageField::ThreadId,
                    Tag::Id(delete_thread_id),
                    FieldOptions::Clear,
                );

                documents.push(document);
            }

            documents.push(WriteBatch::delete(
                JMAP_THREAD,
                delete_thread_id,
                delete_thread_id,
            ));
        }

        Ok(thread_id)
    }
}
