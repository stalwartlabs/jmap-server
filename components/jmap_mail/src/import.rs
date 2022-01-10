use std::{borrow::Cow, collections::HashSet};

use crate::JMAPMailIdImpl;
use jmap_store::{JMAPId, JMAP_MAIL, JMAP_THREAD};
use mail_parser::Message;
use serde::{Deserialize, Serialize};
use store::{
    batch::DocumentWriter,
    field::{FieldOptions, Text},
    AccountId, ChangeLogId, Comparator, DocumentSet, FieldValue, Filter, Store, StoreError, Tag,
    ThreadId, UncommittedDocumentId,
};

use crate::{parse::build_message_document, query::MailboxId, MessageField};

pub fn bincode_serialize<T>(value: &T) -> store::Result<Vec<u8>>
where
    T: Serialize,
{
    bincode::serialize(value).map_err(|e| StoreError::SerializeError(e.to_string()))
}

pub fn bincode_deserialize<'x, T>(bytes: &'x [u8]) -> store::Result<T>
where
    T: Deserialize<'x>,
{
    bincode::deserialize(bytes).map_err(|e| StoreError::DeserializeError(e.to_string()))
}

pub struct JMAPMailImport<'x> {
    pub account_id: AccountId,
    pub if_in_state: Option<Cow<'x, str>>,
    pub emails: Vec<JMAPMailImportItem<'x>>,
}

impl<'x> JMAPMailImport<'x> {
    pub fn new(account_id: AccountId, blob: Cow<'x, [u8]>) -> Self {
        Self {
            account_id,
            if_in_state: None,
            emails: vec![JMAPMailImportItem::new(blob)],
        }
    }
}

pub struct JMAPMailImportItem<'x> {
    pub blob: Cow<'x, [u8]>,
    pub mailbox_ids: Vec<MailboxId>,
    pub keywords: Vec<Tag<'x>>,
    pub received_at: Option<i64>,
}

impl<'x> JMAPMailImportItem<'x> {
    pub fn new(blob: Cow<'x, [u8]>) -> Self {
        Self {
            blob,
            mailbox_ids: vec![],
            keywords: vec![],
            received_at: None,
        }
    }
}

pub struct JMAPMailImportResponse {
    pub account_id: AccountId,
    pub old_state: Option<String>,
    pub new_state: String,
    pub results: Vec<store::Result<JMAPId>>,
}

pub trait JMAPMailLocalStoreImport<'x>: Store<'x> {
    fn mail_import_single(
        &'x self,
        account: AccountId,
        message: JMAPMailImportItem<'x>,
    ) -> jmap_store::Result<JMAPId> {
        // Build message document
        let (mut batch, document_id) = {
            let assigned_id = self.assign_document_id(account, JMAP_MAIL, None)?;
            let document_id = assigned_id.get_document_id();
            (DocumentWriter::insert(JMAP_MAIL, assigned_id), document_id)
        };
        let (reference_ids, thread_name) = build_message_document(
            &mut batch,
            Message::parse(message.blob.as_ref()).ok_or(StoreError::ParseError)?,
            message.received_at,
        )?;
        let mut batches = Vec::new();

        // Add mailbox tags
        if !message.mailbox_ids.is_empty() {
            //TODO validate mailbox ids
            batch.add_binary(
                MessageField::Mailbox.into(),
                bincode_serialize(&message.mailbox_ids)?.into(),
                FieldOptions::Store,
            );
            for mailbox_id in message.mailbox_ids {
                batch.set_tag(MessageField::Mailbox.into(), Tag::Id(mailbox_id));
            }
        }

        // Add keyword tags
        if !message.keywords.is_empty() {
            batch.add_binary(
                MessageField::Keyword.into(),
                bincode_serialize(&message.keywords)?.into(),
                FieldOptions::Store,
            );
            for keyword in message.keywords {
                batch.set_tag(MessageField::Keyword.into(), keyword);
            }
        }

        // Lock account
        let _collection_lock = self.lock_collection(account, JMAP_MAIL)?;

        // Obtain thread id
        let thread_id = if !reference_ids.is_empty() {
            // Obtain thread ids for all matching document ids
            let thread_ids = self
                .get_multi_document_value(
                    account,
                    JMAP_MAIL,
                    self.query(
                        account,
                        JMAP_MAIL,
                        Filter::and(vec![
                            Filter::eq(
                                MessageField::ThreadName.into(),
                                FieldValue::Keyword((&thread_name).into()),
                            ),
                            Filter::or(
                                reference_ids
                                    .iter()
                                    .map(|id| {
                                        Filter::eq(
                                            MessageField::MessageIdRef.into(),
                                            FieldValue::Keyword(id.as_ref().into()),
                                        )
                                    })
                                    .collect(),
                            ),
                        ]),
                        Comparator::None,
                    )?,
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
                        account,
                        &mut batches,
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
            let thread_id = self.assign_document_id(account, JMAP_THREAD, None)?;
            batches.push(DocumentWriter::insert(JMAP_THREAD, thread_id.clone()));
            thread_id.get_document_id()
        };

        for reference_id in reference_ids {
            batch.add_text(
                MessageField::MessageIdRef.into(),
                Text::Keyword(reference_id),
                FieldOptions::None,
            );
        }

        batch.add_integer(
            MessageField::ThreadId.into(),
            thread_id,
            FieldOptions::Store,
        );
        batch.set_tag(MessageField::ThreadId.into(), Tag::Id(thread_id));

        batch.add_text(
            MessageField::ThreadName.into(),
            Text::Keyword(thread_name.into()),
            FieldOptions::Sort,
        );

        let jmap_mail_id = JMAPId::from_email(thread_id, document_id);
        batch.log_insert(jmap_mail_id);
        batches.push(batch);

        // Write batches to store
        self.update_documents(account, batches, None)?;

        Ok(jmap_mail_id)
    }

    fn mail_merge_threads(
        &self,
        account: AccountId,
        batches: &mut Vec<DocumentWriter<impl UncommittedDocumentId>>,
        thread_ids: Vec<ThreadId>,
    ) -> store::Result<ThreadId> {
        // Query tags for all thread ids
        let mut document_sets = Vec::with_capacity(thread_ids.len());

        for (pos, document_set) in self
            .get_tags(
                account,
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
                debug_assert!(document_set.len() > 0);
                document_sets.push((document_set, thread_ids[pos]));
            } else {
                // TODO log this error instead
                debug_assert!(false, "No tags found for thread id {}.", thread_ids[pos]);
            }
        }

        document_sets.sort_unstable_by_key(|i| i.0.len());

        let mut document_sets = document_sets.into_iter().rev();
        let mut deleted_threads = DocumentWriter::delete_many(JMAP_MAIL);
        let thread_id = document_sets.next().unwrap().1;

        for (document_set, delete_thread_id) in document_sets {
            for document_id in document_set {
                let mut batch = DocumentWriter::update(JMAP_MAIL, document_id);
                batch.add_integer(MessageField::ThreadId.into(), thread_id, FieldOptions::Sort);
                batch.set_tag(MessageField::ThreadId.into(), Tag::Id(thread_id));
                batch.log_move(
                    JMAPId::from_email(delete_thread_id, document_id),
                    JMAPId::from_email(thread_id, document_id),
                );
                batches.push(batch);
            }
            deleted_threads.set_tag(MessageField::ThreadId.into(), Tag::Id(delete_thread_id));

            let mut delete_thread = DocumentWriter::delete(JMAP_THREAD, delete_thread_id);
            delete_thread.log_delete(delete_thread_id as ChangeLogId);
            batches.push(delete_thread);
        }

        if !deleted_threads.is_empty() {
            batches.push(deleted_threads);
        }

        Ok(thread_id)
    }
}
