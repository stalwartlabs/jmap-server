use std::{borrow::Cow, collections::HashSet};

use jmap_store::{local_store::JMAPLocalStore, JMAP_MAIL};
use mail_parser::Message;
use store::{
    batch::WriteOperation, field::Text, AccountId, Comparator, DocumentId, DocumentSet, FieldValue,
    Filter, Store, StoreError, Tag, ThreadId,
};

use crate::{
    parse::build_message_document, query::MailboxId, JMAPMailId, JMAPMailStoreImport, MessageField,
};

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
    pub keywords: Vec<Cow<'x, str>>,
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
    pub results: Vec<store::Result<JMAPMailId>>,
}

impl<'x, T> JMAPMailStoreImport<'x> for JMAPLocalStore<T>
where
    T: Store<'x>,
{
    fn mail_import_single(
        &'x self,
        account: AccountId,
        message: JMAPMailImportItem<'x>,
    ) -> store::Result<JMAPMailId> {
        // Build message document
        let (mut batch, reference_ids, thread_name) = build_message_document(
            account,
            Message::parse(message.blob.as_ref()).ok_or(StoreError::ParseError)?,
            message.received_at,
        )?;
        let mut batches = Vec::new();

        // Add mailbox tags
        for mailbox_id in message.mailbox_ids {
            batch.add_tag(MessageField::Mailbox.into(), Tag::Id(mailbox_id));
        }

        // Add keyword tags
        for keyword in message.keywords {
            batch.add_tag(MessageField::Keyword.into(), Tag::Text(keyword));
        }

        // Lock account
        let _account_lock = self.lock_account(account)?;

        // Obtain thread id
        let thread_id = if !reference_ids.is_empty() {
            // Query all document ids containing the reference ids
            let message_doc_ids = self
                .store
                .query(
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
                )?
                .collect::<Vec<DocumentId>>();

            // Obtain thread ids for all matching document ids
            if !message_doc_ids.is_empty() {
                let thread_ids = self
                    .store
                    .get_multi_document_value(
                        account,
                        JMAP_MAIL,
                        &message_doc_ids,
                        MessageField::ThreadId.into(),
                    )?
                    .into_iter()
                    .flatten()
                    .collect::<HashSet<ThreadId>>();

                if thread_ids.len() > 1 {
                    // Merge all matching threads
                    Some(self.mail_merge_threads(
                        account,
                        &mut batches,
                        thread_ids.into_iter().collect(),
                    )?)
                } else {
                    // There was just one match, use it as the thread id
                    thread_ids.into_iter().next()
                }
            } else {
                None
            }
        } else {
            None
        };

        let thread_id = if let Some(thread_id) = thread_id {
            thread_id
        } else {
            let thread_id: ThreadId = if let Some(thread_id) = self.store.get_value::<ThreadId>(
                account.into(),
                JMAP_MAIL.into(),
                Some(MessageField::ThreadId.into()),
            )? {
                thread_id + 1
            } else {
                0
            };

            let mut thread_op = WriteOperation::update_collection(account, JMAP_MAIL);
            thread_op.add_long_int(MessageField::ThreadId.into(), 0, thread_id, true, false);
            batches.push(thread_op);
            thread_id
        };

        for reference_id in reference_ids {
            batch.add_text(
                MessageField::MessageIdRef.into(),
                0,
                Text::Keyword(reference_id),
                false,
                false,
            );
        }

        batch.add_long_int(MessageField::ThreadId.into(), 0, thread_id, true, false);
        batch.add_tag(MessageField::ThreadId.into(), Tag::Id(thread_id));

        batch.add_text(
            MessageField::ThreadName.into(),
            0,
            Text::Keyword(thread_name.into()),
            false,
            true,
        );
        batches.push(batch);

        // Write batches to store
        self.store.update_bulk(batches)?;

        //TODO implement doc id
        Ok(JMAPMailId::new(thread_id, 0))
    }
}

trait JMAPMailStoreThreadMerge {
    fn mail_merge_threads(
        &self,
        account: AccountId,
        batches: &mut Vec<WriteOperation>,
        thread_ids: Vec<ThreadId>,
    ) -> store::Result<ThreadId>;
}

impl<'x, T> JMAPMailStoreThreadMerge for JMAPLocalStore<T>
where
    T: Store<'x>,
{
    fn mail_merge_threads(
        &self,
        account: AccountId,
        batches: &mut Vec<WriteOperation>,
        thread_ids: Vec<ThreadId>,
    ) -> store::Result<ThreadId> {
        // Query tags for all thread ids
        let mut document_sets = Vec::with_capacity(thread_ids.len());

        for (pos, document_set) in self
            .store
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
        let thread_id = if let Some((_, thread_id)) = document_sets.next() {
            thread_id
        } else {
            thread_ids[0]
        };

        let mut deleted_threads = WriteOperation::delete_collection(account, JMAP_MAIL);

        for (document_set, delete_thread_id) in document_sets {
            for document_id in document_set {
                let mut batch = WriteOperation::update_document(account, JMAP_MAIL, document_id);
                batch.add_long_int(MessageField::ThreadId.into(), 0, thread_id, true, false);
                batch.add_tag(MessageField::ThreadId.into(), Tag::Id(thread_id));
                batches.push(batch);
            }
            deleted_threads.add_tag(MessageField::ThreadId.into(), Tag::Id(delete_thread_id));
        }

        if !deleted_threads.is_empty() {
            batches.push(deleted_threads);
        }

        Ok(thread_id)
    }
}
