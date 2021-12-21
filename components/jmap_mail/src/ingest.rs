use std::collections::HashSet;

use mail_parser::{HeaderName, HeaderValue, Message};
use store::{
    batch::{WriteOperation, MAX_ID_LENGTH},
    field::Text,
    AccountId, ComparisonOperator, DocumentId, FieldValue, Filter, Store, StoreError, Tag,
    ThreadId,
};

use crate::{parse::build_message_document, MessageField, MessageStore, MAIL_CID};

impl<'x, T> MessageStore<'x, T>
where
    T: Store<'x>,
{
    pub fn ingest_message(&self, account: AccountId, raw_message: &[u8]) -> store::Result<()> {
        // Parse raw message
        let mut message = Message::parse(raw_message).ok_or(StoreError::ParseError)?;

        // Obtain the thread name
        let thread_name = message
            .get_thread_name()
            .and_then(|val| {
                let val = val.trim();
                if !val.is_empty() {
                    Some(val.to_lowercase())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "!".to_string());

        // Build a list containing all IDs that appear in the header
        let mut reference_ids = Vec::new();
        for header_name in [
            HeaderName::MessageId,
            HeaderName::InReplyTo,
            HeaderName::References,
            HeaderName::ResentMessageId,
        ] {
            match message
                .headers_rfc
                .remove(&header_name)
                .unwrap_or(HeaderValue::Empty)
            {
                HeaderValue::Text(text) if text.len() <= MAX_ID_LENGTH => reference_ids.push(text),
                HeaderValue::TextList(mut list) => {
                    reference_ids.extend(list.drain(..).filter(|text| text.len() <= MAX_ID_LENGTH));
                }
                HeaderValue::Collection(col) => {
                    for item in col {
                        match item {
                            HeaderValue::Text(text) if text.len() <= MAX_ID_LENGTH => {
                                reference_ids.push(text)
                            }
                            HeaderValue::TextList(mut list) => reference_ids
                                .extend(list.drain(..).filter(|text| text.len() <= MAX_ID_LENGTH)),
                            _ => (),
                        }
                    }
                }
                _ => (),
            }
        }

        // Build message document
        let mut batch = build_message_document(account, message)?;
        let mut batches = Vec::new();

        // Lock account
        let _account_lock = self
            .id_lock
            .lock(account)
            .map_err(|_| StoreError::InternalError("Failed to obtain mutex".to_string()))?;

        // Obtain thread id
        let thread_id = if !reference_ids.is_empty() {
            // Query all document ids containing the reference ids
            let message_doc_ids = self
                .db
                .query(
                    account,
                    crate::MAIL_CID,
                    Some(Filter::and(vec![
                        Filter::new_condition(
                            MessageField::ThreadName.into(),
                            ComparisonOperator::Equal,
                            FieldValue::Keyword(&thread_name),
                        ),
                        Filter::or(
                            reference_ids
                                .iter()
                                .map(|id| {
                                    Filter::new_condition(
                                        MessageField::MessageIdRef.into(),
                                        ComparisonOperator::Equal,
                                        FieldValue::Keyword(id),
                                    )
                                })
                                .collect(),
                        ),
                    ])),
                    None,
                )?
                .collect::<Vec<DocumentId>>();

            // Obtain thread ids for all matching document ids
            if !message_doc_ids.is_empty() {
                let thread_ids = self
                    .db
                    .get_multi_value(
                        account,
                        crate::MAIL_CID,
                        &message_doc_ids,
                        MessageField::ThreadId.into(),
                    )?
                    .into_iter()
                    .flatten()
                    .collect::<HashSet<ThreadId>>();

                if thread_ids.len() > 1 {
                    Some(self.merge_threads(
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
            0
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
        self.db.update_bulk(batches)?;

        Ok(())
    }

    fn merge_threads(
        &self,
        account: AccountId,
        batches: &mut Vec<WriteOperation>,
        thread_ids: Vec<ThreadId>,
    ) -> store::Result<ThreadId> {
        // Query tags for all thread ids
        let mut tag_iterators = Vec::with_capacity(thread_ids.len());

        for (pos, tag_iterator) in self
            .db
            .get_tags(
                account,
                MAIL_CID,
                MessageField::ThreadId.into(),
                &thread_ids
                    .iter()
                    .map(|id| Tag::Id(*id))
                    .collect::<Vec<Tag>>(),
            )?
            .into_iter()
            .enumerate()
        {
            if let Some(tag_iterator) = tag_iterator {
                debug_assert!(tag_iterator.size_hint().0 > 0);
                tag_iterators.push((tag_iterator, thread_ids[pos]));
            } else {
                // TODO log this error instead
                debug_assert!(false, "No tags found for thread id {}.", thread_ids[pos]);
            }
        }

        tag_iterators.sort_by_key(|i| i.0.size_hint().0);

        let mut tag_iterators = tag_iterators.into_iter();
        let thread_id = if let Some((_, thread_id)) = tag_iterators.next_back() {
            thread_id
        } else {
            thread_ids[0]
        };

        let mut deleted_threads = WriteOperation::delete(account, MAIL_CID, None);

        while let Some((tag_iterator, delete_thread_id)) = tag_iterators.next_back() {
            for document_id in tag_iterator {
                let mut batch = WriteOperation::update(account, MAIL_CID, document_id);
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
