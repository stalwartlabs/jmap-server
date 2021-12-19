use std::collections::BTreeMap;

use mail_parser::{HeaderName, HeaderValue, Message};
use store::{
    field::Text, AccountId, ComparisonOperator, FieldValue, Filter, Integer, Store, StoreError,
    StoreQuery,
};

use crate::{parse::build_message_document, MessageField, MessageStore};

impl<'x, T> MessageStore<'x, T>
where
    T: Store<'x>,
{
    pub fn ingest_message(&self, account: AccountId, raw_message: &[u8]) -> store::Result<()> {
        // Parse raw message
        let message = Message::parse(raw_message).ok_or(StoreError::ParseError)?;

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
                .get(&header_name)
                .unwrap_or(&HeaderValue::Empty)
            {
                HeaderValue::Text(text) => reference_ids.push(text),
                HeaderValue::TextList(list) => reference_ids.extend(list),
                HeaderValue::Collection(col) => {
                    for item in col {
                        match item {
                            HeaderValue::Text(text) => reference_ids.push(text),
                            HeaderValue::TextList(list) => reference_ids.extend(list),
                            _ => {}
                        }
                    }
                }
                _ => todo!(),
            }
        }

        // Lock all reference ids
        let _reference_ids_lock = self
            .ref_lock
            .lock_hash(reference_ids.iter())
            .map_err(|_| StoreError::InternalError("Failed to obtain mutex".to_string()))?;

        // Obtain thread id
        let thread_id = if !reference_ids.is_empty() {
            // Query all document ids containing the reference ids
            // and lock all those ids to prevent other threads from
            // modifying them.
            let locks = self
                .id_lock
                .lock(self.db.query(
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
                            .into_iter()
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
                )?)
                .map_err(|_| StoreError::InternalError("Failed to obtain mutex".to_string()))?;

            // Obtain thread ids for all matching document ids
            if !locks.is_empty() {
                let message_doc_ids = locks.get_values();
                let thread_ids = self.db.get_integer_multi(
                    account,
                    crate::MAIL_CID,
                    &message_doc_ids,
                    MessageField::ThreadId.into(),
                )?;

                if thread_ids.len() > 1 {
                    let id_map = thread_ids
                        .iter()
                        .enumerate()
                        .filter_map(|(pos, value)| Some(((*value)?, pos)))
                        .collect::<BTreeMap<u32, usize>>();
                    if !id_map.is_empty() {
                        let mut id_map_iter = id_map.iter();
                        let thread_id = *(id_map_iter.next().unwrap().0);
                        for (_, &pos) in id_map_iter {
                            let doc_id = message_doc_ids[pos];
                            /*document.add_integer(
                                MessageField::ThreadId.into(),
                                0,
                                thread_id,
                                true,
                                false,
                            );*/
                        }
                        Some(thread_id)
                    } else {
                        None
                    }
                } else {
                    // There was just one match, use it as the thread id
                    thread_ids[0]
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

        let mut document = build_message_document(message)?;

        document.add_integer(MessageField::ThreadId.into(), 0, thread_id, true, false);

        document.add_text(
            MessageField::ThreadName.into(),
            0,
            Text::Keyword(thread_name.into()),
            false,
            false,
        );

        Ok(())
    }
}

/*

Message 1
ID: 001

Message 2
ID: 002
References: 001

Message 3
ID: 003
References: 002

Message 4
ID: 004
References: 003

Message 5
ID: 005
References: 002, 001


1, 2, 3, 4, 5 =>
1 = ThreadId 1
2 = SELECT ids WHERE ref = 1 => id(1) => ThreadId 1
3 = SELECT ids WHERE ref = 2 => id(2) => ThreadId 1
4 = SELECT ids WHERE ref = 3 => id(3) => ThreadId 1
5 = SELECT ids WHERE ref = 2, 1 => id(1,2) => ThreadId 1

5, 4, 3, 2, 1 =>
5 = SELECT ids WHERE ref = 5, 2, 1 => NULL => ThreadId 5
4 = SELECT ids WHERE ref = 4, 3 => NULL => ThreadId 4
3 = SELECT ids WHERE ref = 3, 2 => id(4, 5) => ThreadId 4, 5 => Delete 5 => Merge => ThreadId 4
2 = SELECT ids WHERE ref = 2, 1 => id(3, 4, 5) => ThreadId 4
1 = SELECT ids WHERE ref = 1 => id(2, 3, 4, 5) => ThreadId 4


*/
