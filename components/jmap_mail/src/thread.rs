use std::collections::HashMap;

use jmap_store::{
    changes::{JMAPLocalChanges, JMAPState},
    id::JMAPIdSerialize,
    json::JSONValue,
    local_store::JMAPLocalStore,
    JMAPChangesResponse, JMAPError, JMAPGet, JMAPGetResponse, JMAPId, JMAP_MAIL, JMAP_THREAD,
};
use store::{Comparator, DocumentSet, FieldComparator, Filter, Store, Tag};

use crate::{JMAPMailIdImpl, JMAPMailProperties, JMAPMailThread, MessageField};

impl<'x, T> JMAPMailThread<'x> for JMAPLocalStore<T>
where
    T: Store<'x>,
{
    fn thread_get(
        &'x self,
        request: JMAPGet<JMAPMailProperties<'x>>,
    ) -> jmap_store::Result<jmap_store::JMAPGetResponse> {
        let thread_ids = request.ids.unwrap_or_else(Vec::new);

        if thread_ids.len() > self.mail_config.thread_max_results {
            return Err(JMAPError::RequestTooLarge);
        }

        let mut not_found = Vec::new();
        let mut results = Vec::with_capacity(thread_ids.len());

        for jmap_thread_id in thread_ids {
            let thread_id = jmap_thread_id.get_document_id();
            if let Some(doc_ids) = self.store.get_tag(
                request.account_id,
                JMAP_MAIL,
                MessageField::ThreadId.into(),
                Tag::Id(thread_id),
            )? {
                let mut thread_obj = HashMap::with_capacity(2);
                thread_obj.insert("id".to_string(), jmap_thread_id.to_jmap_string().into());
                let doc_ids = self.store.query(
                    request.account_id,
                    JMAP_MAIL,
                    Filter::DocumentSet(doc_ids),
                    Comparator::Field(FieldComparator {
                        field: MessageField::ReceivedAt.into(),
                        ascending: true,
                    }),
                )?;
                let mut email_ids = Vec::with_capacity(doc_ids.len());
                for doc_id in doc_ids {
                    email_ids.push(
                        JMAPId::from_email(thread_id, doc_id)
                            .to_jmap_string()
                            .into(),
                    );
                }
                thread_obj.insert("emailIds".to_string(), email_ids.into());
                results.push(thread_obj.into());
            } else {
                not_found.push(jmap_thread_id);
            }
        }

        Ok(JMAPGetResponse {
            state: self.get_state(request.account_id, JMAP_THREAD)?,
            list: if !results.is_empty() {
                JSONValue::Array(results)
            } else {
                JSONValue::Null
            },
            not_found: if not_found.is_empty() {
                None
            } else {
                not_found.into()
            },
        })
    }

    fn thread_changes(
        &'x self,
        account: store::AccountId,
        since_state: JMAPState,
        max_changes: usize,
    ) -> jmap_store::Result<JMAPChangesResponse> {
        self.get_jmap_changes(account, JMAP_THREAD, since_state, max_changes)
            .map_err(|e| e.into())
    }
}
