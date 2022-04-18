use std::collections::HashMap;

use jmap::{
    changes::JMAPChanges, id::JMAPIdSerialize, json::JSONValue, request::GetRequest, JMAPError,
};
use store::{
    query::{JMAPIdMapFnc, JMAPStoreQuery},
    Collection, Comparator, FieldComparator, Filter, JMAPId, JMAPIdPrefix, JMAPStore, Store, Tag,
};

use crate::mail::MessageField;

pub trait JMAPMailThreadGet {
    fn thread_get(&self, request: GetRequest) -> jmap::Result<JSONValue>;
}

impl<T> JMAPMailThreadGet for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn thread_get(&self, request: GetRequest) -> jmap::Result<JSONValue> {
        let thread_ids = request.ids.unwrap_or_default();

        if thread_ids.len() > self.config.mail_thread_max_results {
            return Err(JMAPError::RequestTooLarge);
        }

        let mut not_found = Vec::new();
        let mut results = Vec::with_capacity(thread_ids.len());

        for jmap_thread_id in thread_ids {
            let thread_id = jmap_thread_id.get_document_id();
            if let Some(doc_ids) = self.get_tag(
                request.account_id,
                Collection::Mail,
                MessageField::ThreadId.into(),
                Tag::Id(thread_id),
            )? {
                let mut thread_obj = HashMap::with_capacity(2);
                thread_obj.insert("id".to_string(), jmap_thread_id.to_jmap_string().into());
                let email_ids: Vec<JSONValue> = self
                    .query::<JMAPIdMapFnc>(JMAPStoreQuery::new(
                        request.account_id,
                        Collection::Mail,
                        Filter::DocumentSet(doc_ids),
                        Comparator::Field(FieldComparator {
                            field: MessageField::ReceivedAt.into(),
                            ascending: true,
                        }),
                    ))?
                    .into_iter()
                    .map(|doc_id| {
                        JMAPId::from_parts(thread_id, doc_id.get_document_id())
                            .to_jmap_string()
                            .into()
                    })
                    .collect();

                thread_obj.insert("emailIds".to_string(), email_ids.into());
                results.push(thread_obj.into());
            } else {
                not_found.push(jmap_thread_id.to_jmap_string().into());
            }
        }

        let mut obj = HashMap::new();
        obj.insert(
            "state".to_string(),
            self.get_state(request.account_id, Collection::Thread)?
                .into(),
        );
        obj.insert(
            "list".to_string(),
            if !results.is_empty() {
                JSONValue::Array(results)
            } else {
                JSONValue::Null
            },
        );
        obj.insert(
            "notFound".to_string(),
            if !not_found.is_empty() {
                not_found.into()
            } else {
                JSONValue::Null
            },
        );
        Ok(obj.into())
    }
}
