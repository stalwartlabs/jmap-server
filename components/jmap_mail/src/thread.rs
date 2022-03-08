use std::collections::HashMap;

use jmap_store::{
    async_trait::async_trait,
    changes::{JMAPChanges, JMAPChangesRequest, JMAPChangesResponse},
    id::JMAPIdSerialize,
    json::JSONValue,
    JMAPError, JMAPGet, JMAPGetResponse, JMAP_MAIL, JMAP_THREAD,
};
use store::{
    query::JMAPStoreQuery, Comparator, FieldComparator, Filter, JMAPId, JMAPIdPrefix, JMAPStore,
    Store, Tag,
};

use crate::{JMAPMailProperties, MessageField};

#[async_trait]
pub trait JMAPMailThread {
    async fn thread_get(
        &self,
        request: JMAPGet<JMAPMailProperties, ()>,
    ) -> jmap_store::Result<jmap_store::JMAPGetResponse>;

    async fn thread_changes(
        &self,
        request: JMAPChangesRequest,
    ) -> jmap_store::Result<JMAPChangesResponse<()>>;
}

#[async_trait]
impl<T> JMAPMailThread for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    async fn thread_get(
        &self,
        request: JMAPGet<JMAPMailProperties, ()>,
    ) -> jmap_store::Result<jmap_store::JMAPGetResponse> {
        let thread_ids = request.ids.unwrap_or_default();

        if thread_ids.len() > self.config.mail_thread_max_results {
            return Err(JMAPError::RequestTooLarge);
        }

        let mut not_found = Vec::new();
        let mut results = Vec::with_capacity(thread_ids.len());

        for jmap_thread_id in thread_ids {
            let thread_id = jmap_thread_id.get_document_id();
            if let Some(doc_ids) = self
                .get_tag(
                    request.account_id,
                    JMAP_MAIL,
                    MessageField::ThreadId.into(),
                    Tag::Id(thread_id),
                )
                .await?
            {
                let mut thread_obj = HashMap::with_capacity(2);
                thread_obj.insert("id".to_string(), jmap_thread_id.to_jmap_string().into());
                let email_ids: Vec<JSONValue> = self
                    .query(JMAPStoreQuery::new(
                        request.account_id,
                        JMAP_MAIL,
                        Filter::DocumentSet(doc_ids),
                        Comparator::Field(FieldComparator {
                            field: MessageField::ReceivedAt.into(),
                            ascending: true,
                        }),
                        0,
                    ))
                    .await?
                    .results
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
                not_found.push(jmap_thread_id);
            }
        }

        Ok(JMAPGetResponse {
            state: self.get_state(request.account_id, JMAP_THREAD).await?,
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

    async fn thread_changes(
        &self,
        request: JMAPChangesRequest,
    ) -> jmap_store::Result<JMAPChangesResponse<()>> {
        self.get_jmap_changes(
            request.account,
            JMAP_THREAD,
            request.since_state,
            request.max_changes,
        )
        .await
        .map_err(|e| e.into())
    }
}
