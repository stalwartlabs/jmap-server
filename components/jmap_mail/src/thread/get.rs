use std::collections::HashMap;

use jmap::{
    id::JMAPIdSerialize, jmap_store::get::GetObject, protocol::json::JSONValue,
    request::get::GetRequest,
};
use store::{
    core::{collection::Collection, tag::Tag, JMAPIdPrefix},
    read::{
        comparator::{Comparator, FieldComparator},
        filter::Filter,
        DefaultIdMapper,
    },
    AccountId, JMAPId, JMAPStore, Store,
};

use crate::mail::MessageField;

use super::ThreadProperty;

pub struct GetThread<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    store: &'y JMAPStore<T>,
    account_id: AccountId,
}

impl<'y, T> GetObject<'y, T> for GetThread<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    type Property = ThreadProperty;

    fn new(
        store: &'y JMAPStore<T>,
        request: &mut GetRequest,
        _properties: &[Self::Property],
    ) -> jmap::Result<Self> {
        Ok(GetThread {
            store,
            account_id: request.account_id,
        })
    }

    fn get_item(
        &self,
        jmap_id: JMAPId,
        _properties: &[Self::Property],
    ) -> jmap::Result<Option<JSONValue>> {
        let thread_id = jmap_id.get_document_id();
        if let Some(doc_ids) = self.store.get_tag(
            self.account_id,
            Collection::Mail,
            MessageField::ThreadId.into(),
            Tag::Id(thread_id),
        )? {
            let mut thread_obj = HashMap::with_capacity(2);
            thread_obj.insert("id".to_string(), jmap_id.to_jmap_string().into());
            let email_ids: Vec<JSONValue> = self
                .store
                .query_store::<DefaultIdMapper>(
                    self.account_id,
                    Collection::Mail,
                    Filter::DocumentSet(doc_ids),
                    Comparator::Field(FieldComparator {
                        field: MessageField::ReceivedAt.into(),
                        ascending: true,
                    }),
                )?
                .into_iter()
                .map(|doc_id| {
                    JMAPId::from_parts(thread_id, doc_id.get_document_id())
                        .to_jmap_string()
                        .into()
                })
                .collect();

            thread_obj.insert("emailIds".to_string(), email_ids.into());
            Ok(Some(thread_obj.into()))
        } else {
            Ok(None)
        }
    }

    fn map_ids<W>(&self, document_ids: W) -> jmap::Result<Vec<JMAPId>>
    where
        W: Iterator<Item = store::DocumentId>,
    {
        Ok(document_ids.map(|id| id as JMAPId).collect())
    }

    fn is_virtual() -> bool {
        true
    }

    fn default_properties() -> Vec<Self::Property> {
        vec![]
    }
}
