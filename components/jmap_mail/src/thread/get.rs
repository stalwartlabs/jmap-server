use jmap::{
    id::jmap::JMAPId,
    jmap_store::get::{default_mapper, GetHelper, GetObject},
    request::get::{GetRequest, GetResponse},
};
use store::{
    core::{collection::Collection, tag::Tag, JMAPIdPrefix},
    read::{
        comparator::{Comparator, FieldComparator},
        filter::Filter,
        FilterMapper,
    },
    JMAPStore, Store,
};

use crate::mail::MessageField;

use super::schema::{Property, Thread};

impl GetObject for Thread {
    type GetArguments = ();

    fn default_properties() -> Vec<Self::Property> {
        vec![Property::Id, Property::EmailIds]
    }

    fn get_as_id(&self, property: &Self::Property) -> Option<Vec<JMAPId>> {
        match property {
            Property::Id => vec![self.id],
            Property::EmailIds => self.email_ids.clone(),
        }
        .into()
    }
}

pub trait JMAPGetThread<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn thread_get(&self, request: GetRequest<Thread>) -> jmap::Result<GetResponse<Thread>>;
}

impl<T> JMAPGetThread<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn thread_get(&self, request: GetRequest<Thread>) -> jmap::Result<GetResponse<Thread>> {
        let helper = GetHelper::new(self, request, default_mapper.into())?;
        let account_id = helper.account_id;

        let response = helper.get(|id, _properties| {
            let thread_id = id.get_document_id();
            if let Some(doc_ids) = self.get_tag(
                account_id,
                Collection::Mail,
                MessageField::ThreadId.into(),
                Tag::Id(thread_id),
            )? {
                Ok(Some(Thread {
                    id,
                    email_ids: self
                        .query_store::<FilterMapper>(
                            account_id,
                            Collection::Mail,
                            Filter::DocumentSet(doc_ids),
                            Comparator::Field(FieldComparator {
                                field: MessageField::ReceivedAt.into(),
                                ascending: true,
                            }),
                        )?
                        .into_iter()
                        .map(|doc_id| JMAPId::from_parts(thread_id, doc_id.get_document_id()))
                        .collect(),
                }))
            } else {
                Ok(None)
            }
        })?;

        Ok(response)
    }
}
