use crate::{DocumentId, JMAPId};

pub mod comparator;
pub mod filter;
pub mod get;
pub mod iterator;
pub mod query;

pub type FilterMapper = fn(DocumentId) -> crate::Result<Option<JMAPId>>;

pub fn default_filter_mapper(document_id: DocumentId) -> crate::Result<Option<JMAPId>> {
    Ok(Some(document_id as JMAPId))
}

pub fn default_mapper(document_ids: Vec<DocumentId>) -> crate::Result<Vec<JMAPId>> {
    Ok(document_ids.into_iter().map(|id| id as JMAPId).collect())
}
