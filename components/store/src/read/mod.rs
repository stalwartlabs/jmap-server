use crate::{DocumentId, JMAPId};

pub mod acl;
pub mod comparator;
pub mod filter;
pub mod get;
pub mod iterator;
pub mod query;

pub type FilterMapper = fn(DocumentId) -> crate::Result<Option<JMAPId>>;

pub fn default_filter_mapper(document_id: DocumentId) -> crate::Result<Option<JMAPId>> {
    Ok(Some(document_id as JMAPId))
}
