use crate::{DocumentId, JMAPId};

pub mod comparator;
pub mod filter;
pub mod get;
pub mod iterator;
pub mod query;

pub struct DefaultIdMapper {}

pub trait QueryFilterMap {
    fn filter_map_id(&mut self, document_id: DocumentId) -> crate::Result<Option<JMAPId>>;
}

impl QueryFilterMap for DefaultIdMapper {
    fn filter_map_id(&mut self, document_id: DocumentId) -> crate::Result<Option<JMAPId>> {
        Ok(Some(document_id as JMAPId))
    }
}
