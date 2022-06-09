use crate::{DocumentId, JMAPId};

pub mod acl;
pub mod bitmap;
pub mod collection;
pub mod document;
pub mod error;
pub mod number;
pub mod tag;

pub trait JMAPIdPrefix {
    fn from_parts(prefix_id: DocumentId, doc_id: DocumentId) -> JMAPId;
    fn get_document_id(&self) -> DocumentId;
    fn get_prefix_id(&self) -> DocumentId;
}

impl JMAPIdPrefix for JMAPId {
    fn from_parts(prefix_id: DocumentId, doc_id: DocumentId) -> JMAPId {
        (prefix_id as JMAPId) << 32 | doc_id as JMAPId
    }

    fn get_document_id(&self) -> DocumentId {
        (self & 0xFFFFFFFF) as DocumentId
    }

    fn get_prefix_id(&self) -> DocumentId {
        (self >> 32) as DocumentId
    }
}
