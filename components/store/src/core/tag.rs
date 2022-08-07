use crate::{DocumentId, Integer, TagId};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub enum Tag {
    Static(TagId),
    Id(Integer),
    Text(String),
    Default,
}

impl Tag {
    pub fn as_id(&self) -> Integer {
        match self {
            Tag::Id(id) => *id,
            _ => panic!("Tag is not an ID"),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Tag::Static(_) | Tag::Default => std::mem::size_of::<TagId>(),
            Tag::Id(_) => std::mem::size_of::<DocumentId>(),
            Tag::Text(text) => text.len(),
        }
    }

    pub fn unwrap_id(&self) -> Option<DocumentId> {
        match self {
            Tag::Id(id) => Some(*id),
            _ => None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
