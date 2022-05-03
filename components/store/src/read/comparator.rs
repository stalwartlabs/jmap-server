use roaring::RoaringBitmap;

use crate::FieldId;

#[derive(Debug)]
pub struct FieldComparator {
    pub field: FieldId,
    pub ascending: bool,
}

#[derive(Debug)]
pub struct DocumentSetComparator {
    pub set: RoaringBitmap,
    pub ascending: bool,
}

#[derive(Debug)]
pub enum Comparator {
    List(Vec<Comparator>),
    Field(FieldComparator),
    DocumentSet(DocumentSetComparator),
    None,
}

impl Default for Comparator {
    fn default() -> Self {
        Comparator::None
    }
}

impl Comparator {
    pub fn ascending(field: FieldId) -> Self {
        Comparator::Field(FieldComparator {
            field,
            ascending: true,
        })
    }

    pub fn descending(field: FieldId) -> Self {
        Comparator::Field(FieldComparator {
            field,
            ascending: false,
        })
    }
}
