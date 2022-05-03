use crate::FieldId;

use super::options::Options;

#[allow(clippy::len_without_is_empty)]
pub trait FieldLen {
    fn len(&self) -> usize;
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Field<T> {
    pub field: FieldId,
    pub options: u64,
    pub value: T,
}

impl<T> Field<T> {
    pub fn new(field: FieldId, value: T, options: u64) -> Self {
        Self {
            field,
            value,
            options,
        }
    }

    #[inline(always)]
    pub fn get_field(&self) -> FieldId {
        self.field
    }

    #[inline(always)]
    pub fn get_options(&self) -> u64 {
        self.options
    }

    #[inline(always)]
    pub fn is_sorted(&self) -> bool {
        self.options.is_sort()
    }

    #[inline(always)]
    pub fn is_stored(&self) -> bool {
        self.options.is_store()
    }

    #[inline(always)]
    pub fn is_clear(&self) -> bool {
        self.options.is_clear()
    }

    pub fn size_of(&self) -> usize {
        std::mem::size_of::<T>()
    }
}
