use nlp::Language;

use crate::{
    serialize::StoreSerialize, DocumentId, FieldId, Float, Integer, LongInteger, Tag, TagId,
};

#[derive(Debug)]
pub enum UpdateField {
    Text(Field<Text>),
    Binary(Field<Vec<u8>>),
    Number(Field<Number>),
    Tag(Field<Tag>),
}

#[derive(Debug)]
pub enum Number {
    Integer(Integer),
    LongInteger(LongInteger),
    Float(Float),
}

impl Number {
    pub fn to_be_bytes(&self) -> Vec<u8> {
        match self {
            Number::Integer(i) => i.to_be_bytes().to_vec(),
            Number::LongInteger(i) => i.to_be_bytes().to_vec(),
            Number::Float(f) => f.to_be_bytes().to_vec(),
        }
    }
}

impl From<LongInteger> for Number {
    fn from(value: LongInteger) -> Self {
        Number::LongInteger(value)
    }
}

impl From<Integer> for Number {
    fn from(value: Integer) -> Self {
        Number::Integer(value)
    }
}

impl From<Float> for Number {
    fn from(value: Float) -> Self {
        Number::Float(value)
    }
}

impl StoreSerialize for Number {
    fn serialize(&self) -> Option<Vec<u8>> {
        match self {
            Number::Integer(i) => i.serialize(),
            Number::LongInteger(i) => i.serialize(),
            Number::Float(f) => f.serialize(),
        }
    }
}

#[allow(clippy::len_without_is_empty)]
pub trait FieldLen {
    fn len(&self) -> usize;
}

#[derive(Debug)]
pub struct Field<T> {
    pub field: FieldId,
    pub options: u64,
    pub value: T,
}

pub struct IndexOptions {}

impl IndexOptions {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> u64 {
        0
    }
}

pub trait Options {
    const F_STORE: u64 = 0x01 << 32;
    const F_SORT: u64 = 0x02 << 32;
    const F_CLEAR: u64 = 0x04 << 32;
    const F_NONE: u64 = 0;
    const F_KEYWORD: u64 = 1;
    const F_TOKENIZE: u64 = 2;
    const F_FULL_TEXT: u64 = 3;

    fn store(self) -> Self;
    fn sort(self) -> Self;
    fn clear(self) -> Self;
    fn keyword(self) -> Self;
    fn tokenize(self) -> Self;
    fn full_text(self, part_id: u32) -> Self;

    fn is_store(&self) -> bool;
    fn is_sort(&self) -> bool;
    fn is_clear(&self) -> bool;
    fn get_text_options(&self) -> u64;
}

impl Options for u64 {
    fn store(mut self) -> Self {
        self |= Self::F_STORE;
        self
    }

    fn sort(mut self) -> Self {
        self |= Self::F_SORT;
        self
    }

    fn keyword(self) -> Self {
        self | Self::F_KEYWORD
    }

    fn tokenize(self) -> Self {
        self | Self::F_TOKENIZE
    }

    fn full_text(self, part_id: u32) -> Self {
        self | (Self::F_FULL_TEXT + part_id as u64)
    }

    fn clear(mut self) -> Self {
        self |= Self::F_CLEAR;
        self
    }

    fn is_store(&self) -> bool {
        self & Self::F_STORE != 0
    }

    fn is_sort(&self) -> bool {
        self & Self::F_SORT != 0
    }

    fn is_clear(&self) -> bool {
        self & Self::F_CLEAR != 0
    }

    fn get_text_options(&self) -> u64 {
        *self & 0xFFFFFFFF
    }
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

impl Tag {
    pub fn len(&self) -> usize {
        match self {
            Tag::Static(_) | Tag::Default => std::mem::size_of::<TagId>(),
            Tag::Id(_) => std::mem::size_of::<DocumentId>(),
            Tag::Text(text) => text.len(),
            Tag::Bytes(bytes) => bytes.len(),
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

#[derive(Debug)]
pub enum Text {
    None {
        value: String,
    },
    Keyword {
        value: String,
    },
    Tokenized {
        value: String,
        language: Language,
    },
    Full {
        value: String,
        part_id: u32,
        language: Language,
    },
}
