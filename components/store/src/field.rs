use std::{collections::HashSet, ops::Deref};

use nlp::Language;

use crate::{
    leb128::Leb128,
    serialize::{StoreDeserialize, StoreSerialize, TAG_BYTES, TAG_ID, TAG_STATIC, TAG_TEXT},
    DocumentId, FieldId, Float, Integer, LongInteger, Tag, TagId,
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

pub struct DefaultOptions {}

impl DefaultOptions {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> u64 {
        0
    }
}

pub trait Options {
    fn store(self) -> Self;
    fn sort(self) -> Self;
    fn clear(self) -> Self;
    fn term_index(self) -> Self;

    fn is_store(&self) -> bool;
    fn is_sort(&self) -> bool;
    fn is_clear(&self) -> bool;
    fn build_term_index(&self) -> bool;
}

pub const F_STORE: u64 = 0x01;
pub const F_SORT: u64 = 0x02;
pub const F_CLEAR: u64 = 0x04;
pub const F_TERM_INDEX: u64 = 0x08;

impl Options for u64 {
    fn store(mut self) -> Self {
        self |= F_STORE;
        self
    }

    fn sort(mut self) -> Self {
        self |= F_SORT;
        self
    }

    fn term_index(mut self) -> Self {
        self |= F_TERM_INDEX;
        self
    }

    fn clear(mut self) -> Self {
        self |= F_CLEAR;
        self
    }

    fn is_store(&self) -> bool {
        self & F_STORE != 0
    }

    fn is_sort(&self) -> bool {
        self & F_SORT != 0
    }

    fn is_clear(&self) -> bool {
        self & F_CLEAR != 0
    }

    fn build_term_index(&self) -> bool {
        self & F_TERM_INDEX != 0
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

    #[inline(always)]
    pub fn build_term_index(&self) -> bool {
        self.options.build_term_index()
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

#[derive(Default)]
pub struct Tags {
    pub items: HashSet<Tag>,
    pub changed: bool,
}

impl Tags {
    pub fn insert(&mut self, item: Tag) {
        if self.items.insert(item) && !self.changed {
            self.changed = true;
        }
    }

    pub fn remove(&mut self, item: &Tag) {
        if self.items.remove(item) && !self.changed {
            self.changed = true;
        }
    }

    pub fn contains(&self, item: &Tag) -> bool {
        self.items.contains(item)
    }

    pub fn has_changed(&self) -> bool {
        self.changed
    }
}

impl StoreSerialize for Tags {
    fn serialize(&self) -> Option<Vec<u8>> {
        let mut bytes = Vec::with_capacity(self.items.len() * std::mem::size_of::<Tag>());
        self.items.len().to_leb128_bytes(&mut bytes);
        for tag in &self.items {
            match tag {
                Tag::Static(id) => {
                    bytes.push(TAG_STATIC);
                    bytes.push(*id);
                }
                Tag::Id(id) => {
                    bytes.push(TAG_ID);
                    (*id).to_leb128_bytes(&mut bytes);
                }
                Tag::Text(text) => {
                    bytes.push(TAG_TEXT);
                    text.len().to_leb128_bytes(&mut bytes);
                    bytes.extend_from_slice(text.as_bytes());
                }
                Tag::Bytes(value) => {
                    bytes.push(TAG_BYTES);
                    value.len().to_leb128_bytes(&mut bytes);
                    bytes.extend_from_slice(value);
                }
                Tag::Default => {
                    bytes.push(TAG_STATIC);
                    bytes.push(0);
                }
            }
        }
        Some(bytes)
    }
}

impl StoreDeserialize for Tags {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        let mut bytes_it = bytes.iter();
        let total_tags = usize::from_leb128_it(&mut bytes_it)?;
        let mut tags = HashSet::with_capacity(total_tags);
        for _ in 0..total_tags {
            match *bytes_it.next()? {
                BM_TAG_STATIC => {
                    tags.insert(Tag::Static(*bytes_it.next()?));
                }
                BM_TAG_ID => {
                    tags.insert(Tag::Id(DocumentId::from_leb128_it(&mut bytes_it)?));
                }
                BM_TAG_TEXT => {
                    let text_len = usize::from_leb128_it(&mut bytes_it)?;
                    if text_len > 0 {
                        let mut str_bytes = Vec::with_capacity(text_len);
                        for _ in 0..text_len {
                            str_bytes.push(*bytes_it.next()?);
                        }
                        tags.insert(Tag::Text(String::from_utf8(str_bytes).ok()?));
                    } else {
                        tags.insert(Tag::Text("".to_string()));
                    }
                }
                _ => return None,
            }
        }

        Some(Tags {
            items: tags,
            changed: false,
        })
    }
}

pub struct DocumentIdTag {
    pub item: DocumentId,
}

impl Deref for DocumentIdTag {
    type Target = DocumentId;

    fn deref(&self) -> &Self::Target {
        &self.item
    }
}

impl AsRef<DocumentId> for DocumentIdTag {
    fn as_ref(&self) -> &DocumentId {
        &self.item
    }
}

impl StoreDeserialize for DocumentIdTag {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        debug_assert_eq!(bytes[1], TAG_ID);
        Some(DocumentIdTag {
            item: DocumentId::from_leb128_bytes(bytes.get(2..)?)?.0,
        })
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
    },
    Full {
        value: String,
        part_id: u32,
        language: Language,
    },
}
