use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
};

use nlp::{
    lang::{LanguageDetector, MIN_LANGUAGE_SCORE},
    stemmer::Stemmer,
    tokenizers::{tokenize, Token},
    Language,
};

use crate::{
    batch::MAX_TOKEN_LENGTH,
    blob::BlobIndex,
    leb128::Leb128,
    serialize::{StoreDeserialize, StoreSerialize, BM_TAG_ID, BM_TAG_STATIC, BM_TAG_TEXT},
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
    fn store_blob(self, index: BlobIndex) -> Self;
    fn sort(self) -> Self;
    fn clear(self) -> Self;

    fn is_store(&self) -> bool;
    fn is_sort(&self) -> bool;
    fn is_store_blob(&self) -> Option<BlobIndex>;
    fn is_clear(&self) -> bool;
}

pub const F_STORE: u64 = 0x01 << 32;
pub const F_SORT: u64 = 0x02 << 32;
pub const F_CLEAR: u64 = 0x04 << 32;
pub const F_STORE_BLOB: u64 = 0x08 << 32;

impl Options for u64 {
    fn store(mut self) -> Self {
        self |= F_STORE;
        self
    }

    fn store_blob(mut self, index: BlobIndex) -> Self {
        self |= F_STORE_BLOB | (index as u64);
        self
    }

    fn sort(mut self) -> Self {
        self |= F_SORT;
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

    fn is_store_blob(&self) -> Option<BlobIndex> {
        if self & F_STORE_BLOB != 0 {
            Some((self & 0xFFFFFFFF) as BlobIndex)
        } else {
            None
        }
    }

    fn is_clear(&self) -> bool {
        self & F_CLEAR != 0
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
    pub fn get_blob_index(&self) -> Option<BlobIndex> {
        self.options.is_store_blob()
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
            Tag::Static(_) => std::mem::size_of::<TagId>(),
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
                    bytes.push(BM_TAG_STATIC);
                    bytes.push(*id);
                }
                Tag::Id(id) => {
                    bytes.push(BM_TAG_ID);
                    (*id).to_leb128_bytes(&mut bytes);
                }
                Tag::Text(text) => {
                    bytes.push(BM_TAG_TEXT);
                    text.len().to_leb128_bytes(&mut bytes);
                    bytes.extend_from_slice(text.as_bytes());
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
        debug_assert_eq!(bytes[1], BM_TAG_ID);
        Some(DocumentIdTag {
            item: DocumentId::from_leb128_bytes(bytes.get(2..)?)?.0,
        })
    }
}

#[derive(Default)]
pub struct Keywords {
    pub items: HashMap<String, Vec<FieldId>>,
    pub changed: bool,
}

impl Keywords {
    pub fn insert(&mut self, keyword: String, field: FieldId) {
        let fields = self.items.entry(keyword).or_insert_with(Vec::new);
        if !fields.contains(&field) {
            fields.push(field);
            self.changed = true;
        }
    }

    pub fn remove(&mut self, keyword: &str, field: &FieldId) {
        if let Some(fields) = self.items.get_mut(keyword) {
            if let Some(idx) = fields.iter().position(|f| *f == *field) {
                if fields.len() > 1 {
                    fields.remove(idx);
                } else {
                    self.items.remove(keyword);
                }
                self.changed = true;
            }
        }
    }

    pub fn has_changed(&self) -> bool {
        self.changed
    }
}

impl StoreSerialize for Keywords {
    fn serialize(&self) -> Option<Vec<u8>> {
        let mut bytes = Vec::with_capacity(self.items.len() * 10);
        self.items.len().to_leb128_bytes(&mut bytes);
        for (string, fields) in &self.items {
            fields.len().to_leb128_bytes(&mut bytes);
            for field in fields {
                bytes.push(*field);
            }
            string.len().to_leb128_bytes(&mut bytes);
            bytes.extend_from_slice(string.as_bytes());
        }
        Some(bytes)
    }
}

impl StoreDeserialize for Keywords {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        let mut bytes_it = bytes.iter();
        let total_strings = usize::from_leb128_it(&mut bytes_it)?;
        let mut strings = HashMap::with_capacity(total_strings);
        for _ in 0..total_strings {
            let total_fields = usize::from_leb128_it(&mut bytes_it)?;
            let mut fields = Vec::with_capacity(total_fields);
            for _ in 0..total_fields {
                fields.push(*bytes_it.next()?);
            }
            let text_len = usize::from_leb128_it(&mut bytes_it)?;
            let text = if text_len > 0 {
                let mut str_bytes = Vec::with_capacity(text_len);
                for _ in 0..text_len {
                    str_bytes.push(*bytes_it.next()?);
                }
                String::from_utf8(str_bytes).ok()?
            } else {
                "".to_string()
            };
            strings.insert(text, fields);
        }

        Some(Keywords {
            items: strings,
            changed: false,
        })
    }
}

#[derive(Debug)]
pub enum TextIndex {
    None,
    Keyword,
    Tokenized,
    Full(Language),
}

#[derive(Debug)]
pub struct Text {
    pub text: String,
    pub index: TextIndex,
}

impl Text {
    pub fn keyword(keyword: String) -> Self {
        Text {
            text: keyword,
            index: TextIndex::Keyword,
        }
    }

    pub fn tokenized(text: String) -> Self {
        Text {
            text,
            index: TextIndex::Tokenized,
        }
    }

    pub fn not_indexed(text: String) -> Self {
        Text {
            text,
            index: TextIndex::None,
        }
    }

    pub fn fulltext(text: String, detector: &mut LanguageDetector) -> Self {
        Self {
            index: TextIndex::Full(detector.detect(text.as_ref(), MIN_LANGUAGE_SCORE)),
            text,
        }
    }

    pub fn fulltext_lang(text: String, language: Language) -> Self {
        Self {
            text,
            index: TextIndex::Full(language),
        }
    }

    pub fn len(&self) -> usize {
        self.text.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct TokenIterator<'x> {
    tokenizer: Box<dyn Iterator<Item = Token<'x>> + Send + 'x>,
    stemmer: Option<Stemmer>,
    pub stemmed_token: Option<Token<'x>>,
}

impl<'x> TokenIterator<'x> {
    pub fn new(text: &'x str, language: Language, stemming: bool) -> Self {
        TokenIterator {
            tokenizer: tokenize(text, language, MAX_TOKEN_LENGTH),
            stemmer: if stemming {
                Stemmer::new(language)
            } else {
                None
            },
            stemmed_token: None,
        }
    }
}

impl<'x> Iterator for TokenIterator<'x> {
    type Item = Token<'x>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(stemmer) = &self.stemmer {
            if self.stemmed_token.is_some() {
                std::mem::take(&mut self.stemmed_token)
            } else {
                let token = self.tokenizer.next()?;
                self.stemmed_token = stemmer.stem(&token);
                Some(token)
            }
        } else {
            self.tokenizer.next()
        }
    }
}
