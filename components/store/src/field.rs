use std::borrow::Cow;

use nlp::{
    lang::{LanguageDetector, MIN_LANGUAGE_SCORE},
    stemmer::Stemmer,
    tokenizers::{tokenize, Token},
    Language,
};

use crate::{
    batch::MAX_TOKEN_LENGTH, BlobId, DocumentId, FieldId, Float, Integer, LongInteger, Tag, TagId,
};

#[derive(Debug)]
pub enum UpdateField<'x> {
    Text(Field<Text<'x>>),
    Blob(Field<Cow<'x, [u8]>>),
    Integer(Field<Integer>),
    LongInteger(Field<LongInteger>),
    TagSet(Field<Tag<'x>>),
    TagRemove(Field<Tag<'x>>),
    Float(Field<Float>),
}

impl<'x> UpdateField<'x> {
    pub fn len(&'x self) -> usize {
        match self {
            UpdateField::Text(t) => t.value.len(),
            UpdateField::Blob(b) => b.value.len(),
            UpdateField::Integer(i) => i.size_of(),
            UpdateField::LongInteger(li) => li.size_of(),
            UpdateField::TagSet(t) => t.value.len(),
            UpdateField::TagRemove(t) => t.value.len(),
            UpdateField::Float(f) => f.size_of(),
        }
    }

    pub fn is_empty(&'x self) -> bool {
        self.len() == 0
    }

    pub fn get_field(&self) -> &FieldId {
        match self {
            UpdateField::Text(t) => &t.field,
            UpdateField::Blob(b) => &b.field,
            UpdateField::Integer(i) => &i.field,
            UpdateField::LongInteger(li) => &li.field,
            UpdateField::TagSet(t) => &t.field,
            UpdateField::TagRemove(t) => &t.field,
            UpdateField::Float(f) => &f.field,
        }
    }

    pub fn unwrap_text(&'x self) -> &Field<Text<'x>> {
        match self {
            UpdateField::Text(t) => t,
            _ => panic!("unwrap_text called on non-text field"),
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
    pub options: FieldOptions,
    pub value: T,
}

#[derive(Debug, Clone, Copy)]
pub enum FieldOptions {
    None,
    Store,
    Sort,
    StoreAndSort,
    BlobStore(BlobId),
}

impl<T> Field<T> {
    pub fn new(field: FieldId, value: T, options: FieldOptions) -> Self {
        Self {
            field,
            value,
            options,
        }
    }

    pub fn get_field(&self) -> FieldId {
        self.field
    }

    pub fn get_options(&self) -> FieldOptions {
        self.options
    }

    pub fn get_blob_id(&self) -> Option<BlobId> {
        match self.options {
            FieldOptions::BlobStore(id) => Some(id),
            _ => None,
        }
    }

    pub fn is_sorted(&self) -> bool {
        matches!(
            self.options,
            FieldOptions::Sort | FieldOptions::StoreAndSort
        )
    }

    pub fn is_stored(&self) -> bool {
        matches!(
            self.options,
            FieldOptions::Store | FieldOptions::StoreAndSort
        )
    }

    pub fn size_of(&self) -> usize {
        std::mem::size_of::<T>()
    }
}

impl<'x> Tag<'x> {
    pub fn len(&self) -> usize {
        match self {
            Tag::Static(_) => std::mem::size_of::<TagId>(),
            Tag::Id(_) => std::mem::size_of::<DocumentId>(),
            Tag::Text(text) => text.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug)]
pub struct FullText<'x> {
    pub text: Cow<'x, str>,
    pub language: Language,
}

impl<'x> FullText<'x> {
    pub fn new(text: Cow<'x, str>, detector: &mut LanguageDetector) -> Self {
        Self {
            language: detector.detect(text.as_ref(), MIN_LANGUAGE_SCORE),
            text,
        }
    }

    pub fn new_lang(text: Cow<'x, str>, language: Language) -> Self {
        Self { text, language }
    }
}

#[derive(Debug)]
pub enum Text<'x> {
    Default(Cow<'x, str>),
    Keyword(Cow<'x, str>),
    Tokenized(Cow<'x, str>),
    Full(FullText<'x>),
}

impl<'x> Text<'x> {
    pub fn len(&self) -> usize {
        match self {
            Text::Default(s) => s.len(),
            Text::Keyword(s) => s.len(),
            Text::Tokenized(s) => s.len(),
            Text::Full(ft) => ft.text.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct TokenIterator<'x> {
    tokenizer: Box<dyn Iterator<Item = Token<'x>> + 'x>,
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
