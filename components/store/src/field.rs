use nlp::{
    lang::{LanguageDetector, MIN_LANGUAGE_SCORE},
    stemmer::Stemmer,
    tokenizers::{tokenize, Token},
    Language,
};

use crate::{
    batch::MAX_TOKEN_LENGTH, blob::BlobIndex, DocumentId, FieldId, Float, Integer, LongInteger,
    Tag, TagId,
};

#[derive(Debug)]
pub enum UpdateField {
    Text(Field<Text>),
    Binary(Field<Vec<u8>>),
    Integer(Field<Integer>),
    LongInteger(Field<LongInteger>),
    Tag(Field<Tag>),
    Float(Field<Float>),
}

impl UpdateField {
    pub fn len(&self) -> usize {
        match self {
            UpdateField::Text(t) => t.value.len(),
            UpdateField::Binary(b) => b.value.len(),
            UpdateField::Integer(i) => i.size_of(),
            UpdateField::LongInteger(li) => li.size_of(),
            UpdateField::Tag(t) => t.value.len(),
            UpdateField::Float(f) => f.size_of(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get_field(&self) -> &FieldId {
        match self {
            UpdateField::Text(t) => &t.field,
            UpdateField::Binary(b) => &b.field,
            UpdateField::Integer(i) => &i.field,
            UpdateField::LongInteger(li) => &li.field,
            UpdateField::Tag(t) => &t.field,
            UpdateField::Float(f) => &f.field,
        }
    }

    pub fn unwrap_text(&self) -> &Field<Text> {
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
    StoreAsBlob(BlobIndex),
    Clear,
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

    pub fn get_blob_index(&self) -> Option<BlobIndex> {
        match self.options {
            FieldOptions::StoreAsBlob(idx) => Some(idx),
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

    pub fn is_clear(&self) -> bool {
        matches!(self.options, FieldOptions::Clear)
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug)]
pub struct FullText {
    pub text: String,
    pub language: Language,
}

impl FullText {
    pub fn new(text: String, detector: &mut LanguageDetector) -> Self {
        Self {
            language: detector.detect(text.as_ref(), MIN_LANGUAGE_SCORE),
            text,
        }
    }

    pub fn new_lang(text: String, language: Language) -> Self {
        Self { text, language }
    }
}

#[derive(Debug)]
pub enum Text {
    Default(String),
    Keyword(String),
    Tokenized(String),
    Full(FullText),
}

impl Text {
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
