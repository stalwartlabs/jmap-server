use std::borrow::Cow;

use nlp::{
    stemmer::Stemmer,
    tokenizers::{tokenize, Token},
    Language,
};

use crate::{
    document::{IndexOptions, OptionValue, MAX_TOKEN_LENGTH},
    DocumentId, FieldId, Float, Integer, LongInteger, Tag, TagId,
};

#[derive(Debug)]
pub enum IndexField<'x> {
    Text(Field<TextLang<'x>>),
    Blob(Field<Cow<'x, [u8]>>),
    Integer(Field<Integer>),
    LongInteger(Field<LongInteger>),
    Tag(Field<Tag<'x>>),
    Float(Field<Float>),
}

impl<'x> IndexField<'x> {
    pub fn len(&'x self) -> usize {
        match self {
            IndexField::Text(t) => t.value.text.len(),
            IndexField::Blob(b) => b.value.len(),
            IndexField::Integer(i) => i.size_of(),
            IndexField::LongInteger(li) => li.size_of(),
            IndexField::Tag(t) => t.value.len(),
            IndexField::Float(f) => f.size_of(),
        }
    }

    pub fn is_empty(&'x self) -> bool {
        self.len() == 0
    }

    pub fn get_field(&self) -> &FieldId {
        match self {
            IndexField::Text(t) => &t.field,
            IndexField::Blob(b) => &b.field,
            IndexField::Integer(i) => &i.field,
            IndexField::LongInteger(li) => &li.field,
            IndexField::Tag(t) => &t.field,
            IndexField::Float(f) => &f.field,
        }
    }

    pub fn get_options(&self) -> &OptionValue {
        match self {
            IndexField::Text(t) => &t.options,
            IndexField::Blob(b) => &b.options,
            IndexField::Integer(i) => &i.options,
            IndexField::LongInteger(li) => &li.options,
            IndexField::Tag(t) => &t.options,
            IndexField::Float(f) => &f.options,
        }
    }

    pub fn unwrap_text(&'x self) -> &Field<TextLang<'x>> {
        match self {
            IndexField::Text(t) => t,
            _ => panic!("unwrap_text called on non-text field"),
        }
    }
}

pub trait FieldLen {
    fn len(&self) -> usize;
}

#[derive(Debug)]
pub struct Field<T> {
    pub field: FieldId,
    pub options: OptionValue,
    pub value: T,
}

impl<T> Field<T> {
    pub fn new(field: FieldId, options: OptionValue, value: T) -> Self {
        Self {
            field,
            options,
            value,
        }
    }

    pub fn get_field(&self) -> &FieldId {
        &self.field
    }

    pub fn get_options(&self) -> &OptionValue {
        &self.options
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
pub struct TextLang<'x> {
    pub text: Cow<'x, str>,
    pub language: Language,
    pub confidence: f64,
}

impl<'x> Field<TextLang<'x>> {
    pub fn tokenize(&'x self) -> TokenIterator<'x> {
        TokenIterator::new(
            &self.value.text,
            self.value.language,
            self.options.is_full_text(),
        )
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
