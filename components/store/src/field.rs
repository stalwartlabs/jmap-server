use std::borrow::Cow;

use nlp::{
    stemmer::Stemmer,
    tokenizers::{tokenize, Token},
    Language,
};

use crate::{
    batch::MAX_TOKEN_LENGTH, DocumentId, FieldId, FieldNumber, Float, Integer, LongInteger, Tag,
    TagId,
};

#[derive(Debug)]
pub enum IndexField<'x> {
    Text(Field<Text<'x>>),
    Blob(Field<Cow<'x, [u8]>>),
    Integer(Field<Integer>),
    LongInteger(Field<LongInteger>),
    Tag(Field<Tag<'x>>),
    Float(Field<Float>),
}

impl<'x> IndexField<'x> {
    pub fn len(&'x self) -> usize {
        match self {
            IndexField::Text(t) => t.value.len(),
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

    pub fn unwrap_text(&'x self) -> &Field<Text<'x>> {
        match self {
            IndexField::Text(t) => t,
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
    pub field_num: FieldNumber,
    pub sorted: bool,
    pub stored: bool,
    pub value: T,
}

impl<T> Field<T> {
    pub fn new(
        field: FieldId,
        field_num: FieldNumber,
        value: T,
        stored: bool,
        sorted: bool,
    ) -> Self {
        Self {
            field,
            field_num,
            value,
            sorted,
            stored,
        }
    }

    pub fn get_field(&self) -> FieldId {
        self.field
    }

    pub fn get_field_num(&self) -> FieldNumber {
        self.field_num
    }

    pub fn is_sorted(&self) -> bool {
        self.sorted
    }

    pub fn is_stored(&self) -> bool {
        self.stored
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
pub enum Text<'x> {
    Default(Cow<'x, str>),
    Keyword(Cow<'x, str>),
    Tokenized(Cow<'x, str>),
    Full((Cow<'x, str>, Language)),
}

impl<'x> Text<'x> {
    pub fn len(&self) -> usize {
        match self {
            Text::Default(s) => s.len(),
            Text::Keyword(s) => s.len(),
            Text::Tokenized(s) => s.len(),
            Text::Full((s, _)) => s.len(),
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
