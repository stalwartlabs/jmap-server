use std::{borrow::Cow, collections::HashMap};

use nlp::{
    lang::detect_language,
    stemmer::Stemmer,
    tokenizers::{tokenize, Token},
    Language,
};

use crate::{
    AccountId, ArrayPos, CollectionId, DocumentId, FieldId, Float, Integer, LongInteger, Tag, TagId,
};

pub const MAX_TOKEN_LENGTH: usize = 40;
pub const MAX_ID_LENGTH: usize = 80;
pub const MAX_SORT_FIELD_LENGTH: usize = 255;
pub const MIN_LANGUAGE_SCORE: f64 = 0.5;

#[allow(non_upper_case_globals)]
pub trait IndexOptions {
    const None: OptionValue = 0;
    const Stored: OptionValue = 1 << 16;
    const Array: OptionValue = 2 << 16;
    const Sortable: OptionValue = 4 << 16;
    const Tokenized: OptionValue = 8 << 16;
    const FullText: OptionValue = 10 << 16 | 8 << 16;

    fn is_stored(&self) -> bool;
    fn is_array(&self) -> bool;
    fn is_sortable(&self) -> bool;
    fn is_tokenized(&self) -> bool;
    fn is_full_text(&self) -> bool;
    fn is_none(&self) -> bool;
    fn get_pos(&self) -> ArrayPos;
    fn set_pos(pos: usize) -> OptionValue;
}

#[allow(non_upper_case_globals)]
impl IndexOptions for OptionValue {
    #[inline(always)]
    fn set_pos(pos: usize) -> OptionValue {
        Self::Array | ((pos as OptionValue) & 0xffff)
    }

    #[inline(always)]
    fn is_stored(&self) -> bool {
        self & Self::Stored != 0
    }

    #[inline(always)]
    fn is_array(&self) -> bool {
        self & Self::Array != 0
    }

    #[inline(always)]
    fn is_sortable(&self) -> bool {
        self & Self::Sortable != 0
    }

    #[inline(always)]
    fn is_tokenized(&self) -> bool {
        self & Self::Tokenized != 0
    }

    #[inline(always)]
    fn is_full_text(&self) -> bool {
        self & Self::FullText != 0
    }

    #[inline(always)]
    fn is_none(&self) -> bool {
        self & 0xffff0000 == 0
    }

    #[inline(always)]
    fn get_pos(&self) -> ArrayPos {
        *self as ArrayPos
    }
}

pub trait Field<'x> {
    fn get_field(&self) -> &FieldId;
    fn get_options(&self) -> &OptionValue;
    fn len(&self) -> usize;
}

pub struct TextField<'x> {
    pub field: FieldId,
    pub value: Cow<'x, str>,
    pub language: Language,
    pub confidence: f64,
    pub options: OptionValue,
}

pub struct TokenIterator<'x> {
    tokenizer: Box<dyn Iterator<Item = Token<'x>> + 'x>,
    stemmer: Option<Stemmer>,
    next_token: Option<Token<'x>>,
}

impl<'x> TextField<'x> {
    pub fn tokenize(&'x self) -> TokenIterator<'x> {
        TokenIterator {
            tokenizer: tokenize(&self.value, self.language, MAX_TOKEN_LENGTH),
            stemmer: if self.options.is_full_text() {
                Stemmer::new(self.language)
            } else {
                None
            },
            next_token: None,
        }
    }
}

impl<'x> Field<'x> for TextField<'x> {
    fn get_field(&self) -> &FieldId {
        &self.field
    }

    fn get_options(&self) -> &OptionValue {
        &self.options
    }

    fn len(&self) -> usize {
        self.value.len()
    }
}

impl<'x> Iterator for TokenIterator<'x> {
    type Item = Token<'x>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(stemmer) = &self.stemmer {
            if self.next_token.is_some() {
                std::mem::take(&mut self.next_token)
            } else {
                let token = self.tokenizer.next()?;
                self.next_token = stemmer.stem(&token);
                Some(token)
            }
        } else {
            self.tokenizer.next()
        }
    }
}

pub struct BlobField<'x> {
    pub field: FieldId,
    pub value: Cow<'x, [u8]>,
    pub options: OptionValue,
}

impl<'x> Field<'x> for BlobField<'x> {
    fn get_field(&self) -> &FieldId {
        &self.field
    }

    fn get_options(&self) -> &OptionValue {
        &self.options
    }

    fn len(&self) -> usize {
        self.value.len()
    }
}

pub struct IntegerField {
    pub field: FieldId,
    pub value: Integer,
    pub options: OptionValue,
}

impl<'x> Field<'x> for IntegerField {
    fn get_field(&self) -> &FieldId {
        &self.field
    }

    fn get_options(&self) -> &OptionValue {
        &self.options
    }

    fn len(&self) -> usize {
        std::mem::size_of::<Integer>()
    }
}

pub struct LongIntegerField {
    pub field: FieldId,
    pub value: LongInteger,
    pub options: OptionValue,
}

impl<'x> Field<'x> for LongIntegerField {
    fn get_field(&self) -> &FieldId {
        &self.field
    }

    fn get_options(&self) -> &OptionValue {
        &self.options
    }

    fn len(&self) -> usize {
        std::mem::size_of::<LongInteger>()
    }
}

pub struct FloatField {
    pub field: FieldId,
    pub value: Float,
    pub options: OptionValue,
}

impl<'x> Field<'x> for FloatField {
    fn get_field(&self) -> &FieldId {
        &self.field
    }

    fn get_options(&self) -> &OptionValue {
        &self.options
    }

    fn len(&self) -> usize {
        std::mem::size_of::<Float>()
    }
}

pub struct TagField<'x> {
    pub field: FieldId,
    pub value: Tag<'x>,
    pub options: OptionValue,
}

impl<'x> Field<'x> for TagField<'x> {
    fn get_field(&self) -> &FieldId {
        &self.field
    }

    fn get_options(&self) -> &OptionValue {
        &self.options
    }

    fn len(&self) -> usize {
        match self.value {
            Tag::Static(_) => std::mem::size_of::<TagId>(),
            Tag::Id(_) => std::mem::size_of::<DocumentId>(),
            Tag::Text(text) => text.len(),
        }
    }
}

pub enum IndexField<'x> {
    Text(TextField<'x>),
    Blob(BlobField<'x>),
    Integer(IntegerField),
    LongInteger(LongIntegerField),
    Tag(TagField<'x>),
    Float(FloatField),
}

impl<'x> IndexField<'x> {
    pub fn unwrap(&'x self) -> &dyn Field<'x> {
        match self {
            IndexField::Text(t) => t,
            IndexField::Blob(b) => b,
            IndexField::Integer(i) => i,
            IndexField::LongInteger(li) => li,
            IndexField::Tag(t) => t,
            IndexField::Float(f) => f,
        }
    }
    pub fn unwrap_text(&'x self) -> &TextField {
        match self {
            IndexField::Text(t) => t,
            _ => panic!("unwrap_text called on non-text field"),
        }
    }
}

pub type OptionValue = u32;

struct WeightedAverage {
    weight: usize,
    occurrences: usize,
    confidence: f64,
}

pub struct DocumentBuilder<'x> {
    fields: Vec<IndexField<'x>>,
    lang_detected: HashMap<Language, WeightedAverage>,
    lang_low_score: Vec<usize>,
}

impl<'x> Default for DocumentBuilder<'x> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'x> DocumentBuilder<'x> {
    pub fn new() -> DocumentBuilder<'x> {
        DocumentBuilder {
            fields: Vec::new(),
            lang_detected: HashMap::new(),
            lang_low_score: Vec::new(),
        }
    }

    pub fn most_likely_language(&self) -> Option<&Language> {
        self.lang_detected
            .iter()
            .max_by(|(_, a), (_, b)| {
                ((a.confidence / a.weight as f64) * a.occurrences as f64)
                    .partial_cmp(&((b.confidence / b.weight as f64) * b.occurrences as f64))
                    .unwrap_or(std::cmp::Ordering::Less)
            })
            .map(|(l, _)| l)
    }

    pub fn add_text(&mut self, field: FieldId, value: Cow<'x, str>, options: OptionValue) {
        if !value.is_empty() {
            let (language, confidence) = if options.is_full_text() {
                let result = detect_language(&value);
                let w = self
                    .lang_detected
                    .entry(result.0)
                    .or_insert_with(|| WeightedAverage {
                        weight: 0,
                        confidence: 0.0,
                        occurrences: 0,
                    });
                w.occurrences += 1;
                w.weight += value.len();
                w.confidence += result.1 * value.len() as f64;
                if result.1 < MIN_LANGUAGE_SCORE {
                    self.lang_low_score.push(self.fields.len());
                }
                result
            } else {
                (Language::English, 1.0)
            };
            self.fields.push(IndexField::Text(TextField {
                field,
                value,
                language,
                confidence,
                options,
            }));
        }
    }

    pub fn add_blob(&mut self, field: FieldId, value: Cow<'x, [u8]>, options: OptionValue) {
        self.fields.push(IndexField::Blob(BlobField {
            field,
            value,
            options,
        }));
    }

    pub fn add_integer(&mut self, field: FieldId, value: Integer, options: OptionValue) {
        self.fields.push(IndexField::Integer(IntegerField {
            field,
            value,
            options,
        }));
    }

    pub fn add_long_int(&mut self, field: FieldId, value: LongInteger, options: OptionValue) {
        self.fields.push(IndexField::LongInteger(LongIntegerField {
            field,
            value,
            options,
        }));
    }

    pub fn add_tag(&mut self, field: FieldId, value: Tag<'x>, options: OptionValue) {
        self.fields.push(IndexField::Tag(TagField {
            field,
            value,
            options,
        }));
    }

    pub fn add_float(&mut self, field: FieldId, value: Float, options: OptionValue) {
        self.fields.push(IndexField::Float(FloatField {
            field,
            value,
            options,
        }));
    }
}

impl<'x> IntoIterator for DocumentBuilder<'x> {
    type Item = IndexField<'x>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(mut self) -> Self::IntoIter {
        if !self.lang_low_score.is_empty() && self.lang_detected.len() > 1 {
            let lang = *self.most_likely_language().unwrap();
            for field_id in self.lang_low_score.drain(..) {
                if let IndexField::Text(ref mut field) = self.fields[field_id] {
                    field.language = lang;
                } else {
                    debug_assert!(false, "Unexpected field type.");
                }
            }
        }
        self.fields.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use nlp::Language;

    use super::{DocumentBuilder, WeightedAverage};

    #[test]
    fn weighted_language() {
        let mut builder = DocumentBuilder::new();
        for lang in [
            (Language::Spanish, 0.5, 70),
            (Language::Japanese, 0.2, 100),
            (Language::Japanese, 0.3, 100),
            (Language::Japanese, 0.4, 200),
            (Language::English, 0.7, 50),
        ]
        .iter()
        {
            let w = builder
                .lang_detected
                .entry(lang.0)
                .or_insert_with(|| WeightedAverage {
                    weight: 0,
                    confidence: 0.0,
                    occurrences: 0,
                });
            w.occurrences += 1;
            w.weight += lang.2;
            w.confidence += lang.1 * lang.2 as f64;
        }
        assert_eq!(builder.most_likely_language(), Some(&Language::Japanese));
    }
}
