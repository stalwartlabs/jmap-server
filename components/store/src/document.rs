use std::{borrow::Cow, collections::HashMap};

use nlp::{lang::detect_language, Language};

use crate::{
    field::{Field, IndexField, TextLang},
    ArrayPos, FieldId, Float, Integer, LongInteger, Tag,
};

pub const MAX_TOKEN_LENGTH: usize = 40;
pub const MAX_ID_LENGTH: usize = 80;
pub const MAX_SORT_FIELD_LENGTH: usize = 255;
pub const MIN_LANGUAGE_SCORE: f64 = 0.5;

#[allow(non_upper_case_globals)]
pub trait IndexOptions {
    const None: OptionValue = 0;
    const Stored: OptionValue = 0x1 << 16;
    const Array: OptionValue = 0x2 << 16;
    const Sortable: OptionValue = 0x4 << 16;

    fn is_stored(&self) -> bool;
    fn is_array(&self) -> bool;
    fn is_sortable(&self) -> bool;
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
    fn is_none(&self) -> bool {
        self & 0xffff0000 == 0
    }

    #[inline(always)]
    fn get_pos(&self) -> ArrayPos {
        *self as ArrayPos
    }
}

pub type OptionValue = u32;

#[derive(Debug)]
struct WeightedAverage {
    weight: usize,
    occurrences: usize,
    confidence: f64,
}

#[derive(Debug)]
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

    pub fn add_keyword(&mut self, field: FieldId, value: Cow<'x, str>, options: OptionValue) {
        if !value.is_empty() {
            self.fields
                .push(IndexField::Keyword(Field::new(field, options, value)));
        }
    }

    pub fn add_text(&mut self, field: FieldId, value: Cow<'x, str>, options: OptionValue) {
        if !value.is_empty() {
            self.fields
                .push(IndexField::Text(Field::new(field, options, value)));
        }
    }

    pub fn add_full_text(
        &mut self,
        field: FieldId,
        value: Cow<'x, str>,
        language: Option<Language>,
        options: OptionValue,
    ) {
        if value.is_empty() {
            return;
        }
        let (language, confidence) = if let Some(language) = language {
            (language, 1.0)
        } else {
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
        };
        self.fields.push(IndexField::FullText(Field::new(
            field,
            options,
            TextLang {
                language,
                confidence,
                text: value,
            },
        )));
    }

    pub fn add_blob(&mut self, field: FieldId, value: Cow<'x, [u8]>, options: OptionValue) {
        self.fields
            .push(IndexField::Blob(Field::new(field, options, value)));
    }

    pub fn add_integer(&mut self, field: FieldId, value: Integer, options: OptionValue) {
        self.fields
            .push(IndexField::Integer(Field::new(field, options, value)));
    }

    pub fn add_long_int(&mut self, field: FieldId, value: LongInteger, options: OptionValue) {
        self.fields
            .push(IndexField::LongInteger(Field::new(field, options, value)));
    }

    pub fn add_tag(&mut self, field: FieldId, value: Tag<'x>, options: OptionValue) {
        self.fields
            .push(IndexField::Tag(Field::new(field, options, value)));
    }

    pub fn add_float(&mut self, field: FieldId, value: Float, options: OptionValue) {
        self.fields
            .push(IndexField::Float(Field::new(field, options, value)));
    }
}

impl<'x> IntoIterator for DocumentBuilder<'x> {
    type Item = IndexField<'x>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(mut self) -> Self::IntoIter {
        if !self.lang_low_score.is_empty() && self.lang_detected.len() > 1 {
            let lang = *self.most_likely_language().unwrap();
            for field_id in self.lang_low_score.drain(..) {
                if let IndexField::FullText(ref mut field) = self.fields[field_id] {
                    field.value.language = lang;
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
