use std::{borrow::Cow, collections::HashMap};

use nlp::{lang::detect_language, Language};

use crate::{
    field::{Field, IndexField, Text},
    FieldId, FieldNumber, Float, Integer, LongInteger, Tag,
};

pub const MAX_TOKEN_LENGTH: usize = 40;
pub const MAX_ID_LENGTH: usize = 80;
pub const MAX_SORT_FIELD_LENGTH: usize = 255;
pub const MIN_LANGUAGE_SCORE: f64 = 0.5;

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

    pub fn add_text(
        &mut self,
        field: FieldId,
        field_num: FieldNumber,
        mut value: Text<'x>,
        stored: bool,
        sorted: bool,
    ) {
        if !match &mut value {
            Text::Default(text) => text.is_empty(),
            Text::Keyword(text) => text.is_empty(),
            Text::Tokenized(text) => text.is_empty(),
            Text::Full((text, language)) => {
                if text.is_empty() {
                    true
                } else if language == &Language::Unknown {
                    let result = detect_language(text.as_ref());
                    let w = self
                        .lang_detected
                        .entry(result.0)
                        .or_insert_with(|| WeightedAverage {
                            weight: 0,
                            confidence: 0.0,
                            occurrences: 0,
                        });
                    w.occurrences += 1;
                    w.weight += text.len();
                    w.confidence += result.1 * text.len() as f64;
                    if result.1 < MIN_LANGUAGE_SCORE {
                        self.lang_low_score.push(self.fields.len());
                    }
                    *language = result.0;
                    false
                } else {
                    false
                }
            }
        } {
            self.fields.push(IndexField::Text(Field::new(
                field, field_num, value, stored, sorted,
            )));
        };
    }

    pub fn add_blob(&mut self, field: FieldId, field_num: FieldNumber, value: Cow<'x, [u8]>) {
        self.fields.push(IndexField::Blob(Field::new(
            field, field_num, value, true, false,
        )));
    }

    pub fn add_integer(
        &mut self,
        field: FieldId,
        field_num: FieldNumber,
        value: Integer,
        stored: bool,
        sorted: bool,
    ) {
        self.fields.push(IndexField::Integer(Field::new(
            field, field_num, value, stored, sorted,
        )));
    }

    pub fn add_long_int(
        &mut self,
        field: FieldId,
        field_num: FieldNumber,
        value: LongInteger,
        stored: bool,
        sorted: bool,
    ) {
        self.fields.push(IndexField::LongInteger(Field::new(
            field, field_num, value, stored, sorted,
        )));
    }

    pub fn add_tag(&mut self, field: FieldId, value: Tag<'x>) {
        self.fields
            .push(IndexField::Tag(Field::new(field, 0, value, false, false)));
    }

    pub fn add_float(
        &mut self,
        field: FieldId,
        field_num: FieldNumber,
        value: Float,
        stored: bool,
        sorted: bool,
    ) {
        self.fields.push(IndexField::Float(Field::new(
            field, field_num, value, stored, sorted,
        )));
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
                    if let Text::Full((_, language)) = &mut field.value {
                        *language = lang;
                    }
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
