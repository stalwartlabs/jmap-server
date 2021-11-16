use std::{borrow::Cow, collections::HashMap};

use nlp::{
    lang::detect_language,
    stemmer::Stemmer,
    tokenizers::{tokenize, Token},
    Language,
};

use crate::{AccountId, CollectionId, FieldId, Float, Integer, LongInteger, Tag};

pub const MAX_TOKEN_LENGTH: usize = 40;
pub const MAX_ID_LENGTH: usize = 80;
pub const MAX_SORT_FIELD_LENGTH: usize = 255;

#[allow(non_upper_case_globals)]
pub trait IndexOptions {
    const None: IndexFieldOptions = 0;
    const Stored: IndexFieldOptions = 1 << 16;
    const Array: IndexFieldOptions = 2 << 16;
    const Sortable: IndexFieldOptions = 4 << 16;
    const Tokenized: IndexFieldOptions = 8 << 16;
    const FullText: IndexFieldOptions = 10 << 16;

    fn is_stored(&self) -> bool;
    fn is_array(&self) -> bool;
    fn is_sortable(&self) -> bool;
    fn is_tokenized(&self) -> bool;
    fn is_full_text(&self) -> bool;
    fn is_none(&self) -> bool;
    fn get_pos(&self) -> u16;

    #[inline(always)]
    fn set_pos(pos: usize) -> IndexFieldOptions {
        Self::Array | ((pos as IndexFieldOptions) & 0xffff)
    }    
}

#[allow(non_upper_case_globals)]
impl IndexOptions for IndexFieldOptions {
    fn is_stored(&self) -> bool {
        self & Self::Stored != 0
    }

    fn is_array(&self) -> bool {
        self & Self::Array != 0
    }

    fn is_sortable(&self) -> bool {
        self & Self::Sortable != 0
    }

    fn is_tokenized(&self) -> bool {
        self & Self::Tokenized != 0
    }

    fn is_full_text(&self) -> bool {
        self & Self::FullText != 0
    }

    fn is_none(&self) -> bool {
        self & 0xffff0000 == 0
    }

    fn get_pos(&self) -> u16 {
        *self as u16
    }
}

pub struct TextField<'x> {
    field: FieldId,
    value: Cow<'x, str>,
    language: Language,
    confidence: f64,
    options: IndexFieldOptions,
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
    field: FieldId,
    value: Cow<'x, [u8]>,
    options: IndexFieldOptions,
}

pub struct IntegerField {
    field: FieldId,
    value: Integer,
    options: IndexFieldOptions,
}

pub struct LongIntegerField {
    field: FieldId,
    value: LongInteger,
    options: IndexFieldOptions,
}

pub struct FloatField {
    field: FieldId,
    value: Float,
    options: IndexFieldOptions,
}

pub struct TagField<'x> {
    field: FieldId,
    value: Tag<'x>,
    options: IndexFieldOptions,
}

pub enum IndexField<'x> {
    Text(TextField<'x>),
    Blob(BlobField<'x>),
    Integer(IntegerField),
    LongInteger(LongIntegerField),
    Tag(TagField<'x>),
    Float(FloatField),
}

pub type IndexFieldOptions = u32;

pub struct WeightedAverage {
    weight: usize,
    occurrences: usize,
    confidence: f64,
}

pub struct JMAPObjectBuilder<'x> {
    pub account_id: AccountId,
    pub collection_id: CollectionId,

    pub fields: Vec<IndexField<'x>>,
    pub languages: HashMap<Language, WeightedAverage>,
}

impl<'x> JMAPObjectBuilder<'x> {
    pub fn new(account_id: AccountId, collection_id: CollectionId) -> JMAPObjectBuilder<'x> {
        JMAPObjectBuilder {
            account_id,
            collection_id,
            fields: Vec::new(),
            languages: HashMap::new(),
        }
    }

    pub fn most_likely_language(&self) -> Option<&Language> {
        self.languages
            .iter()
            .max_by(|(_, a), (_, b)| {
                ((a.confidence / a.weight as f64) * a.occurrences as f64)
                    .partial_cmp(&((b.confidence / b.weight as f64) * b.occurrences as f64))
                    .unwrap_or(std::cmp::Ordering::Less)
            })
            .map(|(l, _)| l)
    }

    pub fn add_text(&mut self, field: FieldId, value: Cow<'x, str>, options: IndexFieldOptions) {
        if !value.is_empty() {
            let (language, confidence) = if options.is_full_text() {
                let result = detect_language(&value);
                let w = self
                    .languages
                    .entry(result.0)
                    .or_insert_with(|| WeightedAverage {
                        weight: 0,
                        confidence: 0.0,
                        occurrences: 0,
                    });
                w.occurrences += 1;
                w.weight += value.len();
                w.confidence += result.1 * value.len() as f64;
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

    pub fn add_blob(&mut self, field: FieldId, value: Cow<'x, [u8]>, options: IndexFieldOptions) {
        self.fields.push(IndexField::Blob(BlobField {
            field,
            value,
            options,
        }));
    }

    pub fn add_integer(&mut self, field: FieldId, value: Integer, options: IndexFieldOptions) {
        self.fields.push(IndexField::Integer(IntegerField {
            field,
            value,
            options,
        }));
    }

    pub fn add_long_int(&mut self, field: FieldId, value: LongInteger, options: IndexFieldOptions) {
        self.fields.push(IndexField::LongInteger(LongIntegerField {
            field,
            value,
            options,
        }));
    }

    pub fn add_tag(&mut self, field: FieldId, value: Tag<'x>, options: IndexFieldOptions) {
        self.fields.push(IndexField::Tag(TagField {
            field,
            value,
            options,
        }));
    }

    pub fn add_float(&mut self, field: FieldId, value: Float, options: IndexFieldOptions) {
        self.fields.push(IndexField::Float(FloatField {
            field,
            value,
            options,
        }));
    }

    pub fn iter(&'x mut self) -> JMAPObjectBuilderIterator<'x> {
        JMAPObjectBuilderIterator {
            default_lang: *self.most_likely_language().unwrap_or(&Language::English),
            iter: self.fields.iter_mut(),
        }
    }
}

pub struct JMAPObjectBuilderIterator<'x> {
    iter: std::slice::IterMut<'x, IndexField<'x>>,
    default_lang: Language,
}

impl<'x> Iterator for JMAPObjectBuilderIterator<'x> {
    type Item = &'x mut IndexField<'x>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.iter.next();
        if let Some(IndexField::Text(text)) = result {
            if text.confidence < 0.5 {
                text.language = self.default_lang;
            }
        }
        result
    }
}

//    pub ft_tokens: BTreeMap<Cow<'x, str>, Vec<Token<'x>>>,

/*pub fn add_text_token(
    &mut self,
    field_id: FieldId,
    mut token: Token<'x>,
) {
    token.field_id = field_id;
    self.ft_tokens_len += 1;
    match self.ft_tokens.entry(std::mem::take(&mut token.word)) {
        Entry::Vacant(e) => {
            e.insert(vec![token]);
        }
        Entry::Occupied(e) => {
            e.into_mut().push(token);
        }
    }
}

        match self.tokens.entry(text) {
        Entry::Vacant(e) => {
            e.insert({
                let mut h = HashSet::new();
                h.insert(field_id);
                h
            });
        }
        Entry::Occupied(e) => {
            e.into_mut().insert(field_id);
        }
    }


*/

#[cfg(test)]
mod tests {
    use nlp::Language;

    use super::{JMAPObjectBuilder, WeightedAverage};

    #[test]
    fn weighted_language() {
        let mut builder = JMAPObjectBuilder::new(0, 0);
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
                .languages
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
