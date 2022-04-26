pub mod indo_european;
pub mod japanese;

use std::borrow::Cow;

use crate::Language;

use self::{indo_european::IndoEuropeanTokenizer, japanese::JapaneseTokenizer};

#[derive(Debug, PartialEq)]
pub struct Token<'x> {
    pub word: Cow<'x, str>,
    pub offset: u32, // Word offset in the text part
    pub len: u8,     // Word length
}

impl<'x> Token<'x> {
    pub fn new(offset: usize, len: usize, word: Cow<'x, str>) -> Token<'x> {
        debug_assert!(offset <= u32::max_value() as usize);
        debug_assert!(len <= u8::max_value() as usize);
        Token {
            offset: offset as u32,
            len: len as u8,
            word,
        }
    }
}

//TODO: Implement this for all languages
enum LanguageTokenizer<'x> {
    IndoEuropean(IndoEuropeanTokenizer<'x>),
    Japanese(JapaneseTokenizer<'x>),
}

pub struct Tokenizer<'x> {
    tokenizer: LanguageTokenizer<'x>,
}

impl<'x> Tokenizer<'x> {
    pub fn new(text: &'x str, language: Language, max_token_length: usize) -> Self {
        Tokenizer {
            tokenizer: match language {
                Language::Japanese => {
                    LanguageTokenizer::Japanese(JapaneseTokenizer::new(text, max_token_length))
                }
                _ => LanguageTokenizer::IndoEuropean(IndoEuropeanTokenizer::new(
                    text,
                    max_token_length,
                )),
            },
        }
    }
}

impl<'x> Iterator for Tokenizer<'x> {
    type Item = Token<'x>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.tokenizer {
            LanguageTokenizer::IndoEuropean(tokenizer) => tokenizer.next(),
            LanguageTokenizer::Japanese(tokenizer) => tokenizer.next(),
        }
    }
}
