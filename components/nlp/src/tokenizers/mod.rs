pub mod indo_european;
pub mod japanese;

use std::borrow::Cow;

use crate::Language;

#[derive(Debug, PartialEq)]
pub struct Token<'x> {
    pub word: Cow<'x, str>,
    pub offset: u32,    // Word offset in the text part
    pub len: u8,        // Word length
    pub is_exact: bool, // True if the token is an exact match
}

impl<'x> Token<'x> {
    pub fn new(offset: usize, len: usize, word: Cow<'x, str>) -> Token<'x> {
        debug_assert!(offset <= u32::max_value() as usize);
        debug_assert!(len <= u8::max_value() as usize);
        Token {
            offset: offset as u32,
            len: len as u8,
            word,
            is_exact: true,
        }
    }
}

pub fn tokenize<'x>(
    text: &'x str,
    language: Language,
    max_token_length: usize,
) -> Box<dyn Iterator<Item = Token<'x>> + 'x> {
    TOKENIZER_MAP[language as usize](text, max_token_length)
}

#[allow(clippy::type_complexity)]
static TOKENIZER_MAP: &[for<'x, 'y> fn(
    &'x str,
    usize,
) -> Box<dyn Iterator<Item = Token<'x>> + 'x>] = &[
    indo_european::new_tokenizer, // Esperanto = 0,
    indo_european::new_tokenizer, // English = 1,
    indo_european::new_tokenizer, // Russian = 2,
    indo_european::new_tokenizer, // Mandarin = 3,
    indo_european::new_tokenizer, // Spanish = 4,
    indo_european::new_tokenizer, // Portuguese = 5,
    indo_european::new_tokenizer, // Italian = 6,
    indo_european::new_tokenizer, // Bengali = 7,
    indo_european::new_tokenizer, // French = 8,
    indo_european::new_tokenizer, // German = 9,
    indo_european::new_tokenizer, // Ukrainian = 10,
    indo_european::new_tokenizer, // Georgian = 11,
    indo_european::new_tokenizer, // Arabic = 12,
    indo_european::new_tokenizer, // Hindi = 13,
    japanese::new_tokenizer,      // Japanese = 14,
    indo_european::new_tokenizer, // Hebrew = 15,
    indo_european::new_tokenizer, // Yiddish = 16,
    indo_european::new_tokenizer, // Polish = 17,
    indo_european::new_tokenizer, // Amharic = 18,
    indo_european::new_tokenizer, // Javanese = 19,
    indo_european::new_tokenizer, // Korean = 20,
    indo_european::new_tokenizer, // Bokmal = 21,
    indo_european::new_tokenizer, // Danish = 22,
    indo_european::new_tokenizer, // Swedish = 23,
    indo_european::new_tokenizer, // Finnish = 24,
    indo_european::new_tokenizer, // Turkish = 25,
    indo_european::new_tokenizer, // Dutch = 26,
    indo_european::new_tokenizer, // Hungarian = 27,
    indo_european::new_tokenizer, // Czech = 28,
    indo_european::new_tokenizer, // Greek = 29,
    indo_european::new_tokenizer, // Bulgarian = 30,
    indo_european::new_tokenizer, // Belarusian = 31,
    indo_european::new_tokenizer, // Marathi = 32,
    indo_european::new_tokenizer, // Kannada = 33,
    indo_european::new_tokenizer, // Romanian = 34,
    indo_european::new_tokenizer, // Slovene = 35,
    indo_european::new_tokenizer, // Croatian = 36,
    indo_european::new_tokenizer, // Serbian = 37,
    indo_european::new_tokenizer, // Macedonian = 38,
    indo_european::new_tokenizer, // Lithuanian = 39,
    indo_european::new_tokenizer, // Latvian = 40,
    indo_european::new_tokenizer, // Estonian = 41,
    indo_european::new_tokenizer, // Tamil = 42,
    indo_european::new_tokenizer, // Vietnamese = 43,
    indo_european::new_tokenizer, // Urdu = 44,
    indo_european::new_tokenizer, // Thai = 45,
    indo_european::new_tokenizer, // Gujarati = 46,
    indo_european::new_tokenizer, // Uzbek = 47,
    indo_european::new_tokenizer, // Punjabi = 48,
    indo_european::new_tokenizer, // Azerbaijani = 49,
    indo_european::new_tokenizer, // Indonesian = 50,
    indo_european::new_tokenizer, // Telugu = 51,
    indo_european::new_tokenizer, // Persian = 52,
    indo_european::new_tokenizer, // Malayalam = 53,
    indo_european::new_tokenizer, // Oriya = 54,
    indo_european::new_tokenizer, // Burmese = 55,
    indo_european::new_tokenizer, // Nepali = 56,
    indo_european::new_tokenizer, // Sinhalese = 57,
    indo_european::new_tokenizer, // Khmer = 58,
    indo_european::new_tokenizer, // Turkmen = 59,
    indo_european::new_tokenizer, // Akan = 60,
    indo_european::new_tokenizer, // Zulu = 61,
    indo_european::new_tokenizer, // Shona = 62,
    indo_european::new_tokenizer, // Afrikaans = 63,
    indo_european::new_tokenizer, // Latin = 64,
    indo_european::new_tokenizer, // Slovak = 65,
    indo_european::new_tokenizer, // Catalan = 66,
    indo_european::new_tokenizer, // Unknown = 67,
];
