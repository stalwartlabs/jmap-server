use std::borrow::Cow;

use rust_stemmers::Algorithm;

use crate::{tokenizers::Token, Language};

pub struct Stemmer {
    stemmer: rust_stemmers::Stemmer,
}

impl Stemmer {
    pub fn new(language: Language) -> Option<Stemmer> {
        Stemmer {
            stemmer: rust_stemmers::Stemmer::create(STEMMER_MAP[language as usize]?),
        }
        .into()
    }

    pub fn stem<'x>(&self, token: &Token<'x>) -> Option<Token<'x>> {
        if let Cow::Owned(text) = self.stemmer.stem(&token.word) {
            if text.len() != token.len as usize || text != token.word {
                Some(Token {
                    word: text.into(),
                    len: token.len,
                    offset: token.offset,
                    is_exact: false,
                })
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tokenizers::tokenize;

    use super::*;

    #[test]
    fn stemmer() {
        let inputs = [
            (
                "love loving lovingly loved lovely",
                Language::English,
                "love",
            ),
            ("querer queremos quer", Language::Spanish, "quer"),
        ];

        for input in inputs {
            let stemmer = Stemmer::new(input.1).unwrap();
            for token in tokenize(input.0, input.1, 40) {
                let token = stemmer.stem(&token).unwrap_or(token);
                assert_eq!(token.word, input.2);
            }
        }
    }
}

static STEMMER_MAP: &[Option<Algorithm>] = &[
    None,                        // Esperanto = 0,
    Some(Algorithm::English),    // English = 1,
    Some(Algorithm::Russian),    // Russian = 2,
    None,                        // Mandarin = 3,
    Some(Algorithm::Spanish),    // Spanish = 4,
    Some(Algorithm::Portuguese), // Portuguese = 5,
    Some(Algorithm::Italian),    // Italian = 6,
    None,                        // Bengali = 7,
    Some(Algorithm::French),     // French = 8,
    Some(Algorithm::German),     // German = 9,
    None,                        // Ukrainian = 10,
    None,                        // Georgian = 11,
    Some(Algorithm::Arabic),     // Arabic = 12,
    None,                        // Hindi = 13,
    None,                        // Japanese = 14,
    None,                        // Hebrew = 15,
    None,                        // Yiddish = 16,
    None,                        // Polish = 17,
    None,                        // Amharic = 18,
    None,                        // Javanese = 19,
    None,                        // Korean = 20,
    Some(Algorithm::Norwegian),  // Bokmal = 21,
    Some(Algorithm::Danish),     // Danish = 22,
    Some(Algorithm::Swedish),    // Swedish = 23,
    Some(Algorithm::Finnish),    // Finnish = 24,
    Some(Algorithm::Turkish),    // Turkish = 25,
    Some(Algorithm::Dutch),      // Dutch = 26,
    Some(Algorithm::Hungarian),  // Hungarian = 27,
    None,                        // Czech = 28,
    Some(Algorithm::Greek),      // Greek = 29,
    None,                        // Bulgarian = 30,
    None,                        // Belarusian = 31,
    None,                        // Marathi = 32,
    None,                        // Kannada = 33,
    Some(Algorithm::Romanian),   // Romanian = 34,
    None,                        // Slovene = 35,
    None,                        // Croatian = 36,
    None,                        // Serbian = 37,
    None,                        // Macedonian = 38,
    None,                        // Lithuanian = 39,
    None,                        // Latvian = 40,
    None,                        // Estonian = 41,
    Some(Algorithm::Tamil),      // Tamil = 42,
    None,                        // Vietnamese = 43,
    None,                        // Urdu = 44,
    None,                        // Thai = 45,
    None,                        // Gujarati = 46,
    None,                        // Uzbek = 47,
    None,                        // Punjabi = 48,
    None,                        // Azerbaijani = 49,
    None,                        // Indonesian = 50,
    None,                        // Telugu = 51,
    None,                        // Persian = 52,
    None,                        // Malayalam = 53,
    None,                        // Oriya = 54,
    None,                        // Burmese = 55,
    None,                        // Nepali = 56,
    None,                        // Sinhalese = 57,
    None,                        // Khmer = 58,
    None,                        // Turkmen = 59,
    None,                        // Akan = 60,
    None,                        // Zulu = 61,
    None,                        // Shona = 62,
    None,                        // Afrikaans = 63,
    None,                        // Latin = 64,
    None,                        // Slovak = 65,
    None,                        // Catalan = 66,
    None,                        // Unknown = 67,
];
