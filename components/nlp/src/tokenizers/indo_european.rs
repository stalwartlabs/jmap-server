use std::str::CharIndices;

use super::Token;

pub struct IndoEuropeanTokenizer<'x> {
    max_token_length: usize,
    text: &'x str,
    iterator: CharIndices<'x>,
    tokens: usize,
}

impl<'x> IndoEuropeanTokenizer<'x> {
    pub fn new(text: &str, max_token_length: usize) -> IndoEuropeanTokenizer {
        IndoEuropeanTokenizer {
            max_token_length,
            text,
            iterator: text.char_indices(),
            tokens: 0,
        }
    }
    pub fn new_boxed(
        text: &'x str,
        max_token_length: usize,
    ) -> Box<dyn Iterator<Item = Token<'x>> + 'x> {
        Box::new(Self::new(text, max_token_length))
    }
}

/// Parses an indo-european text into lowercase tokens.
impl<'x> Iterator for IndoEuropeanTokenizer<'x> {
    type Item = Token<'x>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((token_start, ch)) = self.iterator.next() {
            if ch.is_alphanumeric() {
                let mut is_uppercase = ch.is_uppercase();
                let token_end = (&mut self.iterator)
                    .filter(|(_, ch)| {
                        if ch.is_alphanumeric() {
                            if !is_uppercase && ch.is_uppercase() {
                                is_uppercase = true;
                            }
                            false
                        } else {
                            true
                        }
                    })
                    .map(|(pos, _)| pos)
                    .next()
                    .unwrap_or_else(|| self.text.len());

                self.tokens += 1;

                let token_len = token_end - token_start;
                if token_end > token_start && token_len <= self.max_token_length {
                    return Token::new(
                        self.tokens,
                        token_start,
                        token_len,
                        if is_uppercase {
                            self.text[token_start..token_end].to_lowercase().into()
                        } else {
                            self.text[token_start..token_end].into()
                        },
                    )
                    .into();
                }
            }
        }
        None
    }
}

pub fn new_tokenizer<'x>(
    text: &'x str,
    max_token_length: usize,
) -> Box<dyn Iterator<Item = Token<'x>> + 'x> {
    Box::new(IndoEuropeanTokenizer::new(text, max_token_length))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indo_european_tokenizer() {
        let inputs = [
            (
                "The quick brown fox jumps over the lazy dog",
                vec![
                    Token::new(1, 0, 3, "the".into()),
                    Token::new(2, 4, 9, "quick".into()),
                    Token::new(3, 10, 15, "brown".into()),
                    Token::new(4, 16, 19, "fox".into()),
                    Token::new(5, 20, 25, "jumps".into()),
                    Token::new(6, 26, 30, "over".into()),
                    Token::new(7, 31, 34, "the".into()),
                    Token::new(8, 35, 39, "lazy".into()),
                    Token::new(9, 40, 43, "dog".into()),
                ],
            ),
            (
                "Jovencillo EMPONZOÑADO de whisky: ¡qué figurota exhibe!",
                vec![
                    Token::new(1, 0, 10, "jovencillo".into()),
                    Token::new(2, 11, 23, "emponzoñado".into()),
                    Token::new(3, 24, 26, "de".into()),
                    Token::new(4, 27, 33, "whisky".into()),
                    Token::new(5, 37, 41, "qué".into()),
                    Token::new(6, 42, 50, "figurota".into()),
                    Token::new(7, 51, 57, "exhibe".into()),
                ],
            ),
            (
                "ZWÖLF Boxkämpfer jagten Victor quer über den großen Sylter Deich",
                vec![
                    Token::new(1, 0, 6, "zwölf".into()),
                    Token::new(2, 7, 18, "boxkämpfer".into()),
                    Token::new(3, 19, 25, "jagten".into()),
                    Token::new(4, 26, 32, "victor".into()),
                    Token::new(5, 33, 37, "quer".into()),
                    Token::new(6, 38, 43, "über".into()),
                    Token::new(7, 44, 47, "den".into()),
                    Token::new(8, 48, 55, "großen".into()),
                    Token::new(9, 56, 62, "sylter".into()),
                    Token::new(10, 63, 68, "deich".into()),
                ],
            ),
            (
                "Съешь ещё этих мягких французских булок, да выпей же чаю",
                vec![
                    Token::new(1, 0, 10, "съешь".into()),
                    Token::new(2, 11, 17, "ещё".into()),
                    Token::new(3, 18, 26, "этих".into()),
                    Token::new(4, 27, 39, "мягких".into()),
                    Token::new(5, 40, 62, "французских".into()),
                    Token::new(6, 63, 73, "булок".into()),
                    Token::new(7, 75, 79, "да".into()),
                    Token::new(8, 80, 90, "выпей".into()),
                    Token::new(9, 91, 95, "же".into()),
                    Token::new(10, 96, 102, "чаю".into()),
                ],
            ),
            (
                "Pijamalı hasta yağız şoföre çabucak güvendi",
                vec![
                    Token::new(1, 0, 9, "pijamalı".into()),
                    Token::new(2, 10, 15, "hasta".into()),
                    Token::new(3, 16, 23, "yağız".into()),
                    Token::new(4, 24, 32, "şoföre".into()),
                    Token::new(5, 33, 41, "çabucak".into()),
                    Token::new(6, 42, 50, "güvendi".into()),
                ],
            ),
        ];

        for (input, tokens) in inputs.iter() {
            for (pos, token) in IndoEuropeanTokenizer::new(input, 40).enumerate() {
                assert_eq!(token, tokens[pos]);
            }
        }
    }
}
