use std::str::CharIndices;

use super::Token;

pub struct WordTokenizer<'x> {
    text: &'x str,
    iterator: CharIndices<'x>,
}

impl<'x> WordTokenizer<'x> {
    pub fn new(text: &str) -> WordTokenizer {
        WordTokenizer {
            text,
            iterator: text.char_indices(),
        }
    }
}

/// Parses text into tokens, used by non-IndoEuropean tokenizers.
impl<'x> Iterator for WordTokenizer<'x> {
    type Item = Token<'x>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((token_start, ch)) = self.iterator.next() {
            if ch.is_alphanumeric() {
                let token_end = (&mut self.iterator)
                    .filter_map(|(pos, ch)| {
                        if ch.is_alphanumeric() {
                            None
                        } else {
                            pos.into()
                        }
                    })
                    .next()
                    .unwrap_or(self.text.len());

                let token_len = token_end - token_start;
                if token_end > token_start {
                    return Token::new(
                        token_start,
                        token_len,
                        self.text[token_start..token_end].into(),
                    )
                    .into();
                }
            }
        }
        None
    }
}
