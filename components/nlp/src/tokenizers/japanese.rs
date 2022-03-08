use super::Token;

pub struct JapaneseTokenizer<'x> {
    text: &'x str,
    max_token_length: usize,
}

impl<'x> JapaneseTokenizer<'x> {
    pub fn new(text: &str, max_token_length: usize) -> JapaneseTokenizer {
        JapaneseTokenizer {
            text,
            max_token_length,
        }
    }
}

impl<'x> Iterator for JapaneseTokenizer<'x> {
    type Item = Token<'x>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(Token::new(0, self.max_token_length, self.text[0..2].into()))
    }
}

pub fn new_tokenizer<'x>(
    text: &'x str,
    max_token_length: usize,
) -> Box<dyn Iterator<Item = Token<'x>> + Send + 'x> {
    Box::new(JapaneseTokenizer::new(text, max_token_length))
}
