use std::{borrow::Cow, vec::IntoIter};

use jieba_rs::Jieba;

use super::{word::WordTokenizer, Token};

pub struct ChineseTokenizer<'x> {
    jieba: Jieba,
    word_tokenizer: WordTokenizer<'x>,
    tokens: IntoIter<&'x str>,
    token_offset: usize,
    token_len: usize,
    token_len_cur: usize,
    max_token_length: usize,
}

impl<'x> ChineseTokenizer<'x> {
    pub fn new(text: &str, max_token_length: usize) -> ChineseTokenizer {
        ChineseTokenizer {
            jieba: Jieba::new(),
            word_tokenizer: WordTokenizer::new(text),
            tokens: Vec::new().into_iter(),
            max_token_length,
            token_offset: 0,
            token_len: 0,
            token_len_cur: 0,
        }
    }
}

impl<'x> Iterator for ChineseTokenizer<'x> {
    type Item = Token<'x>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ch_token) = self.tokens.next() {
                let offset_start = self.token_offset + self.token_len_cur;
                self.token_len_cur += ch_token.len();

                if ch_token.len() <= self.max_token_length {
                    return Token::new(offset_start, ch_token.len(), ch_token.into()).into();
                }
            } else {
                let token = self.word_tokenizer.next()?;
                let word = match token.word {
                    Cow::Borrowed(word) => word,
                    Cow::Owned(_) => unreachable!(),
                };
                self.tokens = self.jieba.cut(word, false).into_iter();
                self.token_offset = token.offset as usize;
                self.token_len = token.len as usize;
                self.token_len_cur = 0;
            }
        }
    }
}
