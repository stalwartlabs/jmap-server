use std::{
    borrow::Cow,
    collections::{btree_map::Entry, BTreeMap},
    convert::TryInto,
};

use fst::{Map, MapBuilder};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use nlp::tokenizers::Token;

use crate::{ArrayPos, FieldId};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    FstError(fst::raw::Error),
    IoError(std::io::Error),
    DecompressError(lz4_flex::block::DecompressError),
    DataCorruption,
    NotFound,
    InvalidArgument,
}

const T_PART_ID_SIZE: usize = std::mem::size_of::<u16>();
const T_OFFSET_SIZE: usize = std::mem::size_of::<u32>();
const T_POSITION_SIZE: usize = std::mem::size_of::<u32>();
const T_LENGTH_SIZE: usize = std::mem::size_of::<u8>();
const T_SIZE: usize = T_PART_ID_SIZE + T_OFFSET_SIZE + T_POSITION_SIZE + T_LENGTH_SIZE;

const T_PART_ID_START: usize = 0;
const T_OFFSET_START: usize = T_PART_ID_SIZE;
const T_POSITION_START: usize = T_PART_ID_SIZE + T_OFFSET_SIZE;
const T_LENGTH_START: usize = T_PART_ID_SIZE + T_OFFSET_SIZE + T_POSITION_SIZE;

const T_PART_ID_END: usize = T_PART_ID_SIZE;
const T_OFFSET_END: usize = T_OFFSET_START + T_OFFSET_SIZE;
const T_POSITION_END: usize = T_POSITION_START + T_POSITION_SIZE;

#[derive(Debug)]
pub struct Term {
    pub field: FieldId,
    pub field_num: ArrayPos,
    pub offset: u32,
    pub len: u8,
    pub pos: u32,
    pub is_exact: bool,
}

pub struct TermIndexBuilder<'x> {
    terms: BTreeMap<Cow<'x, str>, Vec<Term>>,
    terms_len: usize,
}

impl<'x> Default for TermIndexBuilder<'x> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'x> TermIndexBuilder<'x> {
    pub fn new() -> Self {
        TermIndexBuilder {
            terms: BTreeMap::new(),
            terms_len: 0,
        }
    }

    pub fn add_term(&mut self, field: FieldId, field_num: ArrayPos, token: Token<'x>) {
        let term = Term {
            field,
            field_num,
            offset: token.offset,
            len: token.len,
            pos: token.pos,
            is_exact: token.is_exact,
        };

        self.terms_len += 1;
        match self.terms.entry(token.word) {
            Entry::Vacant(e) => {
                e.insert(vec![term]);
            }
            Entry::Occupied(e) => {
                e.into_mut().push(term);
            }
        }
    }

    pub fn serialize(&self) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut map = MapBuilder::memory();
        let mut bytes = Vec::with_capacity(self.terms_len * T_SIZE);

        for (key, terms) in self.terms.iter() {
            map.insert(
                key.as_bytes(),
                ((terms.len() as u64) << 32) | (bytes.len() as u64),
            )
            .map_err(|e| match e {
                fst::Error::Io(e) => Error::IoError(e),
                fst::Error::Fst(e) => Error::FstError(e),
            })?;

            for term in terms.iter() {
                bytes.extend_from_slice(term.field_num.to_be_bytes().as_ref());
                bytes.extend_from_slice(term.offset.to_be_bytes().as_ref());
                bytes.extend_from_slice(
                    (term.pos
                        | (term.field as u32) << 24
                        | if term.is_exact { 0 } else { 1 << 31 })
                    .to_be_bytes()
                    .as_ref(),
                );
                bytes.push(term.len);
            }
        }

        Ok((
            map.into_inner().map_err(|e| match e {
                fst::Error::Io(e) => Error::IoError(e),
                fst::Error::Fst(e) => Error::FstError(e),
            })?,
            compress_prepend_size(&bytes),
        ))
    }
}

pub struct TermIndex<'x> {
    map: Map<&'x [u8]>,
    terms: Vec<u8>,
}

impl<'x> TermIndex<'x> {
    pub fn new(map: &'x [u8], positions: &'x [u8]) -> Result<Self> {
        Ok(Self {
            map: Map::new(map).map_err(|e| match e {
                fst::Error::Io(e) => Error::IoError(e),
                fst::Error::Fst(e) => Error::FstError(e),
            })?,
            terms: decompress_size_prepended(positions).map_err(Error::DecompressError)?,
        })
    }

    fn build_search_tree(
        &self,
        words: &[&str],
        match_in: Option<u8>,
        match_phrase: bool,
    ) -> Result<BTreeMap<&[u8], usize>> {
        let mut search_tree = BTreeMap::new();

        // Sort all terms by position in the document
        for (word_pos, word) in words.iter().enumerate() {
            let packed_info = self.map.get(word).ok_or(Error::NotFound)?;
            let total_terms = (packed_info >> 32) as usize;
            let offset = (packed_info & 0xFFFFFFFF) as usize;

            for term_num in 0..total_terms {
                // The first bits of the position indicates that the term was added by the stemmer.
                // The following 7 bits contain the Field Id.
                let term_offset = offset + (T_SIZE * term_num);
                let flags = self
                    .terms
                    .get(term_offset + T_POSITION_START)
                    .ok_or(Error::DataCorruption)?;

                // Filter out terms that do not belong to the requested part type or are not an exact match
                if match_in.map_or(false, |match_in| *flags & 0x7f != match_in)
                    || (match_phrase && *flags & 0x80 != 0)
                {
                    continue;
                }

                // Add term to BTreeMap
                search_tree.insert(
                    self.terms
                        .get(term_offset..term_offset + T_SIZE)
                        .ok_or(Error::DataCorruption)?,
                    word_pos,
                );
            }
        }

        Ok(search_tree)
    }

    pub fn match_phrase(&self, words: &[&str], match_in: Option<u8>) -> Result<bool> {
        let mut matched_terms = 0;
        let mut last_field_num = u16::MAX;
        let mut last_term_pos = u32::MAX;

        for (raw_term, word_num) in self.build_search_tree(words, match_in, true)? {
            if matched_terms == word_num {
                let (term_pos, term_field_num) = (
                    self.deserialize_position(raw_term)?,
                    self.deserialize_field_num(raw_term)?,
                );

                if word_num == 0
                    || (term_field_num == last_field_num && term_pos == last_term_pos + 1)
                {
                    matched_terms += 1;
                    if matched_terms == words.len() {
                        return Ok(true);
                    }

                    last_field_num = term_field_num;
                    last_term_pos = term_pos;

                    continue;
                }
            }

            matched_terms = 0;
        }

        Ok(false)
    }

    pub fn match_any(&self, words: &[&str], match_in: Option<u8>) -> Result<bool> {
        let mut last_field_num = u16::MAX;

        if !(1..=64).contains(&words.len()) {
            return Err(Error::InvalidArgument);
        }
        let words_mask: u64 = u64::MAX >> (64 - words.len());
        let mut matched_mask = words_mask;

        for (raw_term, word_num) in self.build_search_tree(words, match_in, false)? {
            let term_field_num = self.deserialize_field_num(raw_term)?;

            if term_field_num != last_field_num {
                if matched_mask == 0 {
                    return Ok(true);
                } else {
                    last_field_num = term_field_num;
                    matched_mask = words_mask;
                }
            }

            matched_mask &= !(1 << word_num);
        }

        Ok(matched_mask == 0)
    }

    #[inline(always)]
    fn deserialize_field_num(&self, raw_term: &[u8]) -> Result<u16> {
        Ok(u16::from_be_bytes(
            raw_term
                .get(T_PART_ID_START..T_PART_ID_END)
                .ok_or(Error::DataCorruption)?
                .try_into()
                .unwrap(),
        ))
    }

    #[inline(always)]
    fn deserialize_offset(&self, raw_term: &[u8]) -> Result<u32> {
        Ok(u32::from_be_bytes(
            raw_term
                .get(T_OFFSET_START..T_OFFSET_END)
                .ok_or(Error::DataCorruption)?
                .try_into()
                .unwrap(),
        ))
    }

    #[inline(always)]
    fn deserialize_position(&self, raw_term: &[u8]) -> Result<u32> {
        Ok(u32::from_be_bytes(
            raw_term
                .get(T_POSITION_START..T_POSITION_END)
                .ok_or(Error::DataCorruption)?
                .try_into()
                .unwrap(),
        ) & !(0xff << 24))
    }

    #[inline(always)]
    fn deserialize_length(&self, raw_term: &[u8]) -> Result<u8> {
        Ok(*raw_term.get(T_LENGTH_START).ok_or(Error::DataCorruption)?)
    }

    pub fn search_phrase(&self, words: &[&'x str], match_in: Option<FieldId>) -> Result<Vec<Term>> {
        let mut result = Vec::new();
        let mut matched_terms = Vec::new();
        let mut last_field_num = u16::MAX;
        let mut last_term_pos = u32::MAX;

        // Iterate over all terms in the search tree
        for (raw_term, word_num) in self.build_search_tree(words, match_in, true)? {
            if matched_terms.len() == word_num {
                let term_field_num = self.deserialize_field_num(raw_term)?;

                if !result.is_empty() && last_field_num > 0 && term_field_num > last_field_num {
                    // Match maximum the Subject (field_num = 0) and one part
                    return Ok(result);
                }

                let term_pos = self.deserialize_position(raw_term)?;

                if word_num == 0
                    || (term_field_num == last_field_num && term_pos == last_term_pos + 1)
                {
                    last_field_num = term_field_num;
                    last_term_pos = term_pos;

                    matched_terms.push(Term {
                        offset: self.deserialize_offset(raw_term)?,
                        len: self.deserialize_length(raw_term)?,
                        pos: term_pos,
                        field_num: term_field_num,
                        field: match_in.unwrap_or(0),
                        is_exact: true,
                    });

                    if matched_terms.len() == words.len() {
                        result.append(&mut matched_terms);
                    }
                    continue;
                }
            }

            if !matched_terms.is_empty() {
                matched_terms.clear();
            }
        }

        Ok(result)
    }

    pub fn search_any(&self, words: &[&'x str], match_in: Option<u8>) -> Result<Vec<Term>> {
        let mut result = Vec::new();
        let mut matched_terms = Vec::new();
        let mut last_field_num = u16::MAX;

        // Safety check to avoid overflowing the bit mask
        if !(1..=64).contains(&words.len()) {
            return Err(Error::InvalidArgument);
        }

        // Term matching is done using a bit mask, where each bit represents a word.
        // Each time a word is matched, the corresponding bit is cleared.
        // When all bits are cleared, all matching terms are added to the result list.
        let words_mask: u64 = u64::MAX >> (64 - words.len());
        let mut matched_mask = words_mask;

        // Iterate over all terms in the search tree
        for (raw_term, word_num) in self.build_search_tree(words, match_in, false)? {
            let term_field_num = self.deserialize_field_num(raw_term)?;

            if term_field_num != last_field_num {
                if matched_mask == 0 {
                    result.append(&mut matched_terms);

                    // Match maximum the Subject (field_num = 0) and one part
                    if last_field_num > 0 {
                        return Ok(result);
                    }
                } else if !matched_terms.is_empty() {
                    matched_terms.clear();
                }

                last_field_num = term_field_num;
                matched_mask = words_mask;
            }

            // Clear the bit corresponding to the matched word
            matched_mask &= !(1 << word_num);
            matched_terms.push(Term {
                offset: self.deserialize_offset(raw_term)?,
                len: self.deserialize_length(raw_term)?,
                pos: self.deserialize_position(raw_term)?,
                field_num: term_field_num,
                field: match_in.unwrap_or(0),
                is_exact: false,
            });
        }

        if matched_mask == 0 {
            if !result.is_empty() {
                result.append(&mut matched_terms);
                Ok(result)
            } else {
                Ok(matched_terms)
            }
        } else {
            Ok(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use nlp::{
        stemmer::Stemmer,
        tokenizers::{tokenize, Token},
        Language,
    };

    use crate::{term_index::TermIndexBuilder, ArrayPos};

    use super::TermIndex;

    #[test]
    fn term_index() {
        const SUBJECT: u8 = 1;
        const BODY: u8 = 2;
        const ATTACHMENT: u8 = 3;

        let parts = [
            (
                r#"I felt happy because I saw the others were happy 
            and because I knew I should feel happy, but I wasn’t 
            really happy."#,
                SUBJECT,
            ),
            (
                r#"But good morning! Good morning to ye and thou! I’d 
            say to all my patients, because I was the worse of the 
            hypocrites, of all the hypocrites, the cruel and phony 
            hypocrites, I was the very worst."#,
                BODY,
            ),
            (
                r#"So I said yes to Thomas Clinton and later thought 
            that I had said yes to God and later still realized I 
            had said yes only to Thomas Clinton."#,
                BODY,
            ),
            (
                r#"Even if they are djinns, I will get djinns that 
            can outdjinn them."#,
                BODY,
            ),
            (
                r#"Hatred was spreading everywhere, blood was being
             spilled everywhere, wars were breaking out 
             everywhere."#,
                BODY,
            ),
            (
                r#"Almost nothing was more annoying than having 
            our wasted time wasted on something not worth 
            wasting it on."#,
                BODY,
            ),
            (
                r#"The depressed person was in terrible and unceasing 
            emotional pain, and the impossibility of sharing or 
            articulating this pain was itself a component of the 
            pain and a contributing factor in its essential horror."#,
                BODY,
            ),
            (
                r#"Paranoids are not paranoid because they’re paranoid, 
            but because they keep putting themselves, darn idiots, 
            deliberately into paranoid situations."#,
                BODY,
            ),
            (
                r#"Because the world is a place of silence, the sky at 
            night when the birds have gone is a vast silent place."#,
                BODY,
            ),
            (
                r#"There are some things that are so unforgivable that 
            they make other things easily forgivable."#,
                BODY,
            ),
            (
                r#"I had known loneliness before, and emptiness upon the 
            moor, but I had never been a NOTHING, a nothing floating 
            on a nothing, known by nothing, lonelier and colder than 
            the space between the stars."#,
                ATTACHMENT,
            ),
            (
                r#"You’re an insomniac, you tell yourself: there are 
            profound truths revealed only to the insomniac by night 
            like those phosphorescent minerals veined and glimmering 
            in the dark but coarse and ordinary otherwise; you have 
            to examine such minerals in the absence of light to 
            discover their beauty, you tell yourself."#,
                ATTACHMENT,
            ),
            (
                r#"Every person had a star, every star had a friend, 
            and for every person carrying a star there was someone 
            else who reflected it, and everyone carried this reflection 
            like a secret confidante in the heart."#,
                ATTACHMENT,
            ),
            (
                r#"As my grandfather went, arm over arm, his heart making 
            sour little shudders against his ribs, he kept listening 
            for a sound, the sound of the tiger, the sound of anything 
            but his own feet and lungs."#,
                ATTACHMENT,
            ),
            (r#"love loving lovingly loved lovely"#, ATTACHMENT),
        ];

        let mut builder = TermIndexBuilder::new();
        let stemmer = Stemmer::new(Language::English).unwrap();

        // Build the term index
        for (field_num, (text, field_id)) in parts.iter().enumerate() {
            for token in tokenize(text, Language::English, 40) {
                if let Some(stemmed_token) = stemmer.stem(&token) {
                    builder.add_term(*field_id, field_num as ArrayPos, stemmed_token);
                }
                builder.add_term(*field_id, field_num as ArrayPos, token);
            }
        }

        let (raw_map, raw_pos) = builder.serialize().unwrap();
        let map = TermIndex::new(&raw_map, &raw_pos).unwrap();

        let tests = [
            (vec!["thomas", "clinton"], None, true, 4),
            (vec!["was", "the", "worse"], None, true, 3),
            (vec!["carri"], None, false, 2),
            (vec!["nothing", "floating"], None, true, 2),
            (vec!["floating", "nothing"], None, false, 5),
            (vec!["floating", "nothing"], None, true, 0),
            (vec!["noth", "floating"], None, true, 0),
            (vec!["noth", "floating"], None, false, 5),
            (vec!["realli", "happi"], None, false, 5),
            (vec!["really", "happy"], None, true, 2),
            (vec!["should", "feel", "happy", "but"], None, true, 4),
            (
                vec!["love", "loving", "lovingly", "loved", "lovely"],
                Some(ATTACHMENT),
                true,
                5,
            ),
            (vec!["love"], Some(ATTACHMENT), false, 5),
            (vec!["but"], None, false, 2),
            (vec!["but"], None, true, 2),
        ];

        for (words, field_id, match_phrase, match_count) in tests {
            let terms = if match_phrase {
                map.search_phrase(&words, field_id).unwrap()
            } else {
                map.search_any(&words, field_id).unwrap()
            };
            let has_match = if match_phrase {
                map.match_phrase(&words, field_id).unwrap()
            } else {
                map.match_any(&words, field_id).unwrap()
            };

            assert_eq!(
                terms.len(),
                match_count,
                "({:?}, {}) != {:?}",
                words,
                match_phrase,
                terms
            );
            assert_eq!(has_match, match_count > 0);

            'outer: for term in terms.iter() {
                let text_word = parts[term.field_num as usize].0
                    [term.offset as usize..term.offset as usize + term.len as usize]
                    .to_lowercase();

                for word in words.iter() {
                    if word == &text_word {
                        continue 'outer;
                    }
                }

                if !match_phrase {
                    if let Some(text_word) = stemmer.stem(&Token::new(0, 0, 0, text_word.into())) {
                        for word in words.iter() {
                            if word == &text_word.word {
                                continue 'outer;
                            }
                        }
                    }
                }
                panic!("({:?}, {}) != {:?}", words, match_phrase, terms);
            }
        }
    }
}
