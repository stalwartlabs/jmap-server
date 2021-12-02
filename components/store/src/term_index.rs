use std::{
    collections::HashSet,
    convert::{TryFrom, TryInto},
};

use integer_encoding::VarInt;
use nlp::tokenizers::Token;

use crate::{ArrayPos, FieldId, TermId};

use bitpacking::{BitPacker, BitPacker1x, BitPacker4x, BitPacker8x};

#[derive(Debug)]
pub enum Error {
    DataCorruption,
    VarintDecodeError,
    BitpackDecodeError,
    InvalidArgument,
}

pub type Result<T> = std::result::Result<T, Error>;

const LENGTH_SIZE: usize = std::mem::size_of::<u32>();

#[derive(Debug)]
pub struct Term {
    pub id: TermId,
    pub id_stemmed: TermId,
    pub offset: u32,
    pub len: u8,
}

#[derive(Debug)]
pub struct TermGroup {
    pub field_id: FieldId,
    pub field_num: ArrayPos,
    pub terms: Vec<Term>,
}

pub struct TermIndexBuilderItem {
    field: FieldId,
    field_num: ArrayPos,
    terms: Vec<Term>,
}

pub struct TermIndexBuilder {
    items: Vec<TermIndexBuilderItem>,
}

pub struct TermIndexItem<'x> {
    pub field_id: FieldId,
    pub field_num: ArrayPos,
    pub terms_len: usize,
    pub terms: &'x [u8],
}

pub struct TermIndex<'x> {
    items: Vec<TermIndexItem<'x>>,
}

#[derive(Debug)]
pub struct MatchTerm {
    pub id: TermId,
    pub id_stemmed: TermId,
}

impl Term {
    pub fn new(id: TermId, id_stemmed: TermId, token: &Token) -> Self {
        Term {
            id,
            id_stemmed,
            offset: token.offset,
            len: token.len,
        }
    }
}

#[derive(Clone, Copy)]
struct TermIndexPacker {
    bitpacker_1: BitPacker1x,
    bitpacker_4: BitPacker4x,
    bitpacker_8: BitPacker8x,
    block_len: usize,
}

impl TermIndexPacker {
    pub fn with_block_len(block_len: usize) -> Self {
        TermIndexPacker {
            bitpacker_1: BitPacker1x::new(),
            bitpacker_4: BitPacker4x::new(),
            bitpacker_8: BitPacker8x::new(),
            block_len,
        }
    }

    pub fn block_len(&mut self, num: usize) {
        self.block_len = num;
    }
}

impl BitPacker for TermIndexPacker {
    const BLOCK_LEN: usize = 0;

    fn new() -> Self {
        TermIndexPacker {
            bitpacker_1: BitPacker1x::new(),
            bitpacker_4: BitPacker4x::new(),
            bitpacker_8: BitPacker8x::new(),
            block_len: 1,
        }
    }

    fn compress(&self, decompressed: &[u32], compressed: &mut [u8], num_bits: u8) -> usize {
        match self.block_len {
            BitPacker8x::BLOCK_LEN => self
                .bitpacker_8
                .compress(decompressed, compressed, num_bits),
            BitPacker4x::BLOCK_LEN => self
                .bitpacker_4
                .compress(decompressed, compressed, num_bits),
            _ => self
                .bitpacker_1
                .compress(decompressed, compressed, num_bits),
        }
    }

    fn compress_sorted(
        &self,
        initial: u32,
        decompressed: &[u32],
        compressed: &mut [u8],
        num_bits: u8,
    ) -> usize {
        match self.block_len {
            BitPacker8x::BLOCK_LEN => {
                self.bitpacker_8
                    .compress_sorted(initial, decompressed, compressed, num_bits)
            }
            BitPacker4x::BLOCK_LEN => {
                self.bitpacker_4
                    .compress_sorted(initial, decompressed, compressed, num_bits)
            }
            _ => self
                .bitpacker_1
                .compress_sorted(initial, decompressed, compressed, num_bits),
        }
    }

    fn decompress(&self, compressed: &[u8], decompressed: &mut [u32], num_bits: u8) -> usize {
        match self.block_len {
            BitPacker8x::BLOCK_LEN => {
                self.bitpacker_8
                    .decompress(compressed, decompressed, num_bits)
            }
            BitPacker4x::BLOCK_LEN => {
                self.bitpacker_4
                    .decompress(compressed, decompressed, num_bits)
            }
            _ => self
                .bitpacker_1
                .decompress(compressed, decompressed, num_bits),
        }
    }

    fn decompress_sorted(
        &self,
        initial: u32,
        compressed: &[u8],
        decompressed: &mut [u32],
        num_bits: u8,
    ) -> usize {
        match self.block_len {
            BitPacker8x::BLOCK_LEN => {
                self.bitpacker_8
                    .decompress_sorted(initial, compressed, decompressed, num_bits)
            }
            BitPacker4x::BLOCK_LEN => {
                self.bitpacker_4
                    .decompress_sorted(initial, compressed, decompressed, num_bits)
            }
            _ => self
                .bitpacker_1
                .decompress_sorted(initial, compressed, decompressed, num_bits),
        }
    }

    fn num_bits(&self, decompressed: &[u32]) -> u8 {
        match self.block_len {
            BitPacker8x::BLOCK_LEN => self.bitpacker_8.num_bits(decompressed),
            BitPacker4x::BLOCK_LEN => self.bitpacker_4.num_bits(decompressed),
            _ => self.bitpacker_1.num_bits(decompressed),
        }
    }

    fn num_bits_sorted(&self, initial: u32, decompressed: &[u32]) -> u8 {
        match self.block_len {
            BitPacker8x::BLOCK_LEN => self.bitpacker_8.num_bits_sorted(initial, decompressed),
            BitPacker4x::BLOCK_LEN => self.bitpacker_4.num_bits_sorted(initial, decompressed),
            _ => self.bitpacker_1.num_bits_sorted(initial, decompressed),
        }
    }
}

#[allow(clippy::new_without_default)]
impl TermIndexBuilder {
    pub fn new() -> TermIndexBuilder {
        TermIndexBuilder { items: Vec::new() }
    }

    pub fn add_item(&mut self, field: FieldId, field_num: ArrayPos, terms: Vec<Term>) {
        self.items.push(TermIndexBuilderItem {
            field,
            field_num,
            terms,
        });
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn compress(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(1024);
        let mut bitpacker = TermIndexPacker::new();
        let mut compressed = vec![0u8; 4 * BitPacker8x::BLOCK_LEN];

        for term_index in self.items {
            let mut ids = Vec::with_capacity(term_index.terms.len() * 4);
            let mut offsets = Vec::with_capacity(term_index.terms.len());
            let mut lengths = Vec::with_capacity(term_index.terms.len());

            let header_pos = bytes.len();
            bytes.extend_from_slice(&[0u8; LENGTH_SIZE]);
            bytes.push(term_index.field);
            {
                let compressed_len = term_index.field_num.encode_var(&mut compressed[..]);
                bytes.extend_from_slice(&compressed[..compressed_len]);
            }
            {
                let compressed_len =
                    (term_index.terms.len() as u32).encode_var(&mut compressed[..]);
                bytes.extend_from_slice(&compressed[..compressed_len]);
            }

            let terms_pos = bytes.len();

            for term in term_index.terms {
                ids.push((term.id >> 32) as u32);
                ids.push((term.id & 0xFFFFFFFF) as u32);
                ids.push((term.id_stemmed >> 32) as u32);
                ids.push((term.id_stemmed & 0xFFFFFFFF) as u32);
                offsets.push(term.offset as u32);
                lengths.push(term.len);
            }

            for (chunk, is_sorted) in [(ids, false), (offsets, true)] {
                let mut pos = 0;
                let len = chunk.len();
                let mut initial_value = 0;

                while pos < len {
                    let block_len = match len - pos {
                        0..=31 => 0,
                        32..=127 => BitPacker1x::BLOCK_LEN,
                        128..=255 => BitPacker4x::BLOCK_LEN,
                        _ => BitPacker8x::BLOCK_LEN,
                    };

                    if block_len > 0 {
                        let chunk = &chunk[pos..pos + block_len as usize];
                        bitpacker.block_len(block_len);
                        if is_sorted {
                            let num_bits: u8 = bitpacker.num_bits_sorted(initial_value, chunk);
                            let compressed_len = bitpacker.compress_sorted(
                                initial_value,
                                chunk,
                                &mut compressed[..],
                                num_bits,
                            );
                            bytes.push(num_bits);
                            bytes.extend_from_slice(&compressed[..compressed_len]);
                            initial_value = chunk[chunk.len() - 1];
                        } else {
                            let num_bits: u8 = bitpacker.num_bits(chunk);
                            let compressed_len =
                                bitpacker.compress(chunk, &mut compressed[..], num_bits);
                            bytes.push(num_bits);
                            bytes.extend_from_slice(&compressed[..compressed_len]);
                        }

                        pos += block_len;
                    } else {
                        for val in &chunk[pos..] {
                            let compressed_len = (*val).encode_var(&mut compressed[..]);
                            bytes.extend_from_slice(&compressed[..compressed_len]);
                        }
                        pos = len;
                    }
                }
            }
            bytes.append(&mut lengths);

            let len = (bytes.len() - terms_pos) as u32;
            bytes[header_pos..header_pos + LENGTH_SIZE].copy_from_slice(&len.to_le_bytes());
        }

        bytes
    }
}

impl<'x> TryFrom<&'x [u8]> for TermIndex<'x> {
    type Error = Error;

    fn try_from(bytes: &'x [u8]) -> Result<Self> {
        let mut term_index = TermIndex { items: Vec::new() };
        let mut pos = 0;

        while pos < bytes.len() {
            let item_len = u32::from_le_bytes(
                bytes
                    .get(pos..pos + LENGTH_SIZE)
                    .ok_or(Error::DataCorruption)?
                    .try_into()
                    .map_err(|_| Error::DataCorruption)?,
            ) as usize;
            pos += LENGTH_SIZE;

            let field = bytes.get(pos).ok_or(Error::DataCorruption)?;
            pos += 1;

            let (field_num, bytes_read) =
                ArrayPos::decode_var(bytes.get(pos..).ok_or(Error::DataCorruption)?)
                    .ok_or(Error::VarintDecodeError)?;
            pos += bytes_read;

            let (terms_len, bytes_read) =
                u32::decode_var(bytes.get(pos..).ok_or(Error::DataCorruption)?)
                    .ok_or(Error::VarintDecodeError)?;
            pos += bytes_read;

            term_index.items.push(TermIndexItem {
                field_id: *field,
                field_num,
                terms_len: terms_len as usize,
                terms: bytes
                    .get(pos..pos + item_len)
                    .ok_or(Error::DataCorruption)?,
            });

            pos += item_len;
        }

        Ok(term_index)
    }
}

impl<'x> TermIndex<'x> {
    fn skip_items(&self, bytes: &[u8], mut remaining_items: usize) -> Result<usize> {
        let mut pos = 0;
        while remaining_items > 0 {
            let block_len = match remaining_items {
                0..=31 => 0,
                32..=127 => BitPacker1x::BLOCK_LEN,
                128..=255 => BitPacker4x::BLOCK_LEN,
                _ => BitPacker8x::BLOCK_LEN,
            };

            if block_len > 0 {
                pos +=
                    ((*bytes.get(pos).ok_or(Error::DataCorruption)? as usize) * block_len / 8) + 1;
                remaining_items -= block_len;
            } else {
                while remaining_items > 0 {
                    let (_, bytes_read) =
                        u32::decode_var(bytes.get(pos..).ok_or(Error::DataCorruption)?)
                            .ok_or(Error::VarintDecodeError)?;

                    pos += bytes_read;
                    remaining_items -= 1;
                }
            }
        }
        Ok(pos)
    }

    fn uncompress_chunk(
        &self,
        bytes: &[u8],
        remaining_items: usize,
        initial_value: Option<u32>,
    ) -> Result<(usize, Vec<u32>)> {
        let block_len = match remaining_items {
            0..=31 => 0,
            32..=127 => BitPacker1x::BLOCK_LEN,
            128..=255 => BitPacker4x::BLOCK_LEN,
            _ => BitPacker8x::BLOCK_LEN,
        };

        if block_len > 0 {
            let bitpacker = TermIndexPacker::with_block_len(block_len);
            let num_bits = *bytes.get(0).ok_or(Error::DataCorruption)?;
            let bytes_read = ((num_bits as usize) * block_len / 8) + 1;
            let mut decompressed = vec![0u32; block_len];

            if let Some(initial_value) = initial_value {
                bitpacker.decompress_sorted(
                    initial_value,
                    &bytes[1..bytes_read],
                    &mut decompressed[..],
                    num_bits,
                );
            } else {
                bitpacker.decompress(&bytes[1..bytes_read], &mut decompressed[..], num_bits);
            }

            Ok((bytes_read, decompressed))
        } else {
            let mut decompressed = Vec::with_capacity(remaining_items);
            let mut pos = 0;
            while decompressed.len() < remaining_items {
                let (val, bytes_read) =
                    u32::decode_var(bytes.get(pos..).ok_or(Error::DataCorruption)?)
                        .ok_or(Error::VarintDecodeError)?;
                decompressed.push(val);
                pos += bytes_read;
            }
            Ok((pos, decompressed))
        }
    }

    pub fn match_terms(
        &self,
        match_terms: &[MatchTerm],
        match_in: Option<HashSet<FieldId>>,
        match_phrase: bool,
        match_many: bool,
        include_offsets: bool,
    ) -> Result<Option<Vec<TermGroup>>> {
        let mut result = Vec::new();

        // Safety check to avoid overflowing the bit mask
        if !match_phrase && !(1..=64).contains(&match_terms.len()) {
            return Err(Error::InvalidArgument);
        }

        // Term matching is done using a bit mask, where each bit represents a word.
        // Each time a word is matched, the corresponding bit is cleared.
        // When all bits are cleared, all matching terms are added to the result list.
        let words_mask: u64 = u64::MAX >> (64 - match_terms.len());
        let mut matched_mask = words_mask;

        for item in &self.items {
            if let Some(ref match_in) = match_in {
                if !match_in.contains(&item.field_id) {
                    continue;
                }
            }

            let mut terms = Vec::new();
            let mut partial_match = Vec::new();

            let mut term_pos = 0;
            let mut byte_pos = 0;

            while term_pos < item.terms_len {
                let (bytes_read, chunk) = self.uncompress_chunk(
                    item.terms.get(byte_pos..).ok_or(Error::DataCorruption)?,
                    (item.terms_len * 4) - (term_pos * 4),
                    None,
                )?;

                byte_pos += bytes_read;

                for encoded_term in chunk.chunks_exact(4) {
                    let term_id = ((encoded_term[0] as u64) << 32) | encoded_term[1] as u64;
                    let term_id_stemmed = ((encoded_term[2] as u64) << 32) | encoded_term[3] as u64;

                    if match_phrase {
                        let match_pos = partial_match.len();
                        if match_terms[match_pos].id == term_id {
                            partial_match.push(Term {
                                id: term_id,
                                id_stemmed: term_id_stemmed,
                                offset: term_pos as u32,
                                len: 0,
                            });
                            if partial_match.len() == match_terms.len() {
                                terms.append(&mut partial_match);
                                if !match_many {
                                    break;
                                }
                            }
                        } else if match_pos > 0 {
                            partial_match.clear();
                        }
                    } else {
                        for (match_pos, match_term) in match_terms.iter().enumerate() {
                            if match_term.id == term_id
                                || match_term.id == term_id_stemmed
                                || (match_term.id_stemmed > 0
                                    && (match_term.id_stemmed == term_id
                                        || match_term.id_stemmed == term_id_stemmed))
                            {
                                partial_match.push(Term {
                                    id: term_id,
                                    id_stemmed: term_id_stemmed,
                                    offset: term_pos as u32,
                                    len: 0,
                                });

                                // Clear the bit corresponding to the matched term
                                matched_mask &= !(1 << match_pos);
                                break;
                            }
                        }

                        if !match_many && matched_mask == 0 {
                            break;
                        }
                    }
                    term_pos += 1;
                }
            }

            if !match_phrase && matched_mask == 0 {
                terms.append(&mut partial_match);
            }

            if !terms.is_empty() {
                if include_offsets {
                    // Skip any term ids that were not uncompressed
                    if term_pos < item.terms_len {
                        byte_pos += self.skip_items(
                            item.terms.get(byte_pos..).ok_or(Error::DataCorruption)?,
                            (item.terms_len * 4) - (term_pos * 4),
                        )?;
                    }

                    // Uncompress offsets
                    let mut term_it = terms.iter_mut();
                    let mut term = term_it.next().unwrap();
                    let mut initial_value = 0;
                    term_pos = 0;

                    'outer: while term_pos < item.terms_len {
                        let (bytes_read, chunk) = self.uncompress_chunk(
                            item.terms.get(byte_pos..).ok_or(Error::DataCorruption)?,
                            item.terms_len - term_pos,
                            Some(initial_value),
                        )?;

                        initial_value = chunk[chunk.len() - 1];
                        byte_pos += bytes_read;

                        for offset in chunk.into_iter() {
                            if term.offset == term_pos as u32 {
                                term.len = *item
                                    .terms
                                    .get(item.terms.len() - item.terms_len + term.offset as usize)
                                    .ok_or(Error::DataCorruption)?;
                                term.offset = offset;
                                if let Some(next_term) = term_it.next() {
                                    term = next_term;
                                } else {
                                    break 'outer;
                                }
                            }
                            term_pos += 1;
                        }
                    }
                }

                result.push(TermGroup {
                    field_id: item.field_id,
                    field_num: item.field_num,
                    terms,
                });

                if !match_many {
                    break;
                }
            }
        }

        Ok(if !result.is_empty() {
            Some(result)
        } else {
            None
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        convert::TryFrom,
    };

    use nlp::{
        stemmer::Stemmer,
        tokenizers::{tokenize, Token},
        Language,
    };

    use crate::term_index::{MatchTerm, Term, TermIndexBuilder};

    use super::TermIndex;

    #[test]
    #[allow(clippy::bind_instead_of_map)]
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
        let mut term_dict = HashMap::new();
        let mut term_id_dict = HashMap::new();

        // Build the term index
        for (field_num, (text, field_id)) in parts.iter().enumerate() {
            let mut terms = Vec::new();
            for token in tokenize(text, Language::English, 40) {
                let dict_len = term_dict.len() as u64 + 1;
                let term_id = *term_dict
                    .entry(token.word.to_string())
                    .or_insert_with(|| dict_len);
                let dict_len = term_dict.len() as u64 + 1;
                let term_id_stemmed = if let Some(stemmed_token) = stemmer.stem(&token) {
                    *term_dict
                        .entry(stemmed_token.word.to_string())
                        .or_insert_with(|| dict_len)
                } else {
                    0
                };
                terms.push(Term::new(term_id, term_id_stemmed, &token));
            }
            builder.add_item(*field_id, field_num as u16, terms);
        }

        // Build the term id dictionary
        for (term, term_id) in term_dict.iter() {
            term_id_dict.insert(term_id, term);
        }

        let compressed_term_index = builder.compress();
        let term_index = TermIndex::try_from(&compressed_term_index[..]).unwrap();

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
            (vec!["but"], None, false, 6),
            (vec!["but"], None, true, 6),
        ];

        for (words, field_id, match_phrase, match_count) in tests {
            let mut match_terms = Vec::new();
            for word in &words {
                match_terms.push(MatchTerm {
                    id: *term_dict.get(*word).unwrap_or(&0),
                    id_stemmed: if let Some(stemmed_token) =
                        stemmer.stem(&Token::new(0, 0, (*word).into()))
                    {
                        *term_dict.get(stemmed_token.word.as_ref()).unwrap_or(&0)
                    } else {
                        0
                    },
                });
            }

            let result = term_index
                .match_terms(
                    &match_terms,
                    field_id.and_then(|f| {
                        let mut h = HashSet::new();
                        h.insert(f);
                        Some(h)
                    }),
                    match_phrase,
                    true,
                    true,
                )
                .unwrap()
                .unwrap_or_else(Vec::new);

            let mut result_len = 0;
            for r in &result {
                result_len += r.terms.len();
            }

            assert_eq!(
                result_len, match_count,
                "({:?}, {}) != {:?}",
                words, match_phrase, result
            );

            for term_group in &result {
                'outer: for term in &term_group.terms {
                    let text_word = parts[term_group.field_num as usize].0
                        [term.offset as usize..term.offset as usize + term.len as usize]
                        .to_lowercase();
                    let token_stemmed_word = if term.id_stemmed > 0 {
                        term_id_dict.get(&term.id_stemmed)
                    } else {
                        None
                    };

                    for word in words.iter() {
                        if word == &text_word
                            || !match_phrase
                                && word == token_stemmed_word.unwrap_or(&&"".to_string())
                        {
                            continue 'outer;
                        }
                    }
                    panic!("({:?}, {}) != {:?}", words, match_phrase, result);
                }
            }
        }
    }
}
