use crate::term_index::Term;

fn escape_char(c: char, string: &mut String) {
    match c {
        '&' => string.push_str("&amp;"),
        '<' => string.push_str("&lt;"),
        '>' => string.push_str("&gt;"),
        '"' => string.push_str("&quot;"),
        '\r' => (),
        '\n' => string.push(' '),
        _ => string.push(c),
    }
}

pub fn generate_snippet(terms: &[Term], text: &str) -> Option<String> {
    let mut snippet = String::with_capacity(text.len());
    let start_offset = terms.get(0)?.offset as usize;

    if start_offset > 0 {
        let mut word_count = 0;
        let mut from_offset = 0;
        if text.len() > 240 {
            for (pos, char) in text.get(0..start_offset)?.char_indices().rev() {
                // Add up to 2 words or 40 characters of context
                if char.is_whitespace() {
                    word_count += 1;
                    if word_count == 3 {
                        break;
                    }
                }
                from_offset = pos;
                if start_offset - from_offset >= 40 {
                    break;
                }
            }
        }

        for char in text.get(from_offset..start_offset)?.chars() {
            escape_char(char, &mut snippet);
        }
    }

    let mut terms = terms.iter().peekable();

    'outer: while let Some(term) = terms.next() {
        snippet.push_str("<mark>");
        snippet.push_str(text.get(term.offset as usize..term.offset as usize + term.len as usize)?);
        snippet.push_str("</mark>");

        let next_offset = if let Some(next_term) = terms.peek() {
            next_term.offset as usize
        } else {
            text.len()
        };

        for char in text
            .get(term.offset as usize + term.len as usize..next_offset)?
            .chars()
        {
            if snippet.len() + 3 > 255 {
                break 'outer;
            }
            escape_char(char, &mut snippet);
        }

        if snippet.len() + 3 > 255 {
            break;
        }
    }

    Some(snippet)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, convert::TryFrom};

    use nlp::{tokenizers::tokenize, Language};

    use crate::{
        serialize::StoreDeserialize,
        term_index::{MatchTerm, TermIndex, TermIndexBuilder},
    };

    use super::*;

    #[test]
    fn search_snippets() {
        let inputs = [
            (vec![
                "Help a friend from Abidjan Côte d'Ivoire",
                concat!(
                "When my mother died when she was given birth to me, my father took me so ", 
                "special because I am motherless. Before the death of my late father on 22nd June ",
                "2013 in a private hospital here in Abidjan Côte d'Ivoire. He secretly called me on his ",
                "bedside and told me that he has a sum of $7.5M (Seven Million five Hundred ",
                "Thousand Dollars) left in a suspense account in a local bank here in Abidjan Côte ",
                "d'Ivoire, that he used my name as his only daughter for the next of kin in deposit of ",
                "the fund. ",
                "I am 24year old. Dear I am honorably seeking your assistance in the following ways. ",
                "1) To provide any bank account where this money would be transferred into. ",
                "2) To serve as the guardian of this fund. ",
                "3) To make arrangement for me to come over to your country to further my ",
                "education and to secure a residential permit for me in your country. ",
                "Moreover, I am willing to offer you 30 percent of the total sum as compensation for ",
                "your effort input after the successful transfer of this fund to your nominated ",
                "account overseas."
            )],
                vec![
                    (
                        vec!["côte"], 
                        vec![
                            "Help a friend from Abidjan <mark>Côte</mark> d'Ivoire", 
                            concat!(
                            "in Abidjan <mark>Côte</mark> d'Ivoire. He secretly called me on his bedside ",
                            "and told me that he has a sum of $7.5M (Seven Million five Hundred Thousand ",
                            "Dollars) left in a suspense account in a local bank here in Abidjan ",
                            "<mark>Côte</mark> d'Ivoire, tha")
                        ]
                    ),
                    (
                        vec!["your", "country"], 
                        vec![
                            concat!(
                            "honorably seeking <mark>your</mark> assistance in the following ways. ", 
                            "1) To provide any bank account where this money would be transferred into. 2) ",
                            "To serve as the guardian of this fund. 3) To make arrangement for me to come ",
                            "over to <mark>your</mark> <mark>country</mark>"
                            )]
                    ),
                    (
                        vec!["overseas"], 
                        vec![
                            "nominated account <mark>overseas</mark>."
                        ]
                    ),

                ],
            ),
            (vec![
                "孫子兵法",
                concat!(
                "<\"孫子兵法：\">",
                "孫子曰：兵者，國之大事，死生之地，存亡之道，不可不察也。", 
                "孫子曰：凡用兵之法，馳車千駟，革車千乘，帶甲十萬；千里饋糧，則內外之費賓客之用，膠漆之材，",
                "車甲之奉，日費千金，然後十萬之師舉矣。",
                "孫子曰：凡用兵之法，全國為上，破國次之；全旅為上，破旅次之；全卒為上，破卒次之；全伍為上，破伍次之。",
                "是故百戰百勝，非善之善者也；不戰而屈人之兵，善之善者也。",
                "孫子曰：昔之善戰者，先為不可勝，以待敵之可勝，不可勝在己，可勝在敵。故善戰者，能為不可勝，不能使敵必可勝。",
                "故曰：勝可知，而不可為。",
                "兵者，詭道也。故能而示之不能，用而示之不用，近而示之遠，遠而示之近。利而誘之，亂而取之，實而備之，強而避之，",
                "怒而撓之，卑而驕之，佚而勞之，親而離之。攻其無備，出其不意，此兵家之勝，不可先傳也。",
                "夫未戰而廟算勝者，得算多也；未戰而廟算不勝者，得算少也；多算勝，少算不勝，而況於無算乎？吾以此觀之，勝負見矣。",
                "孫子曰：凡治眾如治寡，分數是也。鬥眾如鬥寡，形名是也。三軍之眾，可使必受敵而無敗者，奇正是也。兵之所加，",
                "如以碬投卵者，虛實是也。",
            )],
                vec![
                    (
                        vec!["孫子兵法"], 
                        vec![
                            "<mark>孫子兵法</mark>", 
                            concat!(
                            "&lt;&quot;<mark>孫子兵法</mark>：&quot;&gt;孫子曰：兵者，國之大事，死生之地，存亡之道，",
                            "不可不察也。孫子曰：凡用兵之法，馳車千駟，革車千乘，帶甲十萬；千里饋糧，則內外之費賓客之用，膠"),
                        ]
                    ),
                    (
                        vec!["孫子曰"], 
                        vec![
                            concat!(
                            "&lt;&quot;孫子兵法：&quot;&gt;<mark>孫子曰</mark>：兵者，國之大事，死生之地，存亡之道，", 
                            "不可不察也。<mark>孫子曰</mark>：凡用兵之法，馳車千駟，革車千乘，帶甲十萬；千里饋糧，則內外之費賓",
                            )]
                    ),
                ],
            ),
        ];

        for (parts, tests) in inputs {
            let mut builder = TermIndexBuilder::new();
            let mut term_dict = HashMap::new();

            for (field_num, part) in parts.iter().enumerate() {
                let mut terms = Vec::new();
                for token in tokenize(part, Language::English, 40) {
                    let dict_len = term_dict.len() as u64 + 1;
                    terms.push(Term::from_token(
                        *term_dict
                            .entry(token.word.to_string())
                            .or_insert_with(|| dict_len),
                        0,
                        &token,
                    ));
                }
                builder.add_item(field_num as u8, 0, terms);
            }

            let compressed_term_index = builder.compress();
            let term_index = TermIndex::deserialize(&compressed_term_index[..]).unwrap();

            for (match_words, snippets) in tests {
                let mut match_terms = Vec::new();
                for word in &match_words {
                    match_terms.push(MatchTerm {
                        id: *term_dict.get(*word).unwrap_or(&0),
                        id_stemmed: 0,
                    });
                }

                let term_groups = term_index
                    .match_terms(&match_terms, None, false, true, true)
                    .unwrap()
                    .unwrap();

                assert_eq!(term_groups.len(), snippets.len());

                for (term_group, snippet) in term_groups.iter().zip(snippets.iter()) {
                    assert_eq!(
                        snippet,
                        &generate_snippet(&term_group.terms, parts[term_group.field_id as usize])
                            .unwrap()
                    );
                }
            }
        }
    }
}
