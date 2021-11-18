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

pub fn generate_snippet(terms: &[Term], parts: &[&str]) -> Option<Vec<String>> {
    let mut result = Vec::new();
    let mut terms = terms.iter().peekable();

    for part in parts {
        let mut snippet = String::with_capacity(parts.len());
        let start_offset = terms.peek()?.offset as usize;

        if start_offset > 0 {
            let mut word_count = 0;
            let mut from_offset = 0;
            if part.len() > 240 {
                for (pos, char) in part.get(0..start_offset)?.char_indices().rev() {
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

            for char in part.get(from_offset..start_offset)?.chars() {
                escape_char(char, &mut snippet);
            }
        }

        while let Some(term) = terms.next() {
            snippet.push_str("<mark>");
            snippet.push_str(
                part.get(term.offset as usize..term.offset as usize + term.len as usize)?,
            );
            snippet.push_str("</mark>");

            let (next_offset, next_field_num) = if let Some(next_term) = terms.peek() {
                (
                    if next_term.field_num == term.field_num {
                        next_term.offset as usize
                    } else {
                        part.len()
                    },
                    next_term.field_num,
                )
            } else {
                (part.len(), u16::MAX)
            };

            for char in part
                .get(term.offset as usize + term.len as usize..next_offset)?
                .chars()
            {
                if snippet.len() + 3 > 255 {
                    break;
                }
                escape_char(char, &mut snippet);
            }

            if next_field_num != term.field_num {
                break;
            } else if snippet.len() + 3 > 255 {
                while let Some(next_term) = terms.peek() {
                    if next_term.field_num != term.field_num {
                        break;
                    }
                    terms.next();
                }
            }
        }

        result.push(snippet);
    }

    Some(result)
}

#[cfg(test)]
mod tests {
    use nlp::{tokenizers::tokenize, Language};

    use crate::term_index::{TermIndex, TermIndexBuilder};

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
            let mut term_builder = TermIndexBuilder::new();
            for (field_num, part) in parts.iter().enumerate() {
                for token in tokenize(part, Language::English, 40) {
                    term_builder.add_term(
                        if field_num == 0 { 0 } else { 1 },
                        field_num as u16,
                        token,
                    );
                }
            }
            let (raw_map, raw_pos) = term_builder.serialize().unwrap();
            let map = TermIndex::new(&raw_map, &raw_pos).unwrap();

            for test in tests {
                let results = map.search_any(&test.0, None).unwrap();

                let snippet = generate_snippet(
                    &results,
                    if results[0].field_num == 0 {
                        &parts
                    } else {
                        &parts[1..]
                    },
                )
                .unwrap();

                assert_eq!(snippet, test.1);
            }
        }
    }
}
