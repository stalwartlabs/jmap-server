use std::borrow::Cow;
use std::iter::Peekable;
use std::vec::IntoIter;

use jmap_mail::mail::HeaderName;
use mail_parser::parsers::header::{parse_header_name, HeaderParserResult};

use crate::protocol::fetch::{self, Attribute, Section};

use super::receiver::Token;
use super::{parse_integer, parse_sequence_set};

#[allow(clippy::while_let_on_iterator)]
pub fn parse_fetch(tokens: Vec<Token>) -> super::Result<fetch::Arguments> {
    if tokens.len() < 2 {
        return Err("Missing parameters.".into());
    }

    let mut tokens = tokens.into_iter().peekable();
    let mut attributes = Vec::new();
    let sequence_set = parse_sequence_set(
        &tokens
            .next()
            .ok_or_else(|| Cow::from("Missing sequence set."))?
            .unwrap_bytes(),
    )?;

    let mut in_parentheses = false;

    while let Some(token) = tokens.next() {
        match token {
            Token::Argument(value) => {
                if value.eq_ignore_ascii_case(b"ALL") {
                    attributes = vec![
                        Attribute::Flags,
                        Attribute::InternalDate,
                        Attribute::Rfc822Size,
                        Attribute::Envelope,
                    ];
                    break;
                } else if value.eq_ignore_ascii_case(b"FULL") {
                    attributes = vec![
                        Attribute::Flags,
                        Attribute::InternalDate,
                        Attribute::Rfc822Size,
                        Attribute::Envelope,
                        Attribute::Body,
                    ];
                    break;
                } else if value.eq_ignore_ascii_case(b"FAST") {
                    attributes = vec![
                        Attribute::Flags,
                        Attribute::InternalDate,
                        Attribute::Rfc822Size,
                    ];
                    break;
                } else if value.eq_ignore_ascii_case(b"ENVELOPE") {
                    attributes.push(Attribute::Envelope);
                } else if value.eq_ignore_ascii_case(b"FLAGS") {
                    attributes.push(Attribute::Flags);
                } else if value.eq_ignore_ascii_case(b"INTERNALDATE") {
                    attributes.push(Attribute::InternalDate);
                } else if value.eq_ignore_ascii_case(b"BODYSTRUCTURE") {
                    attributes.push(Attribute::BodyStructure);
                } else if value.eq_ignore_ascii_case(b"UID") {
                    attributes.push(Attribute::Uid);
                } else if value.eq_ignore_ascii_case(b"RFC822") {
                    attributes.push(if tokens.peek().map_or(false, |token| token.is_dot()) {
                        tokens.next();
                        let rfc822 = tokens
                            .next()
                            .ok_or_else(|| Cow::from("Missing RFC822 parameter."))?
                            .unwrap_bytes();
                        if rfc822.eq_ignore_ascii_case(b"HEADER") {
                            Attribute::Rfc822Header
                        } else if rfc822.eq_ignore_ascii_case(b"SIZE") {
                            Attribute::Rfc822Size
                        } else if rfc822.eq_ignore_ascii_case(b"TEXT") {
                            Attribute::Rfc822Text
                        } else {
                            return Err(format!(
                                "Invalid RFC822 parameter {:?}.",
                                String::from_utf8_lossy(&rfc822)
                            )
                            .into());
                        }
                    } else {
                        Attribute::Rfc822
                    });
                } else if value.eq_ignore_ascii_case(b"BODY") {
                    let is_peek = match tokens.peek() {
                        Some(Token::BracketOpen) => {
                            tokens.next();
                            false
                        }
                        Some(Token::Dot) => {
                            tokens.next();
                            if tokens
                                .next()
                                .map_or(true, |token| !token.eq_ignore_ascii_case(b"PEEK"))
                            {
                                return Err("Expected 'PEEK' after '.'.".into());
                            }
                            if tokens.next().map_or(true, |token| !token.is_bracket_open()) {
                                return Err("Expected '[' after 'BODY.PEEK'".into());
                            }
                            true
                        }
                        _ => {
                            attributes.push(Attribute::Body);
                            continue;
                        }
                    };

                    // Parse section-spect
                    let mut sections = Vec::new();
                    while let Some(token) = tokens.next() {
                        match token {
                            Token::BracketClose => break,
                            Token::Argument(value) => {
                                let section = if value.eq_ignore_ascii_case(b"HEADER") {
                                    if let Some(Token::Dot) = tokens.peek() {
                                        tokens.next();
                                        if tokens.next().map_or(true, |token| {
                                            !token.eq_ignore_ascii_case(b"FIELDS")
                                        }) {
                                            return Err("Expected 'FIELDS' after 'HEADER.'.".into());
                                        }
                                        let is_not = if let Some(Token::Dot) = tokens.peek() {
                                            tokens.next();
                                            if tokens.next().map_or(true, |token| {
                                                !token.eq_ignore_ascii_case(b"NOT")
                                            }) {
                                                return Err(
                                                    "Expected 'NOT' after 'HEADER.FIELDS.'.".into(),
                                                );
                                            }
                                            true
                                        } else {
                                            false
                                        };
                                        if tokens
                                            .next()
                                            .map_or(true, |token| !token.is_parenthesis_open())
                                        {
                                            return Err(
                                                "Expected '(' after 'HEADER.FIELDS'.".into()
                                            );
                                        }
                                        let mut fields = Vec::new();
                                        while let Some(token) = tokens.next() {
                                            match token {
                                                Token::ParenthesisClose => break,
                                                Token::Argument(value) => {
                                                    fields.push(match parse_header_name(&value) {
                                                        (
                                                            _,
                                                            HeaderParserResult::Rfc(rfc_header),
                                                        ) => HeaderName::Rfc(rfc_header),
                                                        (
                                                            _,
                                                            HeaderParserResult::Other(other_header),
                                                        ) => HeaderName::Other(
                                                            other_header.as_ref().to_owned(),
                                                        ),
                                                        _ => {
                                                            return Err(format!(
                                                                "Failed to parse header {:?}",
                                                                String::from_utf8_lossy(&value)
                                                            )
                                                            .into())
                                                        }
                                                    });
                                                }
                                                _ => return Err("Expected field name.".into()),
                                            }
                                        }
                                        Section::HeaderFields {
                                            not: is_not,
                                            fields,
                                        }
                                    } else {
                                        Section::Header
                                    }
                                } else if value.eq_ignore_ascii_case(b"TEXT") {
                                    Section::Text
                                } else if value.eq_ignore_ascii_case(b"MIME") {
                                    Section::Mime
                                } else {
                                    Section::Part {
                                        num: parse_integer(&value)?,
                                    }
                                };
                                sections.push(section);
                            }
                            Token::Dot => (),
                            _ => {
                                return Err(format!(
                                    "Invalid token {:?} found in section-spect.",
                                    token
                                )
                                .into())
                            }
                        }
                    }

                    attributes.push(Attribute::BodySection {
                        peek: is_peek,
                        sections,
                        partial: parse_partial(&mut tokens)?,
                    });
                } else if value.eq_ignore_ascii_case(b"BINARY") {
                    let (is_peek, is_size) = if let Some(Token::Dot) = tokens.peek() {
                        tokens.next();
                        let param = tokens
                            .next()
                            .ok_or_else(|| Cow::from("Missing parameter after 'BINARY.'."))?
                            .unwrap_bytes();
                        if param.eq_ignore_ascii_case(b"PEEK") {
                            (true, false)
                        } else if param.eq_ignore_ascii_case(b"SIZE") {
                            (false, true)
                        } else {
                            return Err("Expected 'PEEK' or 'SIZE' after 'BINARY.'.".into());
                        }
                    } else {
                        (false, false)
                    };

                    // Parse section-part
                    if tokens.next().map_or(true, |token| !token.is_bracket_open()) {
                        return Err("Expected '[' after 'BINARY'.".into());
                    }
                    let mut sections = Vec::new();
                    while let Some(token) = tokens.next() {
                        match token {
                            Token::Argument(value) => {
                                sections.push(parse_integer(&value)?);
                            }
                            Token::Dot => (),
                            Token::BracketClose => break,
                            _ => {
                                return Err(format!(
                                    "Expected part section integer, got {:?}.",
                                    token.to_string()
                                )
                                .into())
                            }
                        }
                    }
                    attributes.push(if !is_size {
                        Attribute::Binary {
                            peek: is_peek,
                            sections,
                            partial: parse_partial(&mut tokens)?,
                        }
                    } else {
                        Attribute::BinarySize { sections }
                    });
                } else {
                    return Err(
                        format!("Invalid attribute {:?}", String::from_utf8_lossy(&value)).into(),
                    );
                }
            }
            Token::ParenthesisOpen => {
                if !in_parentheses {
                    in_parentheses = true;
                } else {
                    return Err("Unexpected parenthesis open.".into());
                }
            }
            Token::ParenthesisClose => {
                if in_parentheses {
                    break;
                } else {
                    return Err("Unexpected parenthesis close.".into());
                }
            }
            _ => return Err(format!("Invalid fetch argument {:?}.", token.to_string()).into()),
        }
    }

    Ok(fetch::Arguments {
        sequence_set,
        attributes,
    })
}

pub fn parse_partial(tokens: &mut Peekable<IntoIter<Token>>) -> super::Result<Option<(u64, u64)>> {
    if tokens.peek().map_or(true, |token| !token.is_lt()) {
        return Ok(None);
    }
    tokens.next();

    let start = parse_integer(
        &tokens
            .next()
            .ok_or_else(|| Cow::from("Missing partial start."))?
            .unwrap_bytes(),
    )?;

    if tokens.next().map_or(true, |token| !token.is_dot()) {
        return Err("Expected '.' after partial start.".into());
    }

    let end = parse_integer(
        &tokens
            .next()
            .ok_or_else(|| Cow::from("Missing partial end."))?
            .unwrap_bytes(),
    )?;

    if end == 0 || end < start {
        return Err("Invalid partial range.".into());
    }

    if tokens.next().map_or(true, |token| !token.is_gt()) {
        return Err("Expected '>' after range.".into());
    }

    Ok(Some((start, end)))
}

/*

   fetch           = "FETCH" SP sequence-set SP (
                     "ALL" / "FULL" / "FAST" /
                     fetch-att / "(" fetch-att *(SP fetch-att) ")")

   fetch-att       = "ENVELOPE" / "FLAGS" / "INTERNALDATE" /
                     "RFC822" [".HEADER" / ".SIZE" / ".TEXT"] /
                     "BODY" ["STRUCTURE"] / "UID" /
                     "BODY" section [partial] /
                     "BODY.PEEK" section [partial] /
                     "BINARY" [".PEEK"] section-binary [partial] /
                     "BINARY.SIZE" section-binary

   partial         = "<" number64 "." nz-number64 ">"
                       ; Partial FETCH request. 0-based offset of
                       ; the first octet, followed by the number of
                       ; octets in the fragment.

   section         = "[" [section-spec] "]"

   section-binary  = "[" [section-part] "]"

   section-msgtext = "HEADER" /
                     "HEADER.FIELDS" [".NOT"] SP header-list /
                     "TEXT"
                       ; top-level or MESSAGE/RFC822 or
                       ; MESSAGE/GLOBAL part

   section-part    = nz-number *("." nz-number)
                       ; body part reference.
                       ; Allows for accessing nested body parts.

   section-spec    = section-msgtext / (section-part ["." section-text])

   section-text    = section-msgtext / "MIME"
                       ; text other than actual body part (headers,
                       ; etc.)


*/

#[cfg(test)]
mod tests {
    use jmap_mail::mail::HeaderName;
    use mail_parser::RfcHeader;

    use crate::{
        parser::receiver::Receiver,
        protocol::{
            fetch::{self, Attribute, Section},
            Sequence,
        },
    };

    #[test]
    fn parse_fetch() {
        let mut receiver = Receiver::new();

        for (command, arguments) in [
            (
                "A654 FETCH 2:4 (FLAGS BODY[HEADER.FIELDS (DATE FROM)])\r\n",
                fetch::Arguments {
                    sequence_set: vec![Sequence::range(2.into(), 4.into())],
                    attributes: vec![
                        Attribute::Flags,
                        Attribute::BodySection {
                            peek: false,
                            sections: vec![Section::HeaderFields {
                                not: false,
                                fields: vec![
                                    HeaderName::Rfc(RfcHeader::Date),
                                    HeaderName::Rfc(RfcHeader::From),
                                ],
                            }],
                            partial: None,
                        },
                    ],
                },
            ),
            (
                "A001 FETCH 1 BODY[]\r\n",
                fetch::Arguments {
                    sequence_set: vec![Sequence::number(1)],
                    attributes: vec![Attribute::BodySection {
                        peek: false,
                        sections: vec![],
                        partial: None,
                    }],
                },
            ),
            (
                "A001 FETCH 1 (BODY[HEADER])\r\n",
                fetch::Arguments {
                    sequence_set: vec![Sequence::number(1)],
                    attributes: vec![Attribute::BodySection {
                        peek: false,
                        sections: vec![Section::Header],
                        partial: None,
                    }],
                },
            ),
            (
                "A001 FETCH 1 (BODY.PEEK[HEADER.FIELDS (X-MAILER)])\r\n",
                fetch::Arguments {
                    sequence_set: vec![Sequence::number(1)],
                    attributes: vec![Attribute::BodySection {
                        peek: true,
                        sections: vec![Section::HeaderFields {
                            not: false,
                            fields: vec![HeaderName::Other("X-MAILER".to_string())],
                        }],
                        partial: None,
                    }],
                },
            ),
            (
                "A001 FETCH 1 (BODY[HEADER.FIELDS.NOT (FROM TO SUBJECT)])\r\n",
                fetch::Arguments {
                    sequence_set: vec![Sequence::number(1)],
                    attributes: vec![Attribute::BodySection {
                        peek: false,
                        sections: vec![Section::HeaderFields {
                            not: true,
                            fields: vec![
                                HeaderName::Rfc(RfcHeader::From),
                                HeaderName::Rfc(RfcHeader::To),
                                HeaderName::Rfc(RfcHeader::Subject),
                            ],
                        }],
                        partial: None,
                    }],
                },
            ),
            (
                "A001 FETCH 1 (BODY[MIME] BODY[TEXT])\r\n",
                fetch::Arguments {
                    sequence_set: vec![Sequence::number(1)],
                    attributes: vec![
                        Attribute::BodySection {
                            peek: false,
                            sections: vec![Section::Mime],
                            partial: None,
                        },
                        Attribute::BodySection {
                            peek: false,
                            sections: vec![Section::Text],
                            partial: None,
                        },
                    ],
                },
            ),
            (
                "A001 FETCH 1 (BODYSTRUCTURE ENVELOPE FLAGS INTERNALDATE UID)\r\n",
                fetch::Arguments {
                    sequence_set: vec![Sequence::number(1)],
                    attributes: vec![
                        Attribute::BodyStructure,
                        Attribute::Envelope,
                        Attribute::Flags,
                        Attribute::InternalDate,
                        Attribute::Uid,
                    ],
                },
            ),
            (
                "A001 FETCH 1 (RFC822 RFC822.HEADER RFC822.SIZE RFC822.TEXT)\r\n",
                fetch::Arguments {
                    sequence_set: vec![Sequence::number(1)],
                    attributes: vec![
                        Attribute::Rfc822,
                        Attribute::Rfc822Header,
                        Attribute::Rfc822Size,
                        Attribute::Rfc822Text,
                    ],
                },
            ),
            (
                concat!(
                    "A001 FETCH 1 (",
                    "BODY[4.2.HEADER]<0.20> ",
                    "BODY.PEEK[3.2.2.2] ",
                    "BODY[4.2.TEXT]<4.100> ",
                    "BINARY[1.2.3] ",
                    "BINARY.PEEK[4] ",
                    "BINARY[6.5.4]<100.200> ",
                    "BINARY.PEEK[7]<9.88> ",
                    "BINARY.SIZE[9.1]",
                    ")\r\n"
                ),
                fetch::Arguments {
                    sequence_set: vec![Sequence::number(1)],
                    attributes: vec![
                        Attribute::BodySection {
                            peek: false,
                            sections: vec![
                                Section::Part { num: 4 },
                                Section::Part { num: 2 },
                                Section::Header,
                            ],
                            partial: Some((0, 20)),
                        },
                        Attribute::BodySection {
                            peek: true,
                            sections: vec![
                                Section::Part { num: 3 },
                                Section::Part { num: 2 },
                                Section::Part { num: 2 },
                                Section::Part { num: 2 },
                            ],
                            partial: None,
                        },
                        Attribute::BodySection {
                            peek: false,
                            sections: vec![
                                Section::Part { num: 4 },
                                Section::Part { num: 2 },
                                Section::Text,
                            ],
                            partial: Some((4, 100)),
                        },
                        Attribute::Binary {
                            peek: false,
                            sections: vec![1, 2, 3],
                            partial: None,
                        },
                        Attribute::Binary {
                            peek: true,
                            sections: vec![4],
                            partial: None,
                        },
                        Attribute::Binary {
                            peek: false,
                            sections: vec![6, 5, 4],
                            partial: Some((100, 200)),
                        },
                        Attribute::Binary {
                            peek: true,
                            sections: vec![7],
                            partial: Some((9, 88)),
                        },
                        Attribute::BinarySize {
                            sections: vec![9, 1],
                        },
                    ],
                },
            ),
            (
                "A001 FETCH 1 ALL\r\n",
                fetch::Arguments {
                    sequence_set: vec![Sequence::number(1)],
                    attributes: vec![
                        Attribute::Flags,
                        Attribute::InternalDate,
                        Attribute::Rfc822Size,
                        Attribute::Envelope,
                    ],
                },
            ),
            (
                "A001 FETCH 1 FULL\r\n",
                fetch::Arguments {
                    sequence_set: vec![Sequence::number(1)],
                    attributes: vec![
                        Attribute::Flags,
                        Attribute::InternalDate,
                        Attribute::Rfc822Size,
                        Attribute::Envelope,
                        Attribute::Body,
                    ],
                },
            ),
            (
                "A001 FETCH 1 FAST\r\n",
                fetch::Arguments {
                    sequence_set: vec![Sequence::number(1)],
                    attributes: vec![
                        Attribute::Flags,
                        Attribute::InternalDate,
                        Attribute::Rfc822Size,
                    ],
                },
            ),
        ] {
            receiver.parse(command.as_bytes().to_vec());
            assert_eq!(
                super::parse_fetch(receiver.next_request().unwrap().unwrap().tokens)
                    .expect(command),
                arguments,
                "{}",
                command
            );
        }
    }
}
