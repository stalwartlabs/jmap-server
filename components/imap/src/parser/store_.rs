use std::borrow::Cow;

use jmap_mail::mail::schema::Keyword;

use crate::protocol::store_::{self, Operation};

use super::{parse_sequence_set, receiver::Token, ImapFlag};

pub fn parse_store(tokens: Vec<Token>) -> super::Result<store_::Arguments> {
    let mut tokens = tokens.into_iter();
    let sequence_set = parse_sequence_set(
        &tokens
            .next()
            .ok_or_else(|| Cow::from("Missing sequence set."))?
            .unwrap_bytes(),
    )?;
    let operation = tokens
        .next()
        .ok_or_else(|| Cow::from("Missing message data item name."))?
        .unwrap_bytes();
    let operation = if operation.eq_ignore_ascii_case(b"FLAGS") {
        Operation::Set
    } else if operation.eq_ignore_ascii_case(b"FLAGS.SILENT") {
        Operation::SetSilent
    } else if operation.eq_ignore_ascii_case(b"+FLAGS") {
        Operation::Add
    } else if operation.eq_ignore_ascii_case(b"+FLAGS.SILENT") {
        Operation::AddSilent
    } else if operation.eq_ignore_ascii_case(b"-FLAGS") {
        Operation::Clear
    } else if operation.eq_ignore_ascii_case(b"-FLAGS.SILENT") {
        Operation::ClearSilent
    } else {
        return Err(Cow::from(format!(
            "Unsupported message data item name: {:?}",
            String::from_utf8_lossy(&operation)
        )));
    };

    if tokens
        .next()
        .map_or(true, |token| !token.is_parenthesis_open())
    {
        return Err("Expected store parameters between parentheses.".into());
    }

    let mut keywords = Vec::new();
    for token in tokens {
        match token {
            Token::Argument(flag) => {
                keywords.push(Keyword::parse_imap(flag)?);
            }
            Token::ParenthesisClose => {
                break;
            }
            _ => {
                return Err("Unsupported flag.".into());
            }
        }
    }

    if !keywords.is_empty() {
        Ok(store_::Arguments {
            sequence_set,
            operation,
            keywords,
        })
    } else {
        Err("Missing flags.".into())
    }
}

#[cfg(test)]
mod tests {
    use jmap_mail::mail::schema::Keyword;
    use store::core::tag::Tag;

    use crate::{
        parser::receiver::Receiver,
        protocol::{
            store_::{self, Operation},
            Sequence,
        },
    };

    #[test]
    fn parse_store() {
        let mut receiver = Receiver::new();

        for (command, arguments) in [
            (
                "A003 STORE 2:4 +FLAGS (\\Deleted)\r\n",
                store_::Arguments {
                    sequence_set: vec![Sequence::Range {
                        start: 2.into(),
                        end: 4.into(),
                    }],
                    operation: Operation::Add,
                    keywords: vec![Keyword {
                        tag: Tag::Static(Keyword::DELETED),
                    }],
                },
            ),
            (
                "A004 STORE *:100 -FLAGS.SILENT ($Phishing $Junk)\"\r\n",
                store_::Arguments {
                    sequence_set: vec![Sequence::Range {
                        start: None,
                        end: 100.into(),
                    }],
                    operation: Operation::ClearSilent,
                    keywords: vec![
                        Keyword {
                            tag: Tag::Static(Keyword::PHISHING),
                        },
                        Keyword {
                            tag: Tag::Static(Keyword::JUNK),
                        },
                    ],
                },
            ),
        ] {
            receiver.parse(command.as_bytes().to_vec());
            assert_eq!(
                super::parse_store(receiver.next_request().unwrap().unwrap().tokens).unwrap(),
                arguments
            );
        }
    }
}
