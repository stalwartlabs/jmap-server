use std::borrow::Cow;

use jmap_mail::mail::schema::Keyword;

use crate::protocol::append;

use super::{parse_datetime, receiver::Token, ImapFlag};

pub fn parse_append(tokens: Vec<Token>) -> super::Result<append::Arguments> {
    match tokens.len() {
        0 | 1 => Err("Missing arguments.".into()),
        _ => {
            let mut tokens = tokens.into_iter();
            let mailbox_name = tokens.next().unwrap().unwrap_string()?;
            let mut flags = Vec::new();
            let token = match tokens.next().unwrap() {
                Token::ParenthesisOpen => {
                    #[allow(clippy::while_let_on_iterator)]
                    while let Some(token) = tokens.next() {
                        match token {
                            Token::ParenthesisClose => break,
                            Token::Argument(value) => {
                                flags.push(Keyword::parse_imap(value)?);
                            }
                            _ => return Err("Invalid flag.".into()),
                        }
                    }
                    tokens
                        .next()
                        .ok_or_else(|| Cow::from("Missing paramaters after flags."))?
                }
                token => token,
            };
            let (message, received_at) = if let Some(next_token) = tokens.next() {
                (
                    next_token.unwrap_bytes(),
                    parse_datetime(&token.unwrap_bytes())?.into(),
                )
            } else {
                (token.unwrap_bytes(), None)
            };

            Ok(append::Arguments {
                mailbox_name,
                message,
                flags,
                received_at,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use jmap_mail::mail::schema::Keyword;
    use store::core::tag::Tag;

    use crate::{parser::receiver::Receiver, protocol::append};

    #[test]
    fn parse_append() {
        let mut receiver = Receiver::new();

        for (command, arguments) in [
            (
                "A003 APPEND saved-messages (\\Seen) {1+}\r\na\r\n",
                append::Arguments {
                    mailbox_name: "saved-messages".to_string(),
                    message: vec![b'a'],
                    flags: vec![Keyword {
                        tag: Tag::Static(Keyword::SEEN),
                    }],
                    received_at: None,
                },
            ),
            (
                "A003 APPEND \"hello world\" (\\Seen \\Draft $MDNSent) {1+}\r\na\r\n",
                append::Arguments {
                    mailbox_name: "hello world".to_string(),
                    message: vec![b'a'],
                    flags: vec![
                        Keyword {
                            tag: Tag::Static(Keyword::SEEN),
                        },
                        Keyword {
                            tag: Tag::Static(Keyword::DRAFT),
                        },
                        Keyword {
                            tag: Tag::Static(Keyword::MDN_SENT),
                        },
                    ],
                    received_at: None,
                },
            ),
            (
                "A003 APPEND \"hi\" ($Junk) \"7-Feb-1994 22:43:04 -0800\" {1+}\r\na\r\n",
                append::Arguments {
                    mailbox_name: "hi".to_string(),
                    message: vec![b'a'],
                    flags: vec![Keyword {
                        tag: Tag::Static(Keyword::JUNK),
                    }],
                    received_at: Some(760689784),
                },
            ),
            (
                "A003 APPEND \"hi\" \"20-Nov-2022 23:59:59 +0300\" {1+}\r\na\r\n",
                append::Arguments {
                    mailbox_name: "hi".to_string(),
                    message: vec![b'a'],
                    flags: vec![],
                    received_at: Some(1668977999),
                },
            ),
        ] {
            receiver.parse(command.as_bytes().to_vec());
            assert_eq!(
                super::parse_append(receiver.next_request().unwrap().unwrap().tokens).unwrap(),
                arguments,
                "{:?}",
                command
            );
        }
    }
}
