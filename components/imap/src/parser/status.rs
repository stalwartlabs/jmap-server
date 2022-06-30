use crate::protocol::status;
use crate::protocol::status::Status;

use super::receiver::Token;

pub fn parse_status(tokens: Vec<Token>) -> super::Result<status::Arguments> {
    match tokens.len() {
        0..=3 => Err("Missing arguments.".into()),
        len => {
            let mut tokens = tokens.into_iter();
            let name = tokens.next().unwrap().unwrap_string()?;
            let mut items = Vec::with_capacity(len - 2);

            if tokens
                .next()
                .map_or(true, |token| !token.is_parenthesis_open())
            {
                return Err("Expected parenthesis after mailbox name.".into());
            }

            #[allow(clippy::while_let_on_iterator)]
            while let Some(token) = tokens.next() {
                match token {
                    Token::ParenthesisClose => break,
                    Token::Argument(value) => {
                        items.push(Status::parse(&value)?);
                    }
                    _ => return Err("Invalid status return option argument.".into()),
                }
            }

            Ok(status::Arguments { name, items })
        }
    }
}

impl Status {
    pub fn parse(value: &[u8]) -> super::Result<Self> {
        if value.eq_ignore_ascii_case(b"messages") {
            Ok(Self::Messages)
        } else if value.eq_ignore_ascii_case(b"uidnext") {
            Ok(Self::UidNext)
        } else if value.eq_ignore_ascii_case(b"uidvalidity") {
            Ok(Self::UidValidity)
        } else if value.eq_ignore_ascii_case(b"unseen") {
            Ok(Self::Unseen)
        } else if value.eq_ignore_ascii_case(b"deleted") {
            Ok(Self::Deleted)
        } else if value.eq_ignore_ascii_case(b"size") {
            Ok(Self::Size)
        } else {
            Err(format!(
                "Invalid status option '{}'.",
                String::from_utf8_lossy(value)
            )
            .into())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{parser::receiver::Receiver, protocol::status};

    #[test]
    fn parse_status() {
        let mut receiver = Receiver::new();

        for (command, arguments) in [(
            "A042 STATUS blurdybloop (UIDNEXT MESSAGES)\r\n",
            status::Arguments {
                name: "blurdybloop".to_string(),
                items: vec![status::Status::UidNext, status::Status::Messages],
            },
        )] {
            receiver.parse(command.as_bytes().to_vec());
            assert_eq!(
                super::parse_status(receiver.next_request().unwrap().unwrap().tokens).unwrap(),
                arguments
            );
        }
    }
}
