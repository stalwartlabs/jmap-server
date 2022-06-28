use crate::protocol::select;

use super::receiver::Token;

pub fn parse_select(tokens: Vec<Token>) -> super::Result<select::Arguments> {
    match tokens.len() {
        1 => Ok(select::Arguments {
            name: tokens.into_iter().next().unwrap().unwrap_string()?,
        }),
        0 => Err("Missing mailbox name.".into()),
        _ => Err("Too many arguments.".into()),
    }
}

#[cfg(test)]
mod tests {
    use crate::{parser::receiver::Receiver, protocol::select};

    #[test]
    fn parse_select() {
        let mut receiver = Receiver::new();

        for (command, arguments) in [
            (
                "A142 SELECT INBOX\r\n",
                select::Arguments {
                    name: "INBOX".to_string(),
                },
            ),
            (
                "A142 SELECT \"my funky mailbox\"\r\n",
                select::Arguments {
                    name: "my funky mailbox".to_string(),
                },
            ),
        ] {
            receiver.parse(command.as_bytes().to_vec());
            assert_eq!(
                super::parse_select(receiver.next_request().unwrap().unwrap().tokens).unwrap(),
                arguments
            );
        }
    }
}
