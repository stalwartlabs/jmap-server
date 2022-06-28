use crate::protocol::examine;

use super::receiver::Token;

pub fn parse_examine(tokens: Vec<Token>) -> super::Result<examine::Arguments> {
    match tokens.len() {
        1 => Ok(examine::Arguments {
            name: tokens.into_iter().next().unwrap().unwrap_string()?,
        }),
        0 => Err("Missing mailbox name.".into()),
        _ => Err("Too many arguments.".into()),
    }
}

#[cfg(test)]
mod tests {
    use crate::{parser::receiver::Receiver, protocol::examine};

    #[test]
    fn parse_examine() {
        let mut receiver = Receiver::new();

        for (command, arguments) in [
            (
                "A142 EXAMINE INBOX\r\n",
                examine::Arguments {
                    name: "INBOX".to_string(),
                },
            ),
            (
                "A142 EXAMINE {4+}\r\ntest\r\n",
                examine::Arguments {
                    name: "test".to_string(),
                },
            ),
        ] {
            receiver.parse(command.as_bytes().to_vec());
            assert_eq!(
                super::parse_examine(receiver.next_request().unwrap().unwrap().tokens).unwrap(),
                arguments
            );
        }
    }
}
