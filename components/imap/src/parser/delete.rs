use crate::protocol::delete;

use super::receiver::Token;

pub fn parse_delete(tokens: Vec<Token>) -> super::Result<delete::Arguments> {
    match tokens.len() {
        1 => Ok(delete::Arguments {
            name: tokens.into_iter().next().unwrap().unwrap_string()?,
        }),
        0 => Err("Missing mailbox name.".into()),
        _ => Err("Too many arguments.".into()),
    }
}

#[cfg(test)]
mod tests {
    use crate::{parser::receiver::Receiver, protocol::delete};

    #[test]
    fn parse_delete() {
        let mut receiver = Receiver::new();

        for (command, arguments) in [
            (
                "A142 DELETE INBOX\r\n",
                delete::Arguments {
                    name: "INBOX".to_string(),
                },
            ),
            (
                "A142 DELETE \"my funky mailbox\"\r\n",
                delete::Arguments {
                    name: "my funky mailbox".to_string(),
                },
            ),
        ] {
            receiver.parse(command.as_bytes().to_vec());
            assert_eq!(
                super::parse_delete(receiver.next_request().unwrap().unwrap().tokens).unwrap(),
                arguments
            );
        }
    }
}
