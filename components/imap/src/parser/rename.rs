use crate::protocol::rename;

use super::receiver::Token;

pub fn parse_rename(tokens: Vec<Token>) -> super::Result<rename::Arguments> {
    match tokens.len() {
        2 => {
            let mut tokens = tokens.into_iter();
            Ok(rename::Arguments {
                name: tokens.next().unwrap().unwrap_string()?,
                new_name: tokens.next().unwrap().unwrap_string()?,
            })
        }
        0 => Err("Missing argument.".into()),
        1 => Err("Missing new mailbox name.".into()),
        _ => Err("Too many arguments.".into()),
    }
}

#[cfg(test)]
mod tests {
    use crate::{parser::receiver::Receiver, protocol::rename};

    #[test]
    fn parse_rename() {
        let mut receiver = Receiver::new();

        for (command, arguments) in [
            (
                "A142 RENAME \"my funky mailbox\" Private\r\n",
                rename::Arguments {
                    name: "my funky mailbox".to_string(),
                    new_name: "Private".to_string(),
                },
            ),
            (
                "A142 RENAME {1+}\r\na {1+}\r\nb\r\n",
                rename::Arguments {
                    name: "a".to_string(),
                    new_name: "b".to_string(),
                },
            ),
        ] {
            receiver.parse(command.as_bytes().to_vec());
            assert_eq!(
                super::parse_rename(receiver.next_request().unwrap().unwrap().tokens).unwrap(),
                arguments
            );
        }
    }
}
