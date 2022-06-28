use crate::protocol::authenticate;

use super::receiver::Token;

pub fn parse_authenticate(tokens: Vec<Token>) -> super::Result<authenticate::Arguments> {
    match tokens.len() {
        1 | 2 => {
            let mut tokens = tokens.into_iter();
            Ok(authenticate::Arguments {
                mechanism: tokens.next().unwrap().unwrap_string()?,
                initial_response: tokens.next().map(|token| token.unwrap_bytes()),
            })
        }
        0 => Err("Missing arguments.".into()),
        _ => Err("Too many arguments.".into()),
    }
}

#[cfg(test)]
mod tests {
    use crate::{parser::receiver::Receiver, protocol::authenticate};

    #[test]
    fn parse_authenticate() {
        let mut receiver = Receiver::new();

        for (command, arguments) in [
            (
                "a002 AUTHENTICATE \"EXTERNAL\" {16+}\r\nfred@example.com\r\n",
                authenticate::Arguments {
                    mechanism: "EXTERNAL".to_string(),
                    initial_response: Some("fred@example.com".as_bytes().to_vec()),
                },
            ),
            (
                "A01 AUTHENTICATE PLAIN\r\n",
                authenticate::Arguments {
                    mechanism: "PLAIN".to_string(),
                    initial_response: None,
                },
            ),
        ] {
            receiver.parse(command.as_bytes().to_vec());
            assert_eq!(
                super::parse_authenticate(receiver.next_request().unwrap().unwrap().tokens)
                    .unwrap(),
                arguments
            );
        }
    }
}
