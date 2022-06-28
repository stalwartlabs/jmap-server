use crate::protocol::enable;

use super::receiver::Token;

pub fn parse_enable(tokens: Vec<Token>) -> super::Result<enable::Arguments> {
    if !tokens.is_empty() {
        Ok(enable::Arguments {
            capabilities: tokens
                .into_iter()
                .filter_map(|token| token.unwrap_string().ok())
                .collect(),
        })
    } else {
        Err("Too many arguments.".into())
    }
}

#[cfg(test)]
mod tests {
    use crate::{parser::receiver::Receiver, protocol::enable};

    #[test]
    fn parse_enable() {
        let mut receiver = Receiver::new();

        for (command, arguments) in [(
            "t2 ENABLE CONDSTORE X-GOOD-IDEA\r\n",
            enable::Arguments {
                capabilities: vec!["CONDSTORE".to_string(), "X-GOOD-IDEA".to_string()],
            },
        )] {
            receiver.parse(command.as_bytes().to_vec());
            assert_eq!(
                super::parse_enable(receiver.next_request().unwrap().unwrap().tokens).unwrap(),
                arguments
            );
        }
    }
}
