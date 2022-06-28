use crate::protocol::subscribe;

use super::receiver::Token;

pub fn parse_subscribe(tokens: Vec<Token>) -> super::Result<subscribe::Arguments> {
    match tokens.len() {
        1 => Ok(subscribe::Arguments {
            name: tokens.into_iter().next().unwrap().unwrap_string()?,
        }),
        0 => Err("Missing mailbox name.".into()),
        _ => Err("Too many arguments.".into()),
    }
}

#[cfg(test)]
mod tests {
    use crate::{parser::receiver::Receiver, protocol::subscribe};

    #[test]
    fn parse_subscribe() {
        let mut receiver = Receiver::new();

        for (command, arguments) in [
            (
                "A142 SUBSCRIBE #news.comp.mail.mime\r\n",
                subscribe::Arguments {
                    name: "#news.comp.mail.mime".to_string(),
                },
            ),
            (
                "A142 SUBSCRIBE \"#news.comp.mail.mime\"\r\n",
                subscribe::Arguments {
                    name: "#news.comp.mail.mime".to_string(),
                },
            ),
        ] {
            receiver.parse(command.as_bytes().to_vec());
            assert_eq!(
                super::parse_subscribe(receiver.next_request().unwrap().unwrap().tokens).unwrap(),
                arguments
            );
        }
    }
}
