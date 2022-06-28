use crate::protocol::unsubscribe;

use super::receiver::Token;

pub fn parse_unsubscribe(tokens: Vec<Token>) -> super::Result<unsubscribe::Arguments> {
    match tokens.len() {
        1 => Ok(unsubscribe::Arguments {
            name: tokens.into_iter().next().unwrap().unwrap_string()?,
        }),
        0 => Err("Missing mailbox name.".into()),
        _ => Err("Too many arguments.".into()),
    }
}

#[cfg(test)]
mod tests {
    use crate::{parser::receiver::Receiver, protocol::unsubscribe};

    #[test]
    fn parse_unsubscribe() {
        let mut receiver = Receiver::new();

        for (command, arguments) in [
            (
                "A142 UNSUBSCRIBE #news.comp.mail.mime\r\n",
                unsubscribe::Arguments {
                    name: "#news.comp.mail.mime".to_string(),
                },
            ),
            (
                "A142 UNSUBSCRIBE \"#news.comp.mail.mime\"\r\n",
                unsubscribe::Arguments {
                    name: "#news.comp.mail.mime".to_string(),
                },
            ),
        ] {
            receiver.parse(command.as_bytes().to_vec());
            assert_eq!(
                super::parse_unsubscribe(receiver.next_request().unwrap().unwrap().tokens).unwrap(),
                arguments
            );
        }
    }
}
