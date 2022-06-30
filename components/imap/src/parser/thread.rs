use std::borrow::Cow;

use mail_parser::decoders::charsets::map::get_charset_decoder;
use store::read::filter::LogicalOperator;

use crate::protocol::{
    search::Filter,
    thread::{self, Algorithm},
};

use super::{receiver::Token, search::parse_filters};

#[allow(clippy::while_let_on_iterator)]
pub fn parse_thread(tokens: Vec<Token>) -> super::Result<thread::Arguments> {
    if tokens.is_empty() {
        return Err("Missing thread criteria.".into());
    }

    let mut tokens = tokens.into_iter().peekable();
    let algorithm = Algorithm::parse(
        &tokens
            .next()
            .ok_or_else(|| Cow::from("Missing threading algorithm."))?
            .unwrap_bytes(),
    )?;

    let decoder = get_charset_decoder(
        &tokens
            .next()
            .ok_or_else(|| Cow::from("Missing charset."))?
            .unwrap_bytes(),
    );

    let mut filters = parse_filters(&mut tokens, decoder)?;
    match filters.len() {
        0 => Err(Cow::from("No filters found in command.")),
        1 => Ok(thread::Arguments {
            algorithm,
            filter: filters.pop().unwrap(),
        }),
        _ => Ok(thread::Arguments {
            algorithm,
            filter: Filter::Operator(LogicalOperator::And, filters),
        }),
    }
}

impl Algorithm {
    pub fn parse(value: &[u8]) -> super::Result<Self> {
        if value.eq_ignore_ascii_case(b"ORDEREDSUBJECT") {
            Ok(Self::OrderedSubject)
        } else if value.eq_ignore_ascii_case(b"REFERENCES") {
            Ok(Self::References)
        } else {
            Err(format!(
                "Invalid threading algorithm {:?}",
                String::from_utf8_lossy(value)
            )
            .into())
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        parser::receiver::Receiver,
        protocol::{
            search::Filter,
            thread::{self, Algorithm},
        },
    };

    #[test]
    fn parse_thread() {
        let mut receiver = Receiver::new();

        for (command, arguments) in [
            (
                b"A283 THREAD ORDEREDSUBJECT UTF-8 SINCE 5-MAR-2000\r\n".to_vec(),
                thread::Arguments {
                    algorithm: Algorithm::OrderedSubject,
                    filter: Filter::Since(952214400),
                },
            ),
            (
                b"A284 THREAD REFERENCES US-ASCII TEXT \"gewp\"\r\n".to_vec(),
                thread::Arguments {
                    algorithm: Algorithm::References,
                    filter: Filter::Text("gewp".to_string()),
                },
            ),
        ] {
            let command_str = String::from_utf8_lossy(&command).into_owned();
            receiver.parse(command);
            assert_eq!(
                super::parse_thread(receiver.next_request().unwrap().unwrap().tokens)
                    .expect(&command_str),
                arguments,
                "{}",
                command_str
            );
        }
    }
}
