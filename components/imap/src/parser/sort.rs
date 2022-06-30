use std::borrow::Cow;

use mail_parser::decoders::charsets::map::get_charset_decoder;
use store::read::filter::LogicalOperator;

use crate::protocol::{
    search::Filter,
    sort::{self, Comparator, Sort},
};

use super::{receiver::Token, search::parse_filters};

#[allow(clippy::while_let_on_iterator)]
pub fn parse_sort(tokens: Vec<Token>) -> super::Result<sort::Arguments> {
    if tokens.is_empty() {
        return Err("Missing sort criteria.".into());
    }

    let mut tokens = tokens.into_iter().peekable();
    let mut sort = Vec::new();

    if tokens
        .next()
        .map_or(true, |token| !token.is_parenthesis_open())
    {
        return Err("Expected sort criteria between parentheses.".into());
    }

    let mut is_ascending = true;
    while let Some(token) = tokens.next() {
        match token {
            Token::ParenthesisClose => break,
            Token::Argument(value) => {
                if value.eq_ignore_ascii_case(b"REVERSE") {
                    is_ascending = false;
                } else {
                    sort.push(Comparator {
                        sort: Sort::parse(&value)?,
                        ascending: is_ascending,
                    });
                    is_ascending = true;
                }
            }
            _ => return Err("Invalid result option argument.".into()),
        }
    }

    if sort.is_empty() {
        return Err("Missing sort criteria.".into());
    }

    let decoder = get_charset_decoder(
        &tokens
            .next()
            .ok_or_else(|| Cow::from("Missing charset."))?
            .unwrap_bytes(),
    );

    let mut filters = parse_filters(&mut tokens, decoder)?;
    match filters.len() {
        0 => Err(Cow::from("No filters found in command.")),
        1 => Ok(sort::Arguments {
            sort,
            filter: filters.pop().unwrap(),
        }),
        _ => Ok(sort::Arguments {
            sort,
            filter: Filter::Operator(LogicalOperator::And, filters),
        }),
    }
}

impl Sort {
    pub fn parse(value: &[u8]) -> super::Result<Self> {
        if value.eq_ignore_ascii_case(b"ARRIVAL") {
            Ok(Self::Arrival)
        } else if value.eq_ignore_ascii_case(b"CC") {
            Ok(Self::Cc)
        } else if value.eq_ignore_ascii_case(b"DATE") {
            Ok(Self::Date)
        } else if value.eq_ignore_ascii_case(b"FROM") {
            Ok(Self::From)
        } else if value.eq_ignore_ascii_case(b"SIZE") {
            Ok(Self::Size)
        } else if value.eq_ignore_ascii_case(b"SUBJECT") {
            Ok(Self::Subject)
        } else if value.eq_ignore_ascii_case(b"TO") {
            Ok(Self::To)
        } else if value.eq_ignore_ascii_case(b"DISPLAYFROM") {
            Ok(Self::DisplayFrom)
        } else if value.eq_ignore_ascii_case(b"DISPLAYTO") {
            Ok(Self::DisplayTo)
        } else {
            Err(format!("Invalid sort criteria {:?}", String::from_utf8_lossy(value)).into())
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        parser::receiver::Receiver,
        protocol::{
            search::Filter,
            sort::{self, Comparator, Sort},
        },
    };

    #[test]
    fn parse_sort() {
        let mut receiver = Receiver::new();

        for (command, arguments) in [
            (
                b"A282 SORT (SUBJECT) UTF-8 SINCE 1-Feb-1994\r\n".to_vec(),
                sort::Arguments {
                    sort: vec![Comparator {
                        sort: Sort::Subject,
                        ascending: true,
                    }],
                    filter: Filter::Since(760060800),
                },
            ),
            (
                b"A283 SORT (SUBJECT REVERSE DATE) UTF-8 ALL\r\n".to_vec(),
                sort::Arguments {
                    sort: vec![
                        Comparator {
                            sort: Sort::Subject,
                            ascending: true,
                        },
                        Comparator {
                            sort: Sort::Date,
                            ascending: false,
                        },
                    ],
                    filter: Filter::All,
                },
            ),
            (
                b"A284 SORT (SUBJECT) US-ASCII TEXT \"not in mailbox\"\r\n".to_vec(),
                sort::Arguments {
                    sort: vec![Comparator {
                        sort: Sort::Subject,
                        ascending: true,
                    }],
                    filter: Filter::Text("not in mailbox".to_string()),
                },
            ),
            (
                [
                    b"A284 SORT (REVERSE ARRIVAL FROM) iso-8859-6 SUBJECT ".to_vec(),
                    b"\"\xe5\xd1\xcd\xc8\xc7 \xc8\xc7\xe4\xd9\xc7\xe4\xe5\"\r\n".to_vec(),
                ]
                .concat(),
                sort::Arguments {
                    sort: vec![
                        Comparator {
                            sort: Sort::Arrival,
                            ascending: false,
                        },
                        Comparator {
                            sort: Sort::From,
                            ascending: true,
                        },
                    ],
                    filter: Filter::Subject("مرحبا بالعالم".to_string()),
                },
            ),
        ] {
            let command_str = String::from_utf8_lossy(&command).into_owned();
            receiver.parse(command);
            assert_eq!(
                super::parse_sort(receiver.next_request().unwrap().unwrap().tokens)
                    .expect(&command_str),
                arguments,
                "{}",
                command_str
            );
        }
    }
}
