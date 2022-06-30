use std::borrow::Cow;
use std::iter::Peekable;
use std::vec::IntoIter;

use jmap_mail::mail::schema::Keyword;
use mail_parser::decoders::charsets::map::get_charset_decoder;
use mail_parser::decoders::charsets::DecoderFnc;
use store::read::filter::LogicalOperator;

use crate::protocol::search::ResultOption;
use crate::protocol::search::{self, Filter};

use super::receiver::Token;
use super::{parse_date, parse_integer, parse_sequence_set, ImapFlag};

#[allow(clippy::while_let_on_iterator)]
pub fn parse_search(tokens: Vec<Token>) -> super::Result<search::Arguments> {
    if tokens.is_empty() {
        return Err("Missing search criteria.".into());
    }

    let mut tokens = tokens.into_iter().peekable();
    let mut result_options = Vec::new();
    let mut decoder = None;

    loop {
        match tokens.peek() {
            Some(Token::Argument(value)) if value.eq_ignore_ascii_case(b"return") => {
                tokens.next();
                if tokens
                    .next()
                    .map_or(true, |token| !token.is_parenthesis_open())
                {
                    return Err("Invalid result option, expected parenthesis.".into());
                }
                while let Some(token) = tokens.next() {
                    match token {
                        Token::ParenthesisClose => break,
                        Token::Argument(value) => {
                            result_options.push(ResultOption::parse(&value)?);
                        }
                        _ => return Err("Invalid result option argument.".into()),
                    }
                }
            }
            Some(Token::Argument(value)) if value.eq_ignore_ascii_case(b"charset") => {
                tokens.next();
                decoder = get_charset_decoder(
                    &tokens
                        .next()
                        .ok_or_else(|| Cow::from("Missing charset."))?
                        .unwrap_bytes(),
                );
            }
            _ => break,
        }
    }

    let mut filters = parse_filters(&mut tokens, decoder)?;
    match filters.len() {
        0 => Err(Cow::from("No filters found in command.")),
        1 => Ok(search::Arguments {
            result_options,
            filter: filters.pop().unwrap(),
        }),
        _ => Ok(search::Arguments {
            result_options,
            filter: Filter::Operator(LogicalOperator::And, filters),
        }),
    }
}

pub fn parse_filters(
    tokens: &mut Peekable<IntoIter<Token>>,
    decoder: Option<DecoderFnc>,
) -> super::Result<Vec<Filter>> {
    let mut filters = Vec::new();
    let mut operator = LogicalOperator::And;
    let mut filters_stack = Vec::new();

    while let Some(token) = tokens.next() {
        let mut found_parenthesis = false;
        match token {
            Token::Argument(value) => {
                if value.eq_ignore_ascii_case(b"ALL") {
                    filters.push(Filter::All);
                } else if value.eq_ignore_ascii_case(b"ANSWERED") {
                    filters.push(Filter::Answered);
                } else if value.eq_ignore_ascii_case(b"BCC") {
                    filters.push(Filter::Bcc(decode_argument(tokens, decoder)?));
                } else if value.eq_ignore_ascii_case(b"BEFORE") {
                    filters.push(Filter::All);
                } else if value.eq_ignore_ascii_case(b"BODY") {
                    filters.push(Filter::Body(decode_argument(tokens, decoder)?));
                } else if value.eq_ignore_ascii_case(b"CC") {
                    filters.push(Filter::Cc(decode_argument(tokens, decoder)?));
                } else if value.eq_ignore_ascii_case(b"DELETED") {
                    filters.push(Filter::Deleted);
                } else if value.eq_ignore_ascii_case(b"DRAFT") {
                    filters.push(Filter::Draft);
                } else if value.eq_ignore_ascii_case(b"FLAGGED") {
                    filters.push(Filter::Flagged);
                } else if value.eq_ignore_ascii_case(b"FROM") {
                    filters.push(Filter::From(decode_argument(tokens, decoder)?));
                } else if value.eq_ignore_ascii_case(b"HEADER") {
                    filters.push(Filter::Header(
                        decode_argument(tokens, decoder)?,
                        decode_argument(tokens, decoder)?,
                    ));
                } else if value.eq_ignore_ascii_case(b"KEYWORD") {
                    filters.push(Filter::Keyword(Keyword::parse_imap(
                        tokens
                            .next()
                            .ok_or_else(|| Cow::from("Expected keyword"))?
                            .unwrap_bytes(),
                    )?));
                } else if value.eq_ignore_ascii_case(b"LARGER") {
                    filters.push(Filter::Larger(parse_integer(
                        &tokens
                            .next()
                            .ok_or_else(|| Cow::from("Expected integer"))?
                            .unwrap_bytes(),
                    )?));
                } else if value.eq_ignore_ascii_case(b"ON") {
                    filters.push(Filter::On(parse_date(
                        &tokens
                            .next()
                            .ok_or_else(|| Cow::from("Expected date"))?
                            .unwrap_bytes(),
                    )?));
                } else if value.eq_ignore_ascii_case(b"SEEN") {
                    filters.push(Filter::Seen);
                } else if value.eq_ignore_ascii_case(b"SENTBEFORE") {
                    filters.push(Filter::SentBefore(parse_date(
                        &tokens
                            .next()
                            .ok_or_else(|| Cow::from("Expected date"))?
                            .unwrap_bytes(),
                    )?));
                } else if value.eq_ignore_ascii_case(b"SENTON") {
                    filters.push(Filter::SentOn(parse_date(
                        &tokens
                            .next()
                            .ok_or_else(|| Cow::from("Expected date"))?
                            .unwrap_bytes(),
                    )?));
                } else if value.eq_ignore_ascii_case(b"SENTSINCE") {
                    filters.push(Filter::SentSince(parse_date(
                        &tokens
                            .next()
                            .ok_or_else(|| Cow::from("Expected date"))?
                            .unwrap_bytes(),
                    )?));
                } else if value.eq_ignore_ascii_case(b"SINCE") {
                    filters.push(Filter::Since(parse_date(
                        &tokens
                            .next()
                            .ok_or_else(|| Cow::from("Expected date"))?
                            .unwrap_bytes(),
                    )?));
                } else if value.eq_ignore_ascii_case(b"SMALLER") {
                    filters.push(Filter::Smaller(parse_integer(
                        &tokens
                            .next()
                            .ok_or_else(|| Cow::from("Expected integer"))?
                            .unwrap_bytes(),
                    )?));
                } else if value.eq_ignore_ascii_case(b"SUBJECT") {
                    filters.push(Filter::Subject(decode_argument(tokens, decoder)?));
                } else if value.eq_ignore_ascii_case(b"TEXT") {
                    filters.push(Filter::Text(decode_argument(tokens, decoder)?));
                } else if value.eq_ignore_ascii_case(b"TO") {
                    filters.push(Filter::To(decode_argument(tokens, decoder)?));
                } else if value.eq_ignore_ascii_case(b"UID") {
                    filters.push(Filter::Uid(parse_sequence_set(
                        &tokens
                            .next()
                            .ok_or_else(|| Cow::from("Missing sequence set."))?
                            .unwrap_bytes(),
                    )?));
                } else if value.eq_ignore_ascii_case(b"UNANSWERED") {
                    filters.push(Filter::Unanswered);
                } else if value.eq_ignore_ascii_case(b"UNDELETED") {
                    filters.push(Filter::Undeleted);
                } else if value.eq_ignore_ascii_case(b"UNDRAFT") {
                    filters.push(Filter::Undraft);
                } else if value.eq_ignore_ascii_case(b"UNFLAGGED") {
                    filters.push(Filter::Unflagged);
                } else if value.eq_ignore_ascii_case(b"UNKEYWORD") {
                    filters.push(Filter::Unkeyword(Keyword::parse_imap(
                        tokens
                            .next()
                            .ok_or_else(|| Cow::from("Expected keyword"))?
                            .unwrap_bytes(),
                    )?));
                } else if value.eq_ignore_ascii_case(b"UNSEEN") {
                    filters.push(Filter::Unseen);
                } else if value.eq_ignore_ascii_case(b"OLDER") {
                    filters.push(Filter::Older(parse_integer(
                        &tokens
                            .next()
                            .ok_or_else(|| Cow::from("Expected integer"))?
                            .unwrap_bytes(),
                    )?));
                } else if value.eq_ignore_ascii_case(b"YOUNGER") {
                    filters.push(Filter::Younger(parse_integer(
                        &tokens
                            .next()
                            .ok_or_else(|| Cow::from("Expected integer"))?
                            .unwrap_bytes(),
                    )?));
                } else if value.eq_ignore_ascii_case(b"OR") {
                    if filters_stack.len() > 10 {
                        return Err(Cow::from("Too many nested filters"));
                    }

                    filters_stack.push((filters, operator));
                    filters = Vec::with_capacity(2);
                    operator = LogicalOperator::Or;
                    continue;
                } else if value.eq_ignore_ascii_case(b"NOT") {
                    if filters_stack.len() > 10 {
                        return Err(Cow::from("Too many nested filters"));
                    }

                    filters_stack.push((filters, operator));
                    filters = Vec::with_capacity(1);
                    operator = LogicalOperator::Not;
                    continue;
                } else {
                    filters.push(Filter::SequenceSet(parse_sequence_set(&value)?));
                }
            }
            Token::ParenthesisOpen => {
                if filters_stack.len() > 10 {
                    return Err(Cow::from("Too many nested filters"));
                }

                filters_stack.push((filters, operator));
                filters = Vec::with_capacity(5);
                operator = LogicalOperator::And;
                continue;
            }
            Token::ParenthesisClose => {
                if filters_stack.is_empty() {
                    return Err(Cow::from("Unexpected parenthesis."));
                }

                found_parenthesis = true;
            }
            token => return Err(format!("Unexpected token {:?}.", token.to_string()).into()),
        }

        if !filters_stack.is_empty()
            && (found_parenthesis
                || (operator == LogicalOperator::Or && filters.len() == 2)
                || (operator == LogicalOperator::Not && filters.len() == 1))
        {
            while let Some((mut prev_filters, prev_operator)) = filters_stack.pop() {
                if operator == LogicalOperator::And
                    && (prev_operator != LogicalOperator::Or || filters.len() == 1)
                {
                    prev_filters.extend(filters);
                } else {
                    prev_filters.push(Filter::Operator(operator, filters));
                }
                operator = prev_operator;
                filters = prev_filters;

                if operator == LogicalOperator::And
                    || (operator == LogicalOperator::Or && filters.len() < 2)
                {
                    break;
                }
            }
        }
    }
    Ok(filters)
}

pub fn decode_argument(
    tokens: &mut Peekable<IntoIter<Token>>,
    decoder: Option<DecoderFnc>,
) -> super::Result<String> {
    let argument = tokens
        .next()
        .ok_or_else(|| Cow::from("Expected string."))?
        .unwrap_bytes();

    if let Some(decoder) = decoder {
        Ok(decoder(&argument))
    } else {
        Ok(String::from_utf8(argument.to_vec())
            .map_err(|_| Cow::from("Invalid UTF-8 argument."))?)
    }
}

impl ResultOption {
    pub fn parse(value: &[u8]) -> super::Result<Self> {
        if value.eq_ignore_ascii_case(b"min") {
            Ok(Self::Min)
        } else if value.eq_ignore_ascii_case(b"max") {
            Ok(Self::Max)
        } else if value.eq_ignore_ascii_case(b"all") {
            Ok(Self::All)
        } else if value.eq_ignore_ascii_case(b"count") {
            Ok(Self::Count)
        } else if value.eq_ignore_ascii_case(b"save") {
            Ok(Self::Save)
        } else {
            Err(format!("Invalid result option {:?}", String::from_utf8_lossy(value)).into())
        }
    }
}

#[cfg(test)]
mod tests {
    use jmap_mail::mail::schema::Keyword;
    use store::core::tag::Tag;

    use crate::{
        parser::receiver::Receiver,
        protocol::{
            search::{self, Filter, ResultOption},
            Sequence,
        },
    };

    #[test]
    fn parse_search() {
        let mut receiver = Receiver::new();

        for (command, arguments) in [
            (
                b"A282 SEARCH RETURN (MIN COUNT) FLAGGED SINCE 1-Feb-1994 NOT FROM \"Smith\"\r\n"
                    .to_vec(),
                search::Arguments {
                    result_options: vec![ResultOption::Min, ResultOption::Count],
                    filter: Filter::and([
                        Filter::Flagged,
                        Filter::Since(760060800),
                        Filter::not([Filter::From("Smith".to_string())]),
                    ]),
                },
            ),
            (
                b"A283 SEARCH RETURN () FLAGGED SINCE 1-Feb-1994 NOT FROM \"Smith\"\r\n".to_vec(),
                search::Arguments {
                    result_options: vec![],
                    filter: Filter::and([
                        Filter::Flagged,
                        Filter::Since(760060800),
                        Filter::not([Filter::From("Smith".to_string())]),
                    ]),
                },
            ),
            (
                b"A301 SEARCH $ SMALLER 4096\r\n".to_vec(),
                search::Arguments {
                    result_options: vec![],
                    filter: Filter::and([Filter::seq_last_command(), Filter::Smaller(4096)]),
                },
            ),
            (
                "P283 SEARCH CHARSET UTF-8 (OR $ 1,3000:3021) TEXT {8+}\r\nмать\r\n"
                    .as_bytes()
                    .to_vec(),
                search::Arguments {
                    result_options: vec![],
                    filter: Filter::and([
                        Filter::or([
                            Filter::seq_last_command(),
                            Filter::SequenceSet(vec![
                                Sequence::number(1),
                                Sequence::range(3000.into(), 3021.into()),
                            ]),
                        ]),
                        Filter::Text("мать".to_string()),
                    ]),
                },
            ),
            (
                b"F282 SEARCH RETURN (SAVE) KEYWORD $Junk\r\n".to_vec(),
                search::Arguments {
                    result_options: vec![ResultOption::Save],
                    filter: Filter::Keyword(Keyword {
                        tag: Tag::Static(Keyword::JUNK),
                    }),
                },
            ),
            (
                [
                    b"F282 SEARCH OR OR FROM hello@world.com TO ".to_vec(),
                    b"test@example.com OR BCC jane@foobar.com ".to_vec(),
                    b"CC john@doe.com\r\n".to_vec(),
                ]
                .concat(),
                search::Arguments {
                    result_options: vec![],
                    filter: Filter::or([
                        Filter::or([
                            Filter::From("hello@world.com".to_string()),
                            Filter::To("test@example.com".to_string()),
                        ]),
                        Filter::or([
                            Filter::Bcc("jane@foobar.com".to_string()),
                            Filter::Cc("john@doe.com".to_string()),
                        ]),
                    ]),
                },
            ),
            (
                [
                    b"abc SEARCH OR SMALLER 10000 OR ".to_vec(),
                    b"HEADER Subject \"ravioli festival\" ".to_vec(),
                    b"HEADER From \"dr. ravioli\"\r\n".to_vec(),
                ]
                .concat(),
                search::Arguments {
                    result_options: vec![],
                    filter: Filter::or([
                        Filter::Smaller(10000),
                        Filter::or([
                            Filter::Header("Subject".to_string(), "ravioli festival".to_string()),
                            Filter::Header("From".to_string(), "dr. ravioli".to_string()),
                        ]),
                    ]),
                },
            ),
            (
                [
                    b"abc SEARCH (DELETED SEEN ANSWERED) ".to_vec(),
                    b"NOT (FROM john TO jane BCC bill) ".to_vec(),
                    b"(1,30:* UID 1,2,3,4 $)\r\n".to_vec(),
                ]
                .concat(),
                search::Arguments {
                    result_options: vec![],
                    filter: Filter::and([
                        Filter::Deleted,
                        Filter::Seen,
                        Filter::Answered,
                        Filter::not([
                            Filter::From("john".to_string()),
                            Filter::To("jane".to_string()),
                            Filter::Bcc("bill".to_string()),
                        ]),
                        Filter::SequenceSet(vec![
                            Sequence::number(1),
                            Sequence::range(30.into(), None),
                        ]),
                        Filter::Uid(vec![
                            Sequence::number(1),
                            Sequence::number(2),
                            Sequence::number(3),
                            Sequence::number(4),
                        ]),
                        Filter::seq_last_command(),
                    ]),
                },
            ),
            (
                [
                    b"abc SEARCH *:* UID *:100,100:* ".to_vec(),
                    b"(FLAGGED (DRAFT (DELETED (ANSWERED)))) ".to_vec(),
                    b"OR (SENTON 20-Nov-2022) (LARGER 8196)\r\n".to_vec(),
                ]
                .concat(),
                search::Arguments {
                    result_options: vec![],
                    filter: Filter::and([
                        Filter::seq_range(None, None),
                        Filter::Uid(vec![
                            Sequence::range(None, 100.into()),
                            Sequence::range(100.into(), None),
                        ]),
                        Filter::Flagged,
                        Filter::Draft,
                        Filter::Deleted,
                        Filter::Answered,
                        Filter::or([Filter::SentOn(1668902400), Filter::Larger(8196)]),
                    ]),
                },
            ),
            (
                [
                    b"abc SEARCH NOT (FROM john OR TO jane CC bill) ".to_vec(),
                    b"OR (UNDELETED ALL) ($ NOT FLAGGED) ".to_vec(),
                    b"(((KEYWORD \"tps report\")))\r\n".to_vec(),
                ]
                .concat(),
                search::Arguments {
                    result_options: vec![],
                    filter: Filter::and([
                        Filter::not([
                            Filter::From("john".to_string()),
                            Filter::or([
                                Filter::To("jane".to_string()),
                                Filter::Cc("bill".to_string()),
                            ]),
                        ]),
                        Filter::or([
                            Filter::and([Filter::Undeleted, Filter::All]),
                            Filter::and([
                                Filter::seq_last_command(),
                                Filter::not([Filter::Flagged]),
                            ]),
                        ]),
                        Filter::Keyword(Keyword {
                            tag: Tag::Text("tps report".to_string()),
                        }),
                    ]),
                },
            ),
            (
                [
                    b"B283 SEARCH RETURN (SAVE MIN MAX) CHARSET KOI8-R TEXT ".to_vec(),
                    b"{11+}\r\n\xf0\xd2\xc9\xd7\xc5\xd4, \xcd\xc9\xd2\r\n".to_vec(),
                ]
                .concat(),
                search::Arguments {
                    result_options: vec![ResultOption::Save, ResultOption::Min, ResultOption::Max],
                    filter: Filter::Text("Привет, мир".to_string()),
                },
            ),
            (
                b"B283 SEARCH CHARSET BIG5 FROM \"\xa7A\xa6n\xa1A\xa5@\xac\xc9\"\r\n".to_vec(),
                search::Arguments {
                    result_options: vec![],
                    filter: Filter::From("你好，世界".to_string()),
                },
            ),
        ] {
            let command_str = String::from_utf8_lossy(&command).into_owned();
            receiver.parse(command);
            assert_eq!(
                super::parse_search(receiver.next_request().unwrap().unwrap().tokens)
                    .expect(&command_str),
                arguments,
                "{}",
                command_str
            );
        }
    }
}
