pub mod append;
pub mod authenticate;
pub mod copy;
pub mod create;
pub mod delete;
pub mod enable;
pub mod examine;
pub mod fetch;
pub mod list;
pub mod login;
pub mod lsub;
pub mod move_;
pub mod receiver;
pub mod rename;
pub mod search;
pub mod select;
pub mod sort;
pub mod status;
pub mod store_;
pub mod subscribe;
pub mod thread;
pub mod unsubscribe;

use jmap_mail::mail::schema::Keyword;
use store::{
    chrono::{DateTime, NaiveDate},
    core::tag::Tag,
};

use std::{borrow::Cow, fmt::Display};

use crate::{protocol::Sequence, Command};

pub type Result<T> = std::result::Result<T, Cow<'static, str>>;

impl Command {
    pub fn parse(value: &[u8], uid: bool) -> Option<Self> {
        match value {
            b"CAPABILITY" => Some(Command::Capability),
            b"NOOP" => Some(Command::Noop),
            b"LOGOUT" => Some(Command::Logout),
            b"STARTTLS" => Some(Command::StartTls),
            b"AUTHENTICATE" => Some(Command::Authenticate),
            b"LOGIN" => Some(Command::Login),
            b"ENABLE" => Some(Command::Enable),
            b"SELECT" => Some(Command::Select),
            b"EXAMINE" => Some(Command::Examine),
            b"CREATE" => Some(Command::Create),
            b"DELETE" => Some(Command::Delete),
            b"RENAME" => Some(Command::Rename),
            b"SUBSCRIBE" => Some(Command::Subscribe),
            b"UNSUBSCRIBE" => Some(Command::Unsubscribe),
            b"LIST" => Some(Command::List),
            b"NAMESPACE" => Some(Command::Namespace),
            b"STATUS" => Some(Command::Status),
            b"APPEND" => Some(Command::Append),
            b"IDLE" => Some(Command::Idle),
            b"CLOSE" => Some(Command::Close),
            b"UNSELECT" => Some(Command::Unselect),
            b"EXPUNGE" => Some(Command::Expunge(uid)),
            b"SEARCH" => Some(Command::Search(uid)),
            b"FETCH" => Some(Command::Fetch(uid)),
            b"STORE" => Some(Command::Store(uid)),
            b"COPY" => Some(Command::Copy(uid)),
            b"MOVE" => Some(Command::Move(uid)),
            b"SORT" => Some(Command::Sort(uid)),
            b"THREAD" => Some(Command::Thread(uid)),
            b"LSUB" => Some(Command::Lsub),
            b"CHECK" => Some(Command::Check),
            _ => None,
        }
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Command::Capability => write!(f, "CAPABILITY"),
            Command::Noop => write!(f, "NOOP"),
            Command::Logout => write!(f, "LOGOUT"),
            Command::StartTls => write!(f, "STARTTLS"),
            Command::Authenticate => write!(f, "AUTHENTICATE"),
            Command::Login => write!(f, "LOGIN"),
            Command::Enable => write!(f, "ENABLE"),
            Command::Select => write!(f, "SELECT"),
            Command::Examine => write!(f, "EXAMINE"),
            Command::Create => write!(f, "CREATE"),
            Command::Delete => write!(f, "DELETE"),
            Command::Rename => write!(f, "RENAME"),
            Command::Subscribe => write!(f, "SUBSCRIBE"),
            Command::Unsubscribe => write!(f, "UNSUBSCRIBE"),
            Command::List => write!(f, "LIST"),
            Command::Namespace => write!(f, "NAMESPACE"),
            Command::Status => write!(f, "STATUS"),
            Command::Append => write!(f, "APPEND"),
            Command::Idle => write!(f, "IDLE"),
            Command::Close => write!(f, "CLOSE"),
            Command::Unselect => write!(f, "UNSELECT"),
            Command::Expunge(false) => write!(f, "EXPUNGE"),
            Command::Search(false) => write!(f, "SEARCH"),
            Command::Fetch(false) => write!(f, "FETCH"),
            Command::Store(false) => write!(f, "STORE"),
            Command::Copy(false) => write!(f, "COPY"),
            Command::Move(false) => write!(f, "MOVE"),
            Command::Sort(false) => write!(f, "SORT"),
            Command::Thread(false) => write!(f, "THREAD"),
            Command::Expunge(true) => write!(f, "UID EXPUNGE"),
            Command::Search(true) => write!(f, "UID SEARCH"),
            Command::Fetch(true) => write!(f, "UID FETCH"),
            Command::Store(true) => write!(f, "UID STORE"),
            Command::Copy(true) => write!(f, "UID COPY"),
            Command::Move(true) => write!(f, "UID MOVE"),
            Command::Sort(true) => write!(f, "UID SORT"),
            Command::Thread(true) => write!(f, "UID THREAD"),
            Command::Lsub => write!(f, "LSUB"),
            Command::Check => write!(f, "CHECK"),
        }
    }
}

pub trait ImapFlag: Sized {
    fn parse_imap(value: Vec<u8>) -> Result<Self>;
    fn to_imap(&self) -> Cow<'static, str>;
}

impl ImapFlag for Keyword {
    fn parse_imap(value: Vec<u8>) -> Result<Self> {
        Ok(Keyword {
            tag: match value
                .get(0)
                .ok_or_else(|| Cow::from("Null flags are not allowed."))?
            {
                b'\\' => {
                    if value.eq_ignore_ascii_case(b"\\Seen") {
                        Tag::Static(Self::SEEN)
                    } else if value.eq_ignore_ascii_case(b"\\Answered") {
                        Tag::Static(Self::ANSWERED)
                    } else if value.eq_ignore_ascii_case(b"\\Flagged") {
                        Tag::Static(Self::FLAGGED)
                    } else if value.eq_ignore_ascii_case(b"\\Deleted") {
                        Tag::Static(Self::DELETED)
                    } else if value.eq_ignore_ascii_case(b"\\Draft") {
                        Tag::Static(Self::DRAFT)
                    } else if value.eq_ignore_ascii_case(b"\\Recent") {
                        Tag::Static(Self::RECENT)
                    } else {
                        Tag::Text(
                            String::from_utf8(value).map_err(|_| Cow::from("Invalid UTF-8."))?,
                        )
                    }
                }
                b'$' => {
                    if value.eq_ignore_ascii_case(b"$Forwarded") {
                        Tag::Static(Self::FORWARDED)
                    } else if value.eq_ignore_ascii_case(b"$MDNSent") {
                        Tag::Static(Self::MDN_SENT)
                    } else if value.eq_ignore_ascii_case(b"$Junk") {
                        Tag::Static(Self::JUNK)
                    } else if value.eq_ignore_ascii_case(b"$NotJunk") {
                        Tag::Static(Self::NOTJUNK)
                    } else if value.eq_ignore_ascii_case(b"$Phishing") {
                        Tag::Static(Self::PHISHING)
                    } else {
                        Tag::Text(
                            String::from_utf8(value).map_err(|_| Cow::from("Invalid UTF-8."))?,
                        )
                    }
                }
                _ => Tag::Text(String::from_utf8(value).map_err(|_| Cow::from("Invalid UTF-8."))?),
            },
        })
    }

    fn to_imap(&self) -> Cow<'static, str> {
        match &self.tag {
            Tag::Static(keyword) => match *keyword {
                Self::SEEN => "\\Seen".into(),
                Self::DRAFT => "\\Draft".into(),
                Self::FLAGGED => "\\Flagged".into(),
                Self::ANSWERED => "\\Answered".into(),
                Self::RECENT => "\\Recent".into(),
                Self::IMPORTANT => "$Important".into(),
                Self::PHISHING => "$Phishing".into(),
                Self::JUNK => "$Junk".into(),
                Self::NOTJUNK => "$NotJunk".into(),
                Self::DELETED => "\\Deleted".into(),
                Self::FORWARDED => "$Forwarded".into(),
                Self::MDN_SENT => "$MDNSent".into(),
                12..=u8::MAX => "".into(),
            },
            Tag::Text(value) => {
                let mut flag = String::with_capacity(value.len());
                for c in value.chars() {
                    if c.is_ascii_alphanumeric() {
                        flag.push(c);
                    } else {
                        flag.push('_');
                    }
                }
                flag.into()
            }
            _ => "".into(),
        }
    }
}

pub fn parse_datetime(value: &[u8]) -> Result<i64> {
    let datetime = std::str::from_utf8(value)
        .map_err(|_| Cow::from("Expected date/time, found an invalid UTF-8 string."))?
        .trim();
    DateTime::parse_from_str(datetime, "%d-%b-%Y %H:%M:%S %z")
        .map_err(|_| Cow::from(format!("Failed to parse date/time '{}'.", datetime)))
        .map(|dt| dt.timestamp())
}

pub fn parse_date(value: &[u8]) -> Result<i64> {
    let date = std::str::from_utf8(value)
        .map_err(|_| Cow::from("Expected date, found an invalid UTF-8 string."))?
        .trim();
    NaiveDate::parse_from_str(date, "%d-%b-%Y")
        .map_err(|_| Cow::from(format!("Failed to parse date '{}'.", date)))
        .map(|dt| dt.and_hms(0, 0, 0).timestamp())
}

pub fn parse_integer(value: &[u8]) -> Result<u64> {
    std::str::from_utf8(value)
        .map_err(|_| Cow::from("Expected an integer, found an invalid UTF-8 string."))?
        .parse::<u64>()
        .map_err(|_| Cow::from("Failed to parse integer."))
}

pub fn parse_sequence_set(value: &[u8]) -> Result<Vec<Sequence>> {
    let mut sequence_set = Vec::new();
    let mut is_range = false;
    let mut range_start = None;
    let mut token_start = None;
    let mut has_wildcard = false;

    for (mut pos, ch) in value.iter().enumerate() {
        let mut add_token = false;
        match ch {
            b',' => {
                add_token = true;
            }
            b':' => {
                if !is_range {
                    if let Some(from_pos) = token_start {
                        range_start =
                            parse_integer(value.get(from_pos..pos).ok_or_else(|| {
                                Cow::from("Expected sequence set, parse error.")
                            })?)?
                            .into();
                        token_start = None;
                    } else if has_wildcard {
                        has_wildcard = false;
                    } else {
                        return Err(Cow::from(
                            "Invalid sequence set, number expected before ':'.",
                        ));
                    }
                    is_range = true;
                } else {
                    return Err(Cow::from(
                        "Invalid sequence set, ':' appears multiple times.",
                    ));
                }
            }
            b'*' => {
                if !has_wildcard {
                    if token_start.is_none() {
                        has_wildcard = true;
                    } else {
                        return Err(Cow::from("Invalid sequence set, invalid use of '*'."));
                    }
                } else {
                    return Err(Cow::from(
                        "Invalid sequence set, '*' appears multiple times.",
                    ));
                }
            }
            b'$' => {
                if value.len() == 1 {
                    return Ok(vec![Sequence::LastCommand]);
                } else {
                    return Err(Cow::from("Invalid sequence set, can't parse '$' marker."));
                }
            }
            _ => {
                if ch.is_ascii_digit() {
                    if has_wildcard {
                        return Err(Cow::from("Invalid sequence set, invalid use of '*'."));
                    }
                    if token_start.is_none() {
                        token_start = pos.into();
                    }
                } else {
                    return Err(Cow::from(format!(
                        "Expected sequence set, found invalid character '{}' at position {}.",
                        ch, pos
                    )));
                }
            }
        }

        if add_token || pos == value.len() - 1 {
            if is_range {
                sequence_set.push(Sequence::Range {
                    start: range_start,
                    end: if !has_wildcard {
                        if !add_token {
                            pos += 1;
                        }
                        parse_integer(
                            value
                                .get(
                                    token_start.ok_or_else(|| {
                                        Cow::from("Invalid sequence set, expected number.")
                                    })?..pos,
                                )
                                .ok_or_else(|| Cow::from("Expected sequence set, parse error."))?,
                        )?
                        .into()
                    } else {
                        has_wildcard = false;
                        None
                    },
                });
                is_range = false;
                range_start = None;
            } else {
                if !add_token {
                    pos += 1;
                }
                sequence_set.push(Sequence::Number {
                    value: parse_integer(
                        value
                            .get(
                                token_start.ok_or_else(|| {
                                    Cow::from("Invalid sequence set, expected number.")
                                })?..pos,
                            )
                            .ok_or_else(|| Cow::from("Expected sequence set, parse error."))?,
                    )?,
                });
            }
            token_start = None;
        }
    }

    if !sequence_set.is_empty() {
        Ok(sequence_set)
    } else {
        Err(Cow::from("Invalid empty sequence set."))
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::Sequence;

    #[test]
    fn parse_sequence_set() {
        for (sequence, expected_result) in [
            ("$", vec![Sequence::LastCommand]),
            (
                "1,3000:3021",
                vec![
                    Sequence::Number { value: 1 },
                    Sequence::Range {
                        start: 3000.into(),
                        end: 3021.into(),
                    },
                ],
            ),
            (
                "2,4:7,9,12:*",
                vec![
                    Sequence::Number { value: 2 },
                    Sequence::Range {
                        start: 4.into(),
                        end: 7.into(),
                    },
                    Sequence::Number { value: 9 },
                    Sequence::Range {
                        start: 12.into(),
                        end: None,
                    },
                ],
            ),
            (
                "*:4,5:7",
                vec![
                    Sequence::Range {
                        start: None,
                        end: 4.into(),
                    },
                    Sequence::Range {
                        start: 5.into(),
                        end: 7.into(),
                    },
                ],
            ),
            (
                "2,4,5",
                vec![
                    Sequence::Number { value: 2 },
                    Sequence::Number { value: 4 },
                    Sequence::Number { value: 5 },
                ],
            ),
        ] {
            assert_eq!(
                super::parse_sequence_set(sequence.as_bytes()).unwrap(),
                expected_result
            );
        }
    }
}
