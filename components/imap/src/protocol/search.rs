use jmap_mail::mail::schema::Keyword;
use store::read::filter::LogicalOperator;

use super::{quoted_string, ImapResponse, ProtocolVersion, Sequence};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arguments {
    pub result_options: Vec<ResultOption>,
    pub filter: Filter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub ids: Vec<u64>,
    pub min: Option<u64>,
    pub max: Option<u64>,
    pub count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultOption {
    Min,
    Max,
    All,
    Count,
    Save,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filter {
    SequenceSet(Vec<Sequence>),
    All,
    Answered,
    Bcc(String),
    Before(i64),
    Body(String),
    Cc(String),
    Deleted,
    Draft,
    Flagged,
    From(String),
    Header(String, String),
    Keyword(Keyword),
    Larger(u64),
    On(i64),
    Seen,
    SentBefore(i64),
    SentOn(i64),
    SentSince(i64),
    Since(i64),
    Smaller(u64),
    Subject(String),
    Text(String),
    To(String),
    Uid(Vec<Sequence>),
    Unanswered,
    Undeleted,
    Undraft,
    Unflagged,
    Unkeyword(Keyword),
    Unseen,
    Operator(LogicalOperator, Vec<Filter>),

    // RFC5032
    Older(u64),
    Younger(u64),
}

impl Filter {
    pub fn and(filters: impl IntoIterator<Item = Filter>) -> Filter {
        Filter::Operator(LogicalOperator::And, filters.into_iter().collect())
    }
    pub fn or(filters: impl IntoIterator<Item = Filter>) -> Filter {
        Filter::Operator(LogicalOperator::Or, filters.into_iter().collect())
    }
    pub fn not(filters: impl IntoIterator<Item = Filter>) -> Filter {
        Filter::Operator(LogicalOperator::Not, filters.into_iter().collect())
    }

    pub fn seq_last_command() -> Filter {
        Filter::SequenceSet(vec![Sequence::LastCommand])
    }

    pub fn seq_range(start: Option<u64>, end: Option<u64>) -> Filter {
        Filter::SequenceSet(vec![Sequence::Range { start, end }])
    }
}

impl ImapResponse for Response {
    fn serialize(&self, tag: &str, version: ProtocolVersion) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        if version == ProtocolVersion::Rev2 {
            buf.extend_from_slice(b"* ESEARCH (TAG ");
            quoted_string(&mut buf, tag);
            buf.extend_from_slice(b")");
            if let Some(count) = &self.count {
                buf.extend_from_slice(b" COUNT ");
                buf.extend_from_slice(count.to_string().as_bytes());
            }
            if let Some(min) = &self.min {
                buf.extend_from_slice(b" MIN ");
                buf.extend_from_slice(min.to_string().as_bytes());
            }
            if let Some(max) = &self.max {
                buf.extend_from_slice(b" MAX ");
                buf.extend_from_slice(max.to_string().as_bytes());
            }
            if !self.ids.is_empty() {
                buf.extend_from_slice(b" ALL ");
                let mut ids = self.ids.iter().peekable();
                while let Some(&id) = ids.next() {
                    buf.extend_from_slice(id.to_string().as_bytes());
                    let mut range_id = id;
                    loop {
                        match ids.peek() {
                            Some(&&next_id) if next_id == range_id + 1 => {
                                range_id += 1;
                                ids.next();
                            }
                            next => {
                                if range_id != id {
                                    buf.push(b':');
                                    buf.extend_from_slice(range_id.to_string().as_bytes());
                                }
                                if next.is_some() {
                                    buf.push(b',');
                                }
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            buf.extend_from_slice(b"* SEARCH");
            if !self.ids.is_empty() {
                for id in &self.ids {
                    buf.push(b' ');
                    buf.extend_from_slice(id.to_string().as_bytes());
                }
            }
        }
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(tag.as_bytes());
        buf.extend_from_slice(b" OK completed\r\n");
        buf
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::{ImapResponse, ProtocolVersion};

    #[test]
    fn serialize_search() {
        for (response, tag, expected_v2, expected_v1) in [
            (
                super::Response {
                    ids: vec![2, 10, 11],
                    min: 2.into(),
                    max: 11.into(),
                    count: 3.into(),
                },
                "A283",
                concat!(
                    "* ESEARCH (TAG \"A283\") COUNT 3 MIN 2 MAX 11 ALL 2,10:11\r\n",
                    "A283 OK completed\r\n"
                ),
                concat!("* SEARCH 2 10 11\r\n", "A283 OK completed\r\n"),
            ),
            (
                super::Response {
                    ids: vec![
                        1, 2, 3, 5, 10, 11, 12, 13, 90, 92, 93, 94, 95, 96, 97, 98, 99,
                    ],
                    min: None,
                    max: None,
                    count: None,
                },
                "A283",
                concat!(
                    "* ESEARCH (TAG \"A283\") ALL 1:3,5,10:13,90,92:99\r\n",
                    "A283 OK completed\r\n"
                ),
                concat!(
                    "* SEARCH 1 2 3 5 10 11 12 13 90 92 93 94 95 96 97 98 99\r\n",
                    "A283 OK completed\r\n"
                ),
            ),
            (
                super::Response {
                    ids: vec![],
                    min: None,
                    max: None,
                    count: None,
                },
                "A283",
                concat!("* ESEARCH (TAG \"A283\")\r\n", "A283 OK completed\r\n"),
                concat!("* SEARCH\r\n", "A283 OK completed\r\n"),
            ),
        ] {
            let response_v1 =
                String::from_utf8(response.serialize(tag, ProtocolVersion::Rev1)).unwrap();
            let response_v2 =
                String::from_utf8(response.serialize(tag, ProtocolVersion::Rev2)).unwrap();

            assert_eq!(response_v2, expected_v2);
            assert_eq!(response_v1, expected_v1);
        }
    }
}
