use jmap_mail::mail::schema::Keyword;
use store::read::filter::LogicalOperator;

use super::Sequence;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arguments {
    pub result_options: Vec<ResultOption>,
    pub filter: Filter,
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
