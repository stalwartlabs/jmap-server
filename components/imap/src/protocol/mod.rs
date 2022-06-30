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
pub mod rename;
pub mod search;
pub mod select;
pub mod sort;
pub mod status;
pub mod store_;
pub mod subscribe;
pub mod thread;
pub mod unsubscribe;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sequence {
    Number {
        value: u64,
    },
    Range {
        start: Option<u64>,
        end: Option<u64>,
    },
    LastCommand,
}

impl Sequence {
    pub fn number(value: u64) -> Sequence {
        Sequence::Number { value }
    }

    pub fn range(start: Option<u64>, end: Option<u64>) -> Sequence {
        Sequence::Range { start, end }
    }
}
