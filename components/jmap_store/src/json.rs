use std::{borrow::Cow, fmt::Display};

#[derive(Debug)]
pub enum JSONPointer<'x> {
    Wildcard,
    String(Cow<'x, str>),
    Number(u64),
    Path(Vec<JSONPointer<'x>>),
}

impl<'x> JSONPointer<'x> {
    pub fn as_string(&self) -> String {
        "".to_string()
    }
}

impl<'x> Display for JSONPointer<'x> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_string())
    }
}
