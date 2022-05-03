use std::fmt::Display;

use jmap::{request::JSONArgumentParser, Property};
use store::core::collection::Collection;

pub mod changes;
pub mod get;

pub struct ThreadProperty {}

impl Property for ThreadProperty {
    fn parse(_value: &str) -> Option<Self> {
        None
    }

    fn collection() -> store::core::collection::Collection {
        Collection::Thread
    }
}

impl Display for ThreadProperty {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl JSONArgumentParser for ThreadProperty {
    fn parse_argument(_argument: jmap::protocol::json::JSONValue) -> jmap::Result<Self> {
        Ok(ThreadProperty {})
    }
}
