pub mod authenticate;
pub mod create;
pub mod delete;
pub mod enable;
pub mod examine;
pub mod list;
pub mod login;
pub mod receiver;
pub mod rename;
pub mod select;
pub mod status;
pub mod subscribe;
pub mod unsubscribe;

use std::{borrow::Cow, fmt::Display};

use crate::Command;

pub type Result<T> = std::result::Result<T, Cow<'static, str>>;

impl Command {
    pub fn parse(value: &[u8]) -> Option<Self> {
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
            b"EXPUNGE" => Some(Command::Expunge),
            b"SEARCH" => Some(Command::Search),
            b"FETCH" => Some(Command::Fetch),
            b"STORE" => Some(Command::Store),
            b"COPY" => Some(Command::Copy),
            b"MOVE" => Some(Command::Move),
            b"UID" => Some(Command::Uid),
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
            Command::Expunge => write!(f, "EXPUNGE"),
            Command::Search => write!(f, "SEARCH"),
            Command::Fetch => write!(f, "FETCH"),
            Command::Store => write!(f, "STORE"),
            Command::Copy => write!(f, "COPY"),
            Command::Move => write!(f, "MOVE"),
            Command::Uid => write!(f, "UID"),
        }
    }
}
