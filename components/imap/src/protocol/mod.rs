use jmap::from_timestamp;

use crate::{ResponseCode, ResponseType, StatusResponse};

pub mod append;
pub mod authenticate;
pub mod capability;
pub mod copy;
pub mod create;
pub mod delete;
pub mod enable;
pub mod examine;
pub mod expunge;
pub mod fetch;
pub mod list;
pub mod login;
pub mod lsub;
pub mod move_;
pub mod namespace;
pub mod rename;
pub mod search;
pub mod select;
pub mod sort;
pub mod status;
pub mod store_;
pub mod subscribe;
pub mod thread;
pub mod unsubscribe;
pub mod utf7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolVersion {
    Rev1,
    Rev2,
}

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

pub trait ImapResponse {
    fn serialize(&self, tag: String, version: ProtocolVersion) -> Vec<u8>;
}

pub fn quoted_string(buf: &mut Vec<u8>, text: &str) {
    buf.push(b'"');
    for &c in text.as_bytes() {
        if c == b'\\' || c == b'"' {
            buf.push(b'\\');
        }
        buf.push(c);
    }
    buf.push(b'"');
}

pub fn quoted_string_or_nil(buf: &mut Vec<u8>, text: Option<&str>) {
    if let Some(text) = text {
        quoted_string(buf, text);
    } else {
        buf.extend_from_slice(b"NIL");
    }
}

pub fn literal_string(buf: &mut Vec<u8>, text: &str) {
    buf.push(b'{');
    buf.extend_from_slice(text.len().to_string().as_bytes());
    buf.extend_from_slice(b"}\r\n");
    buf.extend_from_slice(text.as_bytes());
}

pub fn quoted_timestamp(buf: &mut Vec<u8>, timestamp: i64) {
    buf.push(b'"');
    buf.extend_from_slice(from_timestamp(timestamp).to_rfc2822().as_bytes());
    buf.push(b'"');
}

pub fn quoted_timestamp_or_nil(buf: &mut Vec<u8>, timestamp: Option<i64>) {
    if let Some(timestamp) = timestamp {
        quoted_timestamp(buf, timestamp);
    } else {
        buf.extend_from_slice(b"NIL");
    }
}

impl ResponseCode {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(match self {
            ResponseCode::Alert => b"ALERT",
            ResponseCode::AlreadyExists => b"ALREADYEXISTS",
            ResponseCode::AppendUid => b"APPENDUID",
            ResponseCode::AuthenticationFailed => b"AUTHENTICATIONFAILED",
            ResponseCode::AuthorizationFailed => b"AUTHORIZATIONFAILED",
            ResponseCode::BadCharset => b"BADCHARSET",
            ResponseCode::Cannot => b"CANNOT",
            ResponseCode::Capability => b"CAPABILITY",
            ResponseCode::ClientBug => b"CLIENTBUG",
            ResponseCode::Closed => b"CLOSED",
            ResponseCode::ContactAdmin => b"CONTACTADMIN",
            ResponseCode::CopyUid => b"COPYUID",
            ResponseCode::Corruption => b"CORRUPTION",
            ResponseCode::Expired => b"EXPIRED",
            ResponseCode::ExpungeIssued => b"EXPUNGEISSUED",
            ResponseCode::HasChildren => b"HASCHILDREN",
            ResponseCode::InUse => b"INUSE",
            ResponseCode::Limit => b"LIMIT",
            ResponseCode::Nonexistent => b"NONEXISTENT",
            ResponseCode::NoPerm => b"NOPERM",
            ResponseCode::OverQuota => b"OVERQUOTA",
            ResponseCode::Parse => b"PARSE",
            ResponseCode::PermanentFlags => b"PERMANENTFLAGS",
            ResponseCode::PrivacyRequired => b"PRIVACYREQUIRED",
            ResponseCode::ReadOnly => b"READ-ONLY",
            ResponseCode::ReadWrite => b"READ-WRITE",
            ResponseCode::ServerBug => b"SERVERBUG",
            ResponseCode::TryCreate => b"TRYCREATE",
            ResponseCode::UidNext => b"UIDNEXT",
            ResponseCode::UidNotSticky => b"UIDNOTSTICKY",
            ResponseCode::UidValidity => b"UIDVALIDITY",
            ResponseCode::Unavailable => b"UNAVAILABLE",
            ResponseCode::UnknownCte => b"UNKNOWN-CTE",
        });
    }
}

impl ResponseType {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(match self {
            ResponseType::Ok => b"OK",
            ResponseType::No => b"NO",
            ResponseType::Bad => b"BAD",
            ResponseType::PreAuth => b"PREAUTH",
            ResponseType::Bye => b"BYE",
        });
    }
}

impl StatusResponse {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        if let Some(tag) = &self.tag {
            buf.extend_from_slice(tag.as_bytes());
        } else {
            buf.push(b'*');
        }
        buf.push(b' ');
        self.rtype.serialize(buf);
        buf.push(b' ');
        if let Some(code) = &self.code {
            buf.push(b'[');
            code.serialize(buf);
            buf.extend_from_slice(b"] ");
        }
        buf.extend_from_slice(self.message.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
}
