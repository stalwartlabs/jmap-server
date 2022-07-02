use crate::StatusResponse;

use super::{ImapResponse, ProtocolVersion};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub capabilities: Vec<Capability>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Capability {
    IMAP4rev2,
    StartTLS,
    LoginDisabled,
    Condstore,
}

impl Capability {
    pub fn to_buf(&self) -> &'static [u8] {
        match self {
            Capability::IMAP4rev2 => b"IMAP4rev2",
            Capability::StartTLS => b"STARTTLS",
            Capability::LoginDisabled => b"LOGINDISABLED",
            Capability::Condstore => b"CONDSTORE",
        }
    }
}

impl ImapResponse for Response {
    fn serialize(&self, tag: String, _imap_rev: ProtocolVersion) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            b"* CAPABILITY  \r\n".len()
                + (self.capabilities.len() * 10)
                + b" OK CAPABILITY completed\r\n".len()
                + tag.len(),
        );
        buf.extend_from_slice(b"* CAPABILITY");
        for capability in self.capabilities.iter() {
            buf.push(b' ');
            buf.extend_from_slice(capability.to_buf());
        }
        buf.extend_from_slice(b"\r\n");
        StatusResponse::ok(tag.into(), None, "completed").serialize(&mut buf);
        buf
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::{
        capability::{Capability, Response},
        ImapResponse, ProtocolVersion,
    };

    #[test]
    fn serialize_capability() {
        assert_eq!(
            &Response {
                capabilities: vec![
                    Capability::IMAP4rev2,
                    Capability::StartTLS,
                    Capability::LoginDisabled
                ],
            }
            .serialize("a003".to_string(), ProtocolVersion::Rev2),
            concat!(
                "* CAPABILITY IMAP4rev2 STARTTLS LOGINDISABLED\r\n",
                "a003 OK completed\r\n"
            )
            .as_bytes()
        );
    }
}
