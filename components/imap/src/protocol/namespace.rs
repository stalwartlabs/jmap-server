use super::{quoted_string, ImapResponse};

pub struct Response {
    pub shared_prefix: Option<String>,
}

impl ImapResponse for Response {
    fn serialize(&self, tag: &str, _version: super::ProtocolVersion) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        if let Some(shared_prefix) = &self.shared_prefix {
            buf.extend_from_slice(b"* NAMESPACE ((\"\" \"/\")) ((");
            quoted_string(&mut buf, shared_prefix);
            buf.extend_from_slice(b" \"/\")) NIL\r\n");
        } else {
            buf.extend_from_slice(b"* NAMESPACE ((\"\" \"/\")) NIL NIL\r\n");
        }

        buf.extend_from_slice(tag.as_bytes());
        buf.extend_from_slice(b" OK completed\r\n");
        buf
    }
}
