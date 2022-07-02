use crate::StatusResponse;

use super::ImapResponse;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub ids: Vec<u64>,
}

impl ImapResponse for Response {
    fn serialize(&self, tag: String, _version: super::ProtocolVersion) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        for id in &self.ids {
            buf.extend_from_slice(b"* ");
            buf.extend_from_slice(id.to_string().as_bytes());
            buf.extend_from_slice(b" EXPUNGE\r\n");
        }
        StatusResponse::ok(tag.into(), None, "completed").serialize(&mut buf);
        buf
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::{ImapResponse, ProtocolVersion};

    #[test]
    fn serialize_expunge() {
        assert_eq!(
            &super::Response { ids: vec![3, 5, 8] }
                .serialize("A202".to_string(), ProtocolVersion::Rev2),
            concat!(
                "* 3 EXPUNGE\r\n",
                "* 5 EXPUNGE\r\n",
                "* 8 EXPUNGE\r\n",
                "A202 OK completed\r\n"
            )
            .as_bytes()
        );
    }
}
