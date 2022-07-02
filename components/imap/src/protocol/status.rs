use crate::StatusResponse;

use super::{quoted_string, utf7::utf7_encode, ImapResponse, ProtocolVersion};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arguments {
    pub name: String,
    pub items: Vec<Status>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: StatusItem,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Messages,
    UidNext,
    UidValidity,
    Unseen,
    Deleted,
    Size,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusItem {
    pub mailbox_name: String,
    pub items: Vec<(Status, u64)>,
}

impl StatusItem {
    pub fn serialize(&self, buf: &mut Vec<u8>, version: ProtocolVersion) {
        buf.extend_from_slice(b"* STATUS ");
        if version == ProtocolVersion::Rev2 {
            quoted_string(buf, &self.mailbox_name);
        } else {
            quoted_string(buf, &utf7_encode(&self.mailbox_name));
        }
        buf.extend_from_slice(b" (");
        for (pos, (status_item, amount)) in self.items.iter().enumerate() {
            if pos > 0 {
                buf.push(b' ');
            }
            buf.extend_from_slice(match status_item {
                Status::Messages => b"MESSAGES ",
                Status::UidNext => b"UIDNEXT ",
                Status::UidValidity => b"UIDVALIDITY ",
                Status::Unseen => b"UNSEEN ",
                Status::Deleted => b"DELETED ",
                Status::Size => b"SIZE ",
            });
            buf.extend_from_slice(amount.to_string().as_bytes());
        }
        buf.extend_from_slice(b")\r\n");
    }
}

impl ImapResponse for Response {
    fn serialize(&self, tag: String, version: super::ProtocolVersion) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        self.status.serialize(&mut buf, version);
        StatusResponse::ok(tag.into(), None, "completed").serialize(&mut buf);
        buf
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::{
        status::{Status, StatusItem},
        ImapResponse, ProtocolVersion,
    };

    #[test]
    fn serialize_status() {
        assert_eq!(
            &super::Response {
                status: StatusItem {
                    mailbox_name: "blurdybloop".to_string(),
                    items: vec![(Status::Messages, 231), (Status::UidNext, 44292)]
                },
            }
            .serialize("A042".to_string(), ProtocolVersion::Rev2),
            concat!(
                "* STATUS \"blurdybloop\" (MESSAGES 231 UIDNEXT 44292)\r\n",
                "A042 OK completed\r\n"
            )
            .as_bytes()
        );
    }
}
