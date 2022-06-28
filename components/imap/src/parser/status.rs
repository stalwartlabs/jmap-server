use crate::protocol::status::Status;

impl Status {
    pub fn parse(value: &[u8]) -> super::Result<Self> {
        if value.eq_ignore_ascii_case(b"messages") {
            Ok(Self::Messages)
        } else if value.eq_ignore_ascii_case(b"uidnext") {
            Ok(Self::UidNext)
        } else if value.eq_ignore_ascii_case(b"uidvalidity") {
            Ok(Self::UidValidity)
        } else if value.eq_ignore_ascii_case(b"unseen") {
            Ok(Self::Unseen)
        } else if value.eq_ignore_ascii_case(b"deleted") {
            Ok(Self::Deleted)
        } else if value.eq_ignore_ascii_case(b"size") {
            Ok(Self::Size)
        } else {
            Err(format!(
                "Invalid status option '{}'.",
                String::from_utf8_lossy(value)
            )
            .into())
        }
    }
}
