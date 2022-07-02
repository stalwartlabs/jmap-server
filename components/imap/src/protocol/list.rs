use crate::StatusResponse;

use super::{
    quoted_string,
    status::{Status, StatusItem},
    utf7::utf7_encode,
    ImapResponse, ProtocolVersion,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Arguments {
    Basic {
        reference_name: String,
        mailbox_name: String,
    },
    Extended {
        reference_name: String,
        mailbox_name: Vec<String>,
        selection_options: Vec<SelectionOption>,
        return_options: Vec<ReturnOption>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub list_items: Vec<ListItem>,
    pub status_items: Vec<StatusItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectionOption {
    Subscribed,
    Remote,
    RecursiveMatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReturnOption {
    Subscribed,
    Children,
    Status(Vec<Status>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attribute {
    NoInferiors,
    NoSelect,
    Marked,
    Unmarked,
    NonExistent,
    HasChildren,
    HasNoChildren,
    Subscribed,
    Remote,
    All,
    Archive,
    Drafts,
    Flagged,
    Junk,
    Sent,
    Trash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildInfo {
    Subscribed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tag {
    ChildInfo(Vec<ChildInfo>),
    OldName(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListItem {
    pub mailbox_name: String,
    pub attributes: Vec<Attribute>,
    pub tags: Vec<Tag>,
}

impl Attribute {
    pub fn is_rev1(&self) -> bool {
        matches!(
            self,
            Attribute::NoInferiors | Attribute::NoSelect | Attribute::Marked | Attribute::Unmarked
        )
    }

    pub fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(match self {
            Attribute::NoInferiors => b"\\NoInferiors",
            Attribute::NoSelect => b"\\NoSelect",
            Attribute::Marked => b"\\Marked",
            Attribute::Unmarked => b"\\Unmarked",
            Attribute::NonExistent => b"\\NonExistent",
            Attribute::HasChildren => b"\\HasChildren",
            Attribute::HasNoChildren => b"\\HasNoChildren",
            Attribute::Subscribed => b"\\Subscribed",
            Attribute::Remote => b"\\Remote",
            Attribute::All => b"\\All",
            Attribute::Archive => b"\\Archive",
            Attribute::Drafts => b"\\Drafts",
            Attribute::Flagged => b"\\Flagged",
            Attribute::Junk => b"\\Junk",
            Attribute::Sent => b"\\Sent",
            Attribute::Trash => b"\\Trash",
        });
    }
}

impl ChildInfo {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        buf.push(b'\"');
        buf.extend_from_slice(match self {
            ChildInfo::Subscribed => b"SUBSCRIBED",
        });
        buf.push(b'\"');
    }
}

impl Tag {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Tag::ChildInfo(child_info) => {
                buf.extend_from_slice(b"\"CHILDINFO\" (");
                for (pos, child_info) in child_info.iter().enumerate() {
                    if pos > 0 {
                        buf.push(b' ');
                    }
                    child_info.serialize(buf);
                }
                buf.push(b')');
            }
            Tag::OldName(old_name) => {
                buf.extend_from_slice(b"\"OLDNAME\" (");
                quoted_string(buf, old_name);
                buf.push(b')');
            }
        }
    }
}

impl ListItem {
    pub fn new(name: impl Into<String>) -> Self {
        ListItem {
            mailbox_name: name.into(),
            attributes: Vec::new(),
            tags: Vec::new(),
        }
    }

    pub fn serialize(&self, buf: &mut Vec<u8>, version: ProtocolVersion) {
        let normalized_mailbox_name = utf7_encode(&self.mailbox_name);
        buf.extend_from_slice(b"* LIST (");
        let mut is_first = true;
        for attr in &self.attributes {
            if version == ProtocolVersion::Rev2 || attr.is_rev1() {
                if is_first {
                    is_first = false;
                } else {
                    buf.push(b' ');
                }
                attr.serialize(buf);
            }
        }
        buf.extend_from_slice(b") \"/\" ");
        let mut extra_tags = Vec::new();

        if normalized_mailbox_name != self.mailbox_name {
            if version == ProtocolVersion::Rev2 {
                quoted_string(buf, &self.mailbox_name);
                extra_tags.push(Tag::OldName(normalized_mailbox_name));
            } else {
                quoted_string(buf, &normalized_mailbox_name);
            }
        } else {
            quoted_string(buf, &self.mailbox_name);
        }

        if version == ProtocolVersion::Rev2 && (!extra_tags.is_empty() || !self.tags.is_empty()) {
            buf.extend_from_slice(b" (");
            for (pos, tag) in extra_tags.iter().chain(self.tags.iter()).enumerate() {
                if pos > 0 {
                    buf.push(b' ');
                }
                tag.serialize(buf);
            }
            buf.extend_from_slice(b")\r\n");
        } else {
            buf.extend_from_slice(b"\r\n");
        }
    }
}

impl ImapResponse for Response {
    fn serialize(&self, tag: String, version: ProtocolVersion) -> Vec<u8> {
        let mut buf = Vec::with_capacity(100);
        for list_item in &self.list_items {
            list_item.serialize(&mut buf, version);
        }
        if version == ProtocolVersion::Rev2 {
            for status_item in &self.status_items {
                status_item.serialize(&mut buf, version);
            }
        }
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

    use super::{Attribute, ChildInfo, ListItem, Tag};

    #[test]
    fn serialize_list_item() {
        for (response, expected_v2, expected_v1) in [
            (
                super::ListItem {
                    mailbox_name: "".to_string(),
                    attributes: vec![],
                    tags: vec![],
                },
                "* LIST () \"/\" \"\"\r\n",
                "* LIST () \"/\" \"\"\r\n",
            ),
            (
                super::ListItem {
                    mailbox_name: "中國書店".to_string(),
                    attributes: vec![Attribute::NoInferiors, Attribute::Drafts],
                    tags: vec![],
                },
                concat!(
                    "* LIST (\\NoInferiors \\Drafts) \"/\" \"中國書店\" ",
                    "(\"OLDNAME\" (\"&Ti1XC2b4Xpc-\"))\r\n"
                ),
                "* LIST (\\NoInferiors) \"/\" \"&Ti1XC2b4Xpc-\"\r\n",
            ),
            (
                super::ListItem {
                    mailbox_name: "☺".to_string(),
                    attributes: vec![Attribute::Subscribed, Attribute::Remote],
                    tags: vec![Tag::ChildInfo(vec![ChildInfo::Subscribed])],
                },
                concat!(
                    "* LIST (\\Subscribed \\Remote) \"/\" \"☺\" ",
                    "(\"OLDNAME\" (\"&Jjo-\") \"CHILDINFO\" (\"SUBSCRIBED\"))\r\n"
                ),
                "* LIST () \"/\" \"&Jjo-\"\r\n",
            ),
            (
                super::ListItem {
                    mailbox_name: "foo".to_string(),
                    attributes: vec![Attribute::HasNoChildren],
                    tags: vec![Tag::ChildInfo(vec![ChildInfo::Subscribed])],
                },
                "* LIST (\\HasNoChildren) \"/\" \"foo\" (\"CHILDINFO\" (\"SUBSCRIBED\"))\r\n",
                "* LIST () \"/\" \"foo\"\r\n",
            ),
        ] {
            let mut buf_1 = Vec::with_capacity(100);
            let mut buf_2 = Vec::with_capacity(100);

            response.serialize(&mut buf_1, ProtocolVersion::Rev1);
            response.serialize(&mut buf_2, ProtocolVersion::Rev2);

            let response_v1 = String::from_utf8(buf_1).unwrap();
            let response_v2 = String::from_utf8(buf_2).unwrap();

            assert_eq!(response_v2, expected_v2);
            assert_eq!(response_v1, expected_v1);
        }
    }

    #[test]
    fn serialize_list() {
        for (response, tag, expected_v2, expected_v1) in [(
            super::Response {
                list_items: vec![
                    ListItem {
                        mailbox_name: "INBOX".to_string(),
                        attributes: vec![Attribute::Subscribed],
                        tags: vec![],
                    },
                    ListItem {
                        mailbox_name: "foo".to_string(),
                        attributes: vec![],
                        tags: vec![Tag::ChildInfo(vec![ChildInfo::Subscribed])],
                    },
                ],
                status_items: vec![
                    StatusItem {
                        mailbox_name: "INBOX".to_string(),
                        items: vec![(Status::Messages, 17)],
                    },
                    StatusItem {
                        mailbox_name: "foo".to_string(),
                        items: vec![(Status::Messages, 30), (Status::Unseen, 29)],
                    },
                ],
            },
            "A01",
            concat!(
                "* LIST (\\Subscribed) \"/\" \"INBOX\"\r\n",
                "* LIST () \"/\" \"foo\" (\"CHILDINFO\" (\"SUBSCRIBED\"))\r\n",
                "* STATUS \"INBOX\" (MESSAGES 17)\r\n",
                "* STATUS \"foo\" (MESSAGES 30 UNSEEN 29)\r\n",
                "A01 OK completed\r\n"
            ),
            concat!(
                "* LIST () \"/\" \"INBOX\"\r\n",
                "* LIST () \"/\" \"foo\"\r\n",
                "A01 OK completed\r\n"
            ),
        )] {
            let response_v1 =
                String::from_utf8(response.serialize(tag.to_string(), ProtocolVersion::Rev1))
                    .unwrap();
            let response_v2 =
                String::from_utf8(response.serialize(tag.to_string(), ProtocolVersion::Rev2))
                    .unwrap();

            assert_eq!(response_v2, expected_v2);
            assert_eq!(response_v1, expected_v1);
        }
    }
}
