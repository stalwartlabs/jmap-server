use jmap_mail::mail::{schema::Keyword, HeaderName};

use crate::{parser::ImapFlag, StatusResponse};

use super::{
    literal_string, quoted_string, quoted_string_or_nil, quoted_timestamp, quoted_timestamp_or_nil,
    ImapResponse, Sequence,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arguments {
    pub sequence_set: Vec<Sequence>,
    pub attributes: Vec<Attribute>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub items: Vec<FetchItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchItem {
    pub id: u64,
    pub items: Vec<DataItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attribute {
    Envelope,
    Flags,
    InternalDate,
    Rfc822,
    Rfc822Size,
    Rfc822Header,
    Rfc822Text,
    Body,
    BodyStructure,
    BodySection {
        peek: bool,
        sections: Vec<Section>,
        partial: Option<(u64, u64)>,
    },
    Uid,
    Binary {
        peek: bool,
        sections: Vec<u64>,
        partial: Option<(u64, u64)>,
    },
    BinarySize {
        sections: Vec<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Section {
    Part { num: u64 },
    Header,
    HeaderFields { not: bool, fields: Vec<HeaderName> },
    Text,
    Mime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataItem {
    Binary {
        sections: Vec<u64>,
        offset: Option<u64>,
        contents: BodyContents,
    },
    Body {
        part: BodyPart,
    },
    BodyStructure {
        part: BodyPart,
    },
    BodySection {
        sections: Vec<Section>,
        origin_octet: Option<u64>,
        contents: String,
    },
    Envelope {
        date: Option<i64>,
        subject: Option<String>,
        from: Vec<Address>,
        sender: Vec<Address>,
        reply_to: Vec<Address>,
        to: Vec<Address>,
        cc: Vec<Address>,
        bcc: Vec<Address>,
        in_reply_to: Option<String>,
        message_id: Option<String>,
    },
    Flags {
        flags: Vec<Keyword>,
    },
    InternalDate {
        date: i64,
    },
    Uid {
        uid: u64,
    },
    Rfc822 {
        contents: String,
    },
    Rfc822Header {
        contents: String,
    },
    Rfc822Size {
        size: usize,
    },
    Rfc822Text {
        contents: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Address {
    Single(EmailAddress),
    Group(AddressGroup),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressGroup {
    pub name: Option<String>,
    pub addresses: Vec<EmailAddress>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmailAddress {
    pub name: Option<String>,
    pub address: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyContents {
    Text(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyPart {
    Multipart {
        parts: Vec<BodyPart>,
        subtype: String,
        // Extension data
        body_parameters: Vec<(String, String)>,
        body_disposition: Option<String>,
        body_language: Option<String>,
        body_location: Option<String>,
    },
    NonMultipart {
        body_type: Option<String>,
        body_subtype: Option<String>,
        body_parameters: Vec<(String, String)>,
        body_id: Option<String>,
        body_description: Option<String>,
        body_encoding: Option<String>,
        body_size_octets: usize,
        body_size_lines: Option<usize>,
        // Extension data
        body_md5: Option<String>,
        body_disposition: Option<String>,
        body_language: Option<String>,
        body_location: Option<String>,
    },
}

impl Address {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Address::Single(addr) => addr.serialize(buf),
            Address::Group(addr) => addr.serialize(buf),
        }
    }
}

impl EmailAddress {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        buf.push(b'(');
        if let Some(name) = &self.name {
            quoted_string(buf, name);
        } else {
            buf.extend_from_slice(b"NIL");
        }
        buf.extend_from_slice(b" NIL ");
        if let Some((local, host)) = self.address.split_once('@') {
            quoted_string(buf, local);
            buf.push(b' ');
            quoted_string(buf, host);
        } else {
            quoted_string(buf, &self.address);
            buf.extend_from_slice(b" \"\"");
        }
        buf.push(b')');
    }
}

impl AddressGroup {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(b"(NIL NIL ");
        if let Some(name) = &self.name {
            quoted_string(buf, name);
        } else {
            buf.extend_from_slice(b"\"\"");
        }
        buf.extend_from_slice(b" NIL)");
        for addr in &self.addresses {
            buf.push(b' ');
            addr.serialize(buf);
        }
        buf.extend_from_slice(b" (NIL NIL NIL NIL)");
    }
}

impl BodyPart {
    pub fn serialize(&self, buf: &mut Vec<u8>, is_extended: bool) {
        match self {
            BodyPart::Multipart {
                parts,
                subtype,
                body_parameters,
                body_disposition,
                body_language,
                body_location,
            } => {
                buf.push(b'(');
                for (pos, part) in parts.iter().enumerate() {
                    if pos > 0 {
                        buf.push(b' ');
                    }
                    part.serialize(buf, is_extended);
                }
                buf.push(b' ');
                quoted_string(buf, subtype);
                if is_extended {
                    if !body_parameters.is_empty() {
                        buf.extend_from_slice(b" (");
                        for (pos, (key, value)) in body_parameters.iter().enumerate() {
                            if pos > 0 {
                                buf.push(b' ');
                            }
                            quoted_string(buf, key);
                            buf.push(b' ');
                            quoted_string(buf, value);
                        }
                        buf.push(b')');
                    } else {
                        buf.extend_from_slice(b" NIL");
                    }
                    for item in [body_disposition, body_language, body_location] {
                        buf.push(b' ');
                        quoted_string_or_nil(buf, item.as_deref());
                    }
                }
                buf.push(b')');
            }
            BodyPart::NonMultipart {
                body_type,
                body_subtype,
                body_parameters,
                body_id,
                body_description,
                body_encoding,
                body_size_lines,
                body_size_octets,
                body_md5,
                body_disposition,
                body_language,
                body_location,
            } => {
                buf.push(b'(');
                quoted_string_or_nil(buf, body_type.as_deref());
                buf.push(b' ');
                quoted_string_or_nil(buf, body_subtype.as_deref());
                if !body_parameters.is_empty() {
                    buf.extend_from_slice(b" (");
                    for (pos, (key, value)) in body_parameters.iter().enumerate() {
                        if pos > 0 {
                            buf.push(b' ');
                        }
                        quoted_string(buf, key);
                        buf.push(b' ');
                        quoted_string(buf, value);
                    }
                    buf.push(b')');
                } else {
                    buf.extend_from_slice(b" NIL");
                }
                for item in [body_id, body_description, body_encoding] {
                    buf.push(b' ');
                    quoted_string_or_nil(buf, item.as_deref());
                }
                buf.push(b' ');
                buf.extend_from_slice(body_size_octets.to_string().as_bytes());
                if let Some(body_size_lines) = body_size_lines {
                    buf.push(b' ');
                    buf.extend_from_slice(body_size_lines.to_string().as_bytes());
                } else {
                    buf.extend_from_slice(b" NIL");
                }
                if is_extended {
                    for item in [body_md5, body_disposition, body_language, body_location] {
                        buf.push(b' ');
                        quoted_string_or_nil(buf, item.as_deref());
                    }
                }
                buf.push(b')');
            }
        }
    }
}

impl Section {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Section::Part { num } => {
                buf.extend_from_slice(num.to_string().as_bytes());
            }
            Section::Header => {
                buf.extend_from_slice(b"HEADER");
            }
            Section::HeaderFields { not, fields } => {
                if !not {
                    buf.extend_from_slice(b"HEADER.FIELDS ");
                } else {
                    buf.extend_from_slice(b"HEADER.FIELDS.NOT ");
                }
                buf.push(b'(');
                for (pos, field) in fields.iter().enumerate() {
                    if pos > 0 {
                        buf.push(b' ');
                    }
                    buf.extend_from_slice(field.as_str().as_bytes());
                }
                buf.push(b')');
            }
            Section::Text => {
                buf.extend_from_slice(b"TEXT");
            }
            Section::Mime => {
                buf.extend_from_slice(b"MIME");
            }
        };
    }
}

impl DataItem {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            DataItem::Binary {
                sections,
                offset,
                contents,
            } => {
                buf.extend_from_slice(b"BINARY[");
                for (pos, section) in sections.iter().enumerate() {
                    if pos > 0 {
                        buf.push(b'.');
                    }
                    buf.extend_from_slice(section.to_string().as_bytes());
                }
                if let Some(offset) = offset {
                    buf.extend_from_slice(b"]<");
                    buf.extend_from_slice(offset.to_string().as_bytes());
                    buf.extend_from_slice(b"> ");
                } else {
                    buf.extend_from_slice(b"] ");
                }
                match contents {
                    BodyContents::Text(text) => {
                        literal_string(buf, text);
                    }
                    BodyContents::Bytes(bytes) => {
                        buf.extend_from_slice(b"~{");
                        buf.extend_from_slice(bytes.len().to_string().as_bytes());
                        buf.extend_from_slice(b"}\r\n");
                        buf.extend_from_slice(bytes);
                    }
                }
            }
            DataItem::Body { part } => {
                buf.extend_from_slice(b"BODY ");
                part.serialize(buf, false);
            }
            DataItem::BodyStructure { part } => {
                buf.extend_from_slice(b"BODYSTRUCTURE ");
                part.serialize(buf, true);
            }
            DataItem::BodySection {
                sections,
                origin_octet,
                contents,
            } => {
                buf.extend_from_slice(b"BODY[");
                for (pos, section) in sections.iter().enumerate() {
                    if pos > 0 {
                        buf.push(b'.');
                    }
                    section.serialize(buf);
                }
                if let Some(origin_octet) = origin_octet {
                    buf.extend_from_slice(b"]<");
                    buf.extend_from_slice(origin_octet.to_string().as_bytes());
                    buf.extend_from_slice(b"> ");
                } else {
                    buf.extend_from_slice(b"] ");
                }
                literal_string(buf, contents);
            }
            DataItem::Envelope {
                date,
                subject,
                from,
                sender,
                reply_to,
                to,
                cc,
                bcc,
                in_reply_to,
                message_id,
            } => {
                buf.extend_from_slice(b"ENVELOPE (");
                quoted_timestamp_or_nil(buf, *date);
                buf.push(b' ');
                quoted_string_or_nil(buf, subject.as_deref());
                for addresses in [from, sender, reply_to, to, cc, bcc] {
                    buf.push(b' ');
                    if !addresses.is_empty() {
                        buf.push(b'(');
                        for (pos, address) in addresses.iter().enumerate() {
                            if pos > 0 {
                                buf.push(b' ');
                            }
                            address.serialize(buf);
                        }
                        buf.push(b')');
                    } else {
                        buf.extend_from_slice(b"NIL");
                    }
                }
                for item in [in_reply_to, message_id] {
                    buf.push(b' ');
                    quoted_string_or_nil(buf, item.as_deref());
                }
                buf.push(b')');
            }
            DataItem::Flags { flags } => {
                buf.extend_from_slice(b"FLAGS (");
                for (pos, flag) in flags.iter().enumerate() {
                    if pos > 0 {
                        buf.push(b' ');
                    }
                    buf.extend_from_slice(flag.to_imap().as_bytes());
                }
                buf.push(b')');
            }
            DataItem::InternalDate { date } => {
                buf.extend_from_slice(b"INTERNALDATE ");
                quoted_timestamp(buf, *date);
            }
            DataItem::Uid { uid } => {
                buf.extend_from_slice(b"UID ");
                buf.extend_from_slice(uid.to_string().as_bytes());
            }
            DataItem::Rfc822 { contents } => {
                buf.extend_from_slice(b"RFC822 ");
                literal_string(buf, contents);
            }
            DataItem::Rfc822Header { contents } => {
                buf.extend_from_slice(b"RFC822.HEADER ");
                literal_string(buf, contents);
            }
            DataItem::Rfc822Size { size } => {
                buf.extend_from_slice(b"RFC822.SIZE ");
                buf.extend_from_slice(size.to_string().as_bytes());
            }
            DataItem::Rfc822Text { contents } => {
                buf.extend_from_slice(b"RFC822.TEXT ");
                literal_string(buf, contents);
            }
        }
    }
}

impl FetchItem {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(b"* ");
        buf.extend_from_slice(self.id.to_string().as_bytes());
        buf.extend_from_slice(b" (");
        for (pos, item) in self.items.iter().enumerate() {
            if pos > 0 {
                buf.push(b' ');
            }
            item.serialize(buf);
        }
        buf.extend_from_slice(b")\r\n");
    }
}

impl ImapResponse for Response {
    fn serialize(&self, tag: String, _version: super::ProtocolVersion) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        for item in &self.items {
            item.serialize(&mut buf);
        }
        StatusResponse::ok(tag.into(), None, "completed").serialize(&mut buf);
        buf
    }
}

#[cfg(test)]
mod tests {
    use jmap_mail::mail::{schema::Keyword, HeaderName};
    use mail_parser::RfcHeader;
    use store::core::tag::Tag;

    use crate::protocol::{ImapResponse, ProtocolVersion};

    use super::{Address, AddressGroup, EmailAddress, FetchItem, Response, Section};

    #[test]
    fn serialize_fetch_data_item() {
        for (item, expected_response) in [
            (
                super::DataItem::Envelope {
                    date: 837570205.into(),
                    subject: "IMAP4rev2 WG mtg summary and minutes".to_string().into(),
                    from: vec![Address::Single(EmailAddress {
                        name: "Terry Gray".to_string().into(),
                        address: "gray@cac.washington.edu".to_string(),
                    })],
                    sender: vec![Address::Single(EmailAddress {
                        name: "Terry Gray".to_string().into(),
                        address: "gray@cac.washington.edu".to_string(),
                    })],
                    reply_to: vec![Address::Single(EmailAddress {
                        name: "Terry Gray".to_string().into(),
                        address: "gray@cac.washington.edu".to_string(),
                    })],
                    to: vec![Address::Single(EmailAddress {
                        name: None,
                        address: "imap@cac.washington.edu".to_string(),
                    })],
                    cc: vec![
                        Address::Single(EmailAddress {
                            name: None,
                            address: "minutes@CNRI.Reston.VA.US".to_string(),
                        }),
                        Address::Single(EmailAddress {
                            name: "John Klensin".to_string().into(),
                            address: "KLENSIN@MIT.EDU".to_string(),
                        }),
                    ],
                    bcc: vec![],
                    in_reply_to: None,
                    message_id: "<B27397-0100000@cac.washington.ed>".to_string().into(),
                },
                concat!(
                    "ENVELOPE (\"Wed, 17 Jul 1996 02:23:25 +0000\" ",
                    "\"IMAP4rev2 WG mtg summary and minutes\" ",
                    "((\"Terry Gray\" NIL \"gray\" \"cac.washington.edu\")) ",
                    "((\"Terry Gray\" NIL \"gray\" \"cac.washington.edu\")) ",
                    "((\"Terry Gray\" NIL \"gray\" \"cac.washington.edu\")) ",
                    "((NIL NIL \"imap\" \"cac.washington.edu\")) ",
                    "((NIL NIL \"minutes\" \"CNRI.Reston.VA.US\") ",
                    "(\"John Klensin\" NIL \"KLENSIN\" \"MIT.EDU\")) NIL NIL ",
                    "\"<B27397-0100000@cac.washington.ed>\")"
                ),
            ),
            (
                super::DataItem::Envelope {
                    date: 837570205.into(),
                    subject: "Group test".to_string().into(),
                    from: vec![Address::Single(EmailAddress {
                        name: "Bill Foobar".to_string().into(),
                        address: "foobar@example.com".to_string(),
                    })],
                    sender: vec![],
                    reply_to: vec![],
                    to: vec![Address::Group(AddressGroup {
                        name: "Friends and Family".to_string().into(),
                        addresses: vec![
                            EmailAddress {
                                name: "John Doe".to_string().into(),
                                address: "jdoe@example.com".to_string(),
                            },
                            EmailAddress {
                                name: "Jane Smith".to_string().into(),
                                address: "jane.smith@example.com".to_string(),
                            },
                        ],
                    })],
                    cc: vec![],
                    bcc: vec![],
                    in_reply_to: None,
                    message_id: "<B27397-0100000@cac.washington.ed>".to_string().into(),
                },
                concat!(
                    "ENVELOPE (\"Wed, 17 Jul 1996 02:23:25 +0000\" ",
                    "\"Group test\" ",
                    "((\"Bill Foobar\" NIL \"foobar\" \"example.com\")) NIL NIL ",
                    "((NIL NIL \"Friends and Family\" NIL) ",
                    "(\"John Doe\" NIL \"jdoe\" \"example.com\") ",
                    "(\"Jane Smith\" NIL \"jane.smith\" \"example.com\") ",
                    "(NIL NIL NIL NIL)) ",
                    "NIL NIL NIL \"<B27397-0100000@cac.washington.ed>\")"
                ),
            ),
            (
                super::DataItem::Body {
                    part: super::BodyPart::NonMultipart {
                        body_type: "TEXT".to_string().into(),
                        body_subtype: "PLAIN".to_string().into(),
                        body_parameters: vec![("CHARSET".to_string(), "US-ASCII".to_string())],
                        body_id: None,
                        body_description: None,
                        body_encoding: "7BIT".to_string().into(),
                        body_size_octets: 2279,
                        body_size_lines: 48.into(),
                        body_md5: None,
                        body_disposition: None,
                        body_language: None,
                        body_location: None,
                    },
                },
                "BODY (\"TEXT\" \"PLAIN\" (\"CHARSET\" \"US-ASCII\") NIL NIL \"7BIT\" 2279 48)",
            ),
            (
                super::DataItem::Body {
                    part: super::BodyPart::Multipart {
                        parts: vec![
                            super::BodyPart::NonMultipart {
                                body_type: "TEXT".to_string().into(),
                                body_subtype: "PLAIN".to_string().into(),
                                body_parameters: vec![(
                                    "CHARSET".to_string(),
                                    "US-ASCII".to_string(),
                                )],
                                body_id: None,
                                body_description: None,
                                body_encoding: "7BIT".to_string().into(),
                                body_size_octets: 1152,
                                body_size_lines: 23.into(),
                                body_md5: None,
                                body_disposition: None,
                                body_language: None,
                                body_location: None,
                            },
                            super::BodyPart::NonMultipart {
                                body_type: "TEXT".to_string().into(),
                                body_subtype: "PLAIN".to_string().into(),
                                body_parameters: vec![
                                    ("CHARSET".to_string(), "US-ASCII".to_string()),
                                    ("NAME".to_string(), "cc.diff".to_string()),
                                ],
                                body_id: "<960723163407.20117h@cac.washington.edu>"
                                    .to_string()
                                    .into(),
                                body_description: "Compiler diff".to_string().into(),
                                body_encoding: "BASE64".to_string().into(),
                                body_size_octets: 4554,
                                body_size_lines: 73.into(),
                                body_md5: None,
                                body_disposition: None,
                                body_language: None,
                                body_location: None,
                            },
                        ],
                        subtype: "MIXED".to_string(),
                        body_parameters: vec![],
                        body_disposition: None,
                        body_language: None,
                        body_location: None,
                    },
                },
                concat!(
                    "BODY ((\"TEXT\" \"PLAIN\" (\"CHARSET\" \"US-ASCII\") ",
                    "NIL NIL \"7BIT\" 1152 23) ",
                    "(\"TEXT\" \"PLAIN\" (\"CHARSET\" \"US-ASCII\" \"NAME\" \"cc.diff\") ",
                    "\"<960723163407.20117h@cac.washington.edu>\" \"Compiler diff\" ",
                    "\"BASE64\" 4554 73) \"MIXED\")",
                ),
            ),
            (
                super::DataItem::BodyStructure {
                    part: super::BodyPart::Multipart {
                        parts: vec![
                            super::BodyPart::Multipart {
                                parts: vec![
                                    super::BodyPart::NonMultipart {
                                        body_type: "TEXT".to_string().into(),
                                        body_subtype: "PLAIN".to_string().into(),
                                        body_parameters: vec![(
                                            "CHARSET".to_string(),
                                            "UTF-8".to_string(),
                                        )],
                                        body_id: "<111@domain.com>".to_string().into(),
                                        body_description: "Text part".to_string().into(),
                                        body_encoding: "7BIT".to_string().into(),
                                        body_size_octets: 1152,
                                        body_size_lines: 23.into(),
                                        body_md5: "8o3456".to_string().into(),
                                        body_disposition: "inline".to_string().into(),
                                        body_language: "en-US".to_string().into(),
                                        body_location: "right here".to_string().into(),
                                    },
                                    super::BodyPart::NonMultipart {
                                        body_type: "TEXT".to_string().into(),
                                        body_subtype: "HTML".to_string().into(),
                                        body_parameters: vec![(
                                            "CHARSET".to_string(),
                                            "UTF-8".to_string(),
                                        )],
                                        body_id: "<54535@domain.com>".to_string().into(),
                                        body_description: "HTML part".to_string().into(),
                                        body_encoding: "8BIT".to_string().into(),
                                        body_size_octets: 45345,
                                        body_size_lines: 994.into(),
                                        body_md5: "53454".to_string().into(),
                                        body_disposition: "inline".to_string().into(),
                                        body_language: "en-US".to_string().into(),
                                        body_location: "right there".to_string().into(),
                                    },
                                ],
                                subtype: "ALTERNATIVE".to_string(),
                                body_parameters: vec![(
                                    "x-param".to_string(),
                                    "a very special parameter".to_string(),
                                )],
                                body_disposition: None,
                                body_language: "en-US".to_string().into(),
                                body_location: "unknown".to_string().into(),
                            },
                            super::BodyPart::NonMultipart {
                                body_type: "APPLICATION".to_string().into(),
                                body_subtype: "MSWORD".to_string().into(),
                                body_parameters: vec![(
                                    "NAME".to_string(),
                                    "chimichangas.docx".to_string(),
                                )],
                                body_id: "<4444@chimi.changa>".to_string().into(),
                                body_description: "Chimichangas recipe".to_string().into(),
                                body_encoding: "base64".to_string().into(),
                                body_size_octets: 84723,
                                body_size_lines: None,
                                body_md5: "1234".to_string().into(),
                                body_disposition: "attachment".to_string().into(),
                                body_language: "es-MX".to_string().into(),
                                body_location: "secret location".to_string().into(),
                            },
                        ],
                        subtype: "MIXED".to_string(),
                        body_parameters: vec![],
                        body_disposition: None,
                        body_language: None,
                        body_location: None,
                    },
                },
                concat!(
                    "BODYSTRUCTURE (((\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") ",
                    "\"<111@domain.com>\" \"Text part\" \"7BIT\" 1152 23 \"8o3456\"",
                    " \"inline\" \"en-US\" \"right here\") (\"TEXT\" \"HTML\" ",
                    "(\"CHARSET\" \"UTF-8\") \"<54535@domain.com>\" \"HTML part\" ",
                    "\"8BIT\" 45345 994 \"53454\" \"inline\" \"en-US\" \"right ",
                    "there\") \"ALTERNATIVE\" (\"x-param\" \"a very special parameter\") ",
                    "NIL \"en-US\" \"unknown\") (\"APPLICATION\" \"MSWORD\" (\"NAME\" ",
                    "\"chimichangas.docx\") \"<4444@chimi.changa>\" \"Chimichangas ",
                    "recipe\" \"base64\" 84723 NIL \"1234\" \"attachment\" ",
                    "\"es-MX\" \"secret location\") \"MIXED\" NIL NIL NIL NIL)",
                ),
            ),
            (
                super::DataItem::Binary {
                    sections: vec![1, 2, 3],
                    offset: 10.into(),
                    contents: super::BodyContents::Bytes(b"hello".to_vec()),
                },
                "BINARY[1.2.3]<10> ~{5}\r\nhello",
            ),
            (
                super::DataItem::Binary {
                    sections: vec![1, 2, 3],
                    offset: None,
                    contents: super::BodyContents::Text("hello".to_string()),
                },
                "BINARY[1.2.3] {5}\r\nhello",
            ),
            (
                super::DataItem::BodySection {
                    sections: vec![
                        Section::Part { num: 1 },
                        Section::Part { num: 2 },
                        Section::Mime,
                    ],
                    origin_octet: 11.into(),
                    contents: "howdy".to_string(),
                },
                "BODY[1.2.MIME]<11> {5}\r\nhowdy",
            ),
            (
                super::DataItem::BodySection {
                    sections: vec![Section::HeaderFields {
                        not: true,
                        fields: vec![
                            HeaderName::Rfc(RfcHeader::Subject),
                            HeaderName::Other("x-special".to_string()),
                        ],
                    }],
                    origin_octet: None,
                    contents: "howdy".to_string(),
                },
                "BODY[HEADER.FIELDS.NOT (Subject x-special)] {5}\r\nhowdy",
            ),
            (
                super::DataItem::BodySection {
                    sections: vec![Section::HeaderFields {
                        not: false,
                        fields: vec![
                            HeaderName::Rfc(RfcHeader::From),
                            HeaderName::Rfc(RfcHeader::ListArchive),
                        ],
                    }],
                    origin_octet: None,
                    contents: "howdy".to_string(),
                },
                "BODY[HEADER.FIELDS (From List-Archive)] {5}\r\nhowdy",
            ),
            (
                super::DataItem::Flags {
                    flags: vec![Keyword {
                        tag: Tag::Static(Keyword::SEEN),
                    }],
                },
                "FLAGS (\\Seen)",
            ),
            (
                super::DataItem::InternalDate { date: 482374938 },
                "INTERNALDATE \"Mon, 15 Apr 1985 01:02:18 +0000\"",
            ),
        ] {
            let mut buf = Vec::with_capacity(100);

            item.serialize(&mut buf);

            assert_eq!(String::from_utf8(buf).unwrap(), expected_response);
        }
    }

    #[test]
    fn serialize_fetch() {
        assert_eq!(
            String::from_utf8(
                Response {
                    items: vec![FetchItem {
                        id: 123,
                        items: vec![
                            super::DataItem::Flags {
                                flags: vec![
                                    Keyword {
                                        tag: Tag::Static(Keyword::DELETED),
                                    },
                                    Keyword {
                                        tag: Tag::Static(Keyword::FLAGGED),
                                    },
                                ],
                            },
                            super::DataItem::Uid { uid: 983 },
                            super::DataItem::Rfc822Size { size: 443 },
                            super::DataItem::Rfc822Text {
                                contents: "hi".to_string()
                            },
                            super::DataItem::Rfc822Header {
                                contents: "header".to_string()
                            },
                        ],
                    }],
                }
                .serialize("abc".to_string(), ProtocolVersion::Rev1),
            )
            .unwrap(),
            concat!(
                "* 123 (FLAGS (\\Deleted \\Flagged) ",
                "UID 983 ",
                "RFC822.SIZE 443 ",
                "RFC822.TEXT {2}\r\nhi ",
                "RFC822.HEADER {6}\r\nheader)\r\n",
                "abc OK completed\r\n"
            )
        );
    }
}
