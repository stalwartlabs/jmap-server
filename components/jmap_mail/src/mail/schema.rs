use std::{
    collections::HashMap,
    fmt::{self, Display},
};

use jmap::{
    id::{blob::JMAPBlob, jmap::JMAPId},
    jmap_store::Object,
    request::ResultReference,
};
use mail_parser::{
    parsers::header::{parse_header_name, HeaderParserResult},
    RfcHeader,
};
use serde::{Deserialize, Serialize};
use store::{
    chrono::{DateTime, Utc},
    core::{collection::Collection, tag::Tag},
    FieldId,
};

use super::{HeaderName, MessageField};

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct Email {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<JMAPId>,

    #[serde(rename = "blobId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_id: Option<JMAPBlob>,

    #[serde(rename = "threadId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<JMAPId>,

    #[serde(rename = "mailboxIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mailbox_ids: Option<HashMap<JMAPId, bool>>,

    #[serde(rename = "#mailboxIds")]
    #[serde(skip_serializing)]
    pub mailbox_ids_ref: Option<ResultReference>,

    #[serde(rename = "keywords")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keywords: Option<HashMap<Keyword, bool>>,

    #[serde(rename = "size")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<usize>,

    #[serde(rename = "receivedAt")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_at: Option<DateTime<Utc>>,

    #[serde(rename = "messageId", alias = "header:Message-ID:asMessageIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<Vec<String>>,

    #[serde(rename = "inReplyTo", alias = "header:In-Reply-To:asMessageIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<Vec<String>>,

    #[serde(rename = "references", alias = "header:References:asMessageIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<String>>,

    #[serde(rename = "sender", alias = "header:Sender:asAddresses")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender: Option<Vec<EmailAddress>>,

    #[serde(rename = "from", alias = "header:From:asAddresses")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<Vec<EmailAddress>>,

    #[serde(rename = "to", alias = "header:To:asAddresses")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<Vec<EmailAddress>>,

    #[serde(rename = "cc", alias = "header:Cc:asAddresses")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cc: Option<Vec<EmailAddress>>,

    #[serde(rename = "bcc", alias = "header:Bcc:asAddresses")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bcc: Option<Vec<EmailAddress>>,

    #[serde(rename = "replyTo", alias = "header:Reply-To:asAddresses")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<Vec<EmailAddress>>,

    #[serde(rename = "subject", alias = "header:Subject:asText")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,

    #[serde(rename = "sentAt", alias = "header:Date:asDate")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sent_at: Option<DateTime<Utc>>,

    #[serde(rename = "bodyStructure")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_structure: Option<Box<EmailBodyPart>>,

    #[serde(rename = "bodyValues")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_values: Option<HashMap<String, EmailBodyValue>>,

    #[serde(rename = "textBody")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text_body: Option<Vec<EmailBodyPart>>,

    #[serde(rename = "htmlBody")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub html_body: Option<Vec<EmailBodyPart>>,

    #[serde(rename = "attachments")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attachments: Option<Vec<EmailBodyPart>>,

    #[serde(rename = "hasAttachment")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_attachment: Option<bool>,

    #[serde(rename = "preview")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
    /*#[serde(flatten)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<Header, Option<HeaderValue>>,

    #[serde(flatten)]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub patch: HashMap<String, bool>,*/
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EmailBodyPart {
    #[serde(rename = "partId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub part_id: Option<String>,

    #[serde(rename = "blobId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_id: Option<JMAPBlob>,

    #[serde(rename = "size")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<usize>,

    #[serde(rename = "headers")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<Vec<EmailHeader>>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,

    #[serde(rename = "charset")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub charset: Option<String>,

    #[serde(rename = "disposition")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disposition: Option<String>,

    #[serde(rename = "cid")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cid: Option<String>,

    #[serde(rename = "language")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<Vec<String>>,

    #[serde(rename = "location")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,

    #[serde(rename = "subParts")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_parts: Option<Vec<EmailBodyPart>>,
    /*#[serde(flatten)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header: Option<HashMap<Header, HeaderValue>>,*/
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EmailBodyValue {
    #[serde(rename = "value")]
    pub value: String,

    #[serde(rename = "isEncodingProblem")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_encoding_problem: Option<bool>,

    #[serde(rename = "isTruncated")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_truncated: Option<bool>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct EmailAddress {
    pub name: Option<String>,
    pub email: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct EmailAddressGroup {
    pub name: Option<String>,
    pub addresses: Vec<EmailAddress>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct EmailHeader {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Keyword {
    pub tag: Tag,
}

impl Keyword {
    pub const SEEN: u8 = 0;
    pub const DRAFT: u8 = 1;
    pub const FLAGGED: u8 = 2;
    pub const ANSWERED: u8 = 3;
    pub const RECENT: u8 = 4;
    pub const IMPORTANT: u8 = 5;
    pub const PHISHING: u8 = 6;
    pub const JUNK: u8 = 7;
    pub const NOTJUNK: u8 = 8;

    pub fn new(tag: Tag) -> Self {
        Self { tag }
    }

    pub fn parse(value: &str) -> Self {
        Keyword {
            tag: if value.starts_with('$') {
                match value {
                    "$seen" => Tag::Static(Self::SEEN),
                    "$draft" => Tag::Static(Self::DRAFT),
                    "$flagged" => Tag::Static(Self::FLAGGED),
                    "$answered" => Tag::Static(Self::ANSWERED),
                    "$recent" => Tag::Static(Self::RECENT),
                    "$important" => Tag::Static(Self::IMPORTANT),
                    "$phishing" => Tag::Static(Self::PHISHING),
                    "$junk" => Tag::Static(Self::JUNK),
                    "$notjunk" => Tag::Static(Self::NOTJUNK),
                    _ => Tag::Text(value.to_string()),
                }
            } else {
                Tag::Text(value.to_string())
            },
        }
    }
}

impl Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.tag {
            Tag::Static(keyword) => match *keyword {
                Self::SEEN => write!(f, "$seen"),
                Self::DRAFT => write!(f, "$draft"),
                Self::FLAGGED => write!(f, "$flagged"),
                Self::ANSWERED => write!(f, "$answered"),
                Self::RECENT => write!(f, "$recent"),
                Self::IMPORTANT => write!(f, "$important"),
                Self::PHISHING => write!(f, "$phishing"),
                Self::JUNK => write!(f, "$junk"),
                Self::NOTJUNK => write!(f, "$notjunk"),
                9..=u8::MAX => Err(std::fmt::Error),
            },
            Tag::Text(value) => write!(f, "{}", value),
            _ => Err(std::fmt::Error),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Property {
    Id,
    BlobId,
    ThreadId,
    MailboxIds,
    Keywords,
    Size,
    ReceivedAt,
    MessageId,
    InReplyTo,
    References,
    Sender,
    From,
    To,
    Cc,
    Bcc,
    ReplyTo,
    Subject,
    SentAt,
    HasAttachment,
    Preview,
    BodyValues,
    TextBody,
    HtmlBody,
    Attachments,
    BodyStructure,
    Header(HeaderProperty),
}

impl Property {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "id" => Some(Property::Id),
            "blobId" => Some(Property::BlobId),
            "threadId" => Some(Property::ThreadId),
            "mailboxIds" => Some(Property::MailboxIds),
            "keywords" => Some(Property::Keywords),
            "size" => Some(Property::Size),
            "receivedAt" => Some(Property::ReceivedAt),
            "messageId" => Some(Property::MessageId),
            "inReplyTo" => Some(Property::InReplyTo),
            "references" => Some(Property::References),
            "sender" => Some(Property::Sender),
            "from" => Some(Property::From),
            "to" => Some(Property::To),
            "cc" => Some(Property::Cc),
            "bcc" => Some(Property::Bcc),
            "replyTo" => Some(Property::ReplyTo),
            "subject" => Some(Property::Subject),
            "sentAt" => Some(Property::SentAt),
            "hasAttachment" => Some(Property::HasAttachment),
            "preview" => Some(Property::Preview),
            "bodyValues" => Some(Property::BodyValues),
            "textBody" => Some(Property::TextBody),
            "htmlBody" => Some(Property::HtmlBody),
            "attachments" => Some(Property::Attachments),
            "bodyStructure" => Some(Property::BodyStructure),
            _ if value.starts_with("header:") => {
                Some(Property::Header(HeaderProperty::parse(value)?))
            }
            _ => None,
        }
    }

    pub fn as_rfc_header(&self) -> RfcHeader {
        match self {
            Property::MessageId => RfcHeader::MessageId,
            Property::InReplyTo => RfcHeader::InReplyTo,
            Property::References => RfcHeader::References,
            Property::Sender => RfcHeader::Sender,
            Property::From => RfcHeader::From,
            Property::To => RfcHeader::To,
            Property::Cc => RfcHeader::Cc,
            Property::Bcc => RfcHeader::Bcc,
            Property::ReplyTo => RfcHeader::ReplyTo,
            Property::Subject => RfcHeader::Subject,
            Property::SentAt => RfcHeader::Date,
            Property::Header(HeaderProperty {
                header: HeaderName::Rfc(rfc),
                ..
            }) => *rfc,
            _ => unreachable!(),
        }
    }
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::BlobId => write!(f, "blobId"),
            Property::ThreadId => write!(f, "threadId"),
            Property::MailboxIds => write!(f, "mailboxIds"),
            Property::Keywords => write!(f, "keywords"),
            Property::Size => write!(f, "size"),
            Property::ReceivedAt => write!(f, "receivedAt"),
            Property::MessageId => write!(f, "messageId"),
            Property::InReplyTo => write!(f, "inReplyTo"),
            Property::References => write!(f, "references"),
            Property::Sender => write!(f, "sender"),
            Property::From => write!(f, "from"),
            Property::To => write!(f, "to"),
            Property::Cc => write!(f, "cc"),
            Property::Bcc => write!(f, "bcc"),
            Property::ReplyTo => write!(f, "replyTo"),
            Property::Subject => write!(f, "subject"),
            Property::SentAt => write!(f, "sentAt"),
            Property::HasAttachment => write!(f, "hasAttachment"),
            Property::Preview => write!(f, "preview"),
            Property::BodyValues => write!(f, "bodyValues"),
            Property::TextBody => write!(f, "textBody"),
            Property::HtmlBody => write!(f, "htmlBody"),
            Property::Attachments => write!(f, "attachments"),
            Property::BodyStructure => write!(f, "bodyStructure"),
            Property::Header(header) => header.fmt(f),
        }
    }
}

impl Default for Property {
    fn default() -> Self {
        Property::Id
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub enum BodyProperty {
    PartId,
    BlobId,
    Size,
    Name,
    Type,
    Charset,
    Header(HeaderProperty),
    Headers,
    Disposition,
    Cid,
    Language,
    Location,
    Subparts,
}

impl BodyProperty {
    pub fn parse(value: &str) -> Option<BodyProperty> {
        match value {
            "partId" => Some(BodyProperty::PartId),
            "blobId" => Some(BodyProperty::BlobId),
            "size" => Some(BodyProperty::Size),
            "name" => Some(BodyProperty::Name),
            "type" => Some(BodyProperty::Type),
            "charset" => Some(BodyProperty::Charset),
            "headers" => Some(BodyProperty::Headers),
            "disposition" => Some(BodyProperty::Disposition),
            "cid" => Some(BodyProperty::Cid),
            "language" => Some(BodyProperty::Language),
            "location" => Some(BodyProperty::Location),
            "subParts" => Some(BodyProperty::Subparts),
            _ if value.starts_with("header:") => {
                Some(BodyProperty::Header(HeaderProperty::parse(value)?))
            }
            _ => None,
        }
    }
}

impl Display for BodyProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BodyProperty::PartId => write!(f, "partId"),
            BodyProperty::BlobId => write!(f, "blobId"),
            BodyProperty::Size => write!(f, "size"),
            BodyProperty::Name => write!(f, "name"),
            BodyProperty::Type => write!(f, "type"),
            BodyProperty::Charset => write!(f, "charset"),
            BodyProperty::Header(header) => header.fmt(f),
            BodyProperty::Headers => write!(f, "headers"),
            BodyProperty::Disposition => write!(f, "disposition"),
            BodyProperty::Cid => write!(f, "cid"),
            BodyProperty::Language => write!(f, "language"),
            BodyProperty::Location => write!(f, "location"),
            BodyProperty::Subparts => write!(f, "subParts"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct HeaderProperty {
    pub form: HeaderForm,
    pub header: HeaderName,
    pub all: bool,
}

impl HeaderProperty {
    pub fn new_rfc(header: RfcHeader, form: HeaderForm, all: bool) -> Self {
        HeaderProperty {
            form,
            header: HeaderName::Rfc(header),
            all,
        }
    }
    pub fn new_other(header: String, form: HeaderForm, all: bool) -> Self {
        HeaderProperty {
            form,
            header: HeaderName::Other(header),
            all,
        }
    }

    pub fn parse(value: &str) -> Option<HeaderProperty> {
        let mut all = false;
        let mut form = HeaderForm::Raw;
        let mut header = None;
        for (pos, part) in value.split(':').enumerate() {
            match pos {
                0 if part == "header" => (),
                1 => match parse_header_name(part.as_bytes()) {
                    (_, HeaderParserResult::Rfc(rfc_header)) => {
                        header = Some(HeaderName::Rfc(rfc_header));
                    }
                    (_, HeaderParserResult::Other(other_header)) => {
                        header = Some(HeaderName::Other(other_header.as_ref().to_owned()));
                    }
                    _ => return None,
                },
                2 | 3 if part == "all" => all = true,
                2 => {
                    form = HeaderForm::parse(part)?;
                }
                _ => return None,
            }
        }
        Some(HeaderProperty {
            form,
            header: header?,
            all,
        })
    }
}

impl Display for HeaderProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "header:")?;
        match &self.header {
            HeaderName::Rfc(rfc) => rfc.fmt(f)?,
            HeaderName::Other(name) => name.fmt(f)?,
        }
        self.form.fmt(f)?;
        if self.all {
            write!(f, ":all")
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum HeaderForm {
    Raw,
    Text,
    Addresses,
    GroupedAddresses,
    MessageIds,
    Date,
    URLs,
}

impl HeaderForm {
    pub fn parse(value: &str) -> Option<HeaderForm> {
        match value {
            "asText" => Some(HeaderForm::Text),
            "asAddresses" => Some(HeaderForm::Addresses),
            "asGroupedAddresses" => Some(HeaderForm::GroupedAddresses),
            "asMessageIds" => Some(HeaderForm::MessageIds),
            "asDate" => Some(HeaderForm::Date),
            "asURLs" => Some(HeaderForm::URLs),
            _ => None,
        }
    }
}

impl Display for HeaderForm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HeaderForm::Raw => Ok(()),
            HeaderForm::Text => write!(f, ":asText"),
            HeaderForm::Addresses => write!(f, ":asAddresses"),
            HeaderForm::GroupedAddresses => write!(f, ":asGroupedAddresses"),
            HeaderForm::MessageIds => write!(f, ":asMessageIds"),
            HeaderForm::Date => write!(f, ":asDate"),
            HeaderForm::URLs => write!(f, ":asURLs"),
        }
    }
}

// Property de/serialization
impl Serialize for Property {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}
struct PropertyVisitor;

impl<'de> serde::de::Visitor<'de> for PropertyVisitor {
    type Value = Property;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP e-mail property")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Property::parse(v).ok_or_else(|| E::custom(format!("Invalid property: {}", v)))
    }
}

impl<'de> Deserialize<'de> for Property {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(PropertyVisitor)
    }
}

// BodyProperty de/serialization
impl Serialize for BodyProperty {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}
struct BodyPropertyVisitor;

impl<'de> serde::de::Visitor<'de> for BodyPropertyVisitor {
    type Value = BodyProperty;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP body property")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        BodyProperty::parse(v).ok_or_else(|| E::custom(format!("Invalid body property: {}", v)))
    }
}

impl<'de> Deserialize<'de> for BodyProperty {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(BodyPropertyVisitor)
    }
}

// HeaderProperty de/serialization
impl Serialize for HeaderProperty {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}
struct HeaderPropertyVisitor;

impl<'de> serde::de::Visitor<'de> for HeaderPropertyVisitor {
    type Value = HeaderProperty;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP header property")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        HeaderProperty::parse(v).ok_or_else(|| E::custom(format!("Invalid property: {}", v)))
    }
}

impl<'de> Deserialize<'de> for HeaderProperty {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(HeaderPropertyVisitor)
    }
}

// Keyword de/serialization
impl Serialize for Keyword {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}
struct KeywordVisitor;

impl<'de> serde::de::Visitor<'de> for KeywordVisitor {
    type Value = Keyword;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP keyword")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Keyword::parse(v))
    }
}

impl<'de> Deserialize<'de> for Keyword {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(KeywordVisitor)
    }
}

impl From<Property> for FieldId {
    fn from(value: Property) -> Self {
        match value {
            Property::ThreadId => MessageField::ThreadId.into(),
            Property::MailboxIds => MessageField::Mailbox.into(),
            Property::Keywords => MessageField::Keyword.into(),
            _ => u8::MAX - 1, // Not used
        }
    }
}

impl Object for Email {
    type Property = Property;

    type Value = ();

    fn id(&self) -> Option<&JMAPId> {
        self.id.as_ref()
    }

    fn required() -> &'static [Self::Property] {
        &[]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[]
    }

    fn collection() -> Collection {
        Collection::Mail
    }

    fn hide_account() -> bool {
        false
    }
}
