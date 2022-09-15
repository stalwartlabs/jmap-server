/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use std::{borrow::Cow, fmt};

use jmap::{
    request::{
        query::FilterDeserializer, ArgumentDeserializer, MaybeIdReference, MaybeResultReference,
    },
    types::{blob::JMAPBlob, jmap::JMAPId},
    types::{date::JMAPDate, json_pointer::JSONPointer},
};
use serde::{de::IgnoredAny, ser::SerializeMap, Deserialize, Serialize};
use store::{ahash::AHashSet, core::vec_map::VecMap};

use super::{
    get::GetArguments,
    import::EmailImport,
    schema::{
        BodyProperty, Email, EmailAddress, EmailBodyPart, EmailHeader, Filter, HeaderForm,
        HeaderProperty, Keyword, Property, Value,
    },
    search_snippet::SearchSnippetGetRequest,
};

// Email de/serialization
impl Serialize for Email {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(self.properties.len().into())?;

        for (name, value) in &self.properties {
            match value {
                Value::Id { value } => map.serialize_entry(name, value)?,
                Value::Blob { value } => map.serialize_entry(name, value)?,
                Value::Size { value } => map.serialize_entry(name, value)?,
                Value::Bool { value } => map.serialize_entry(name, value)?,
                Value::Keywords { value, .. } => map.serialize_entry(name, value)?,
                Value::MailboxIds { value, .. } => map.serialize_entry(name, value)?,
                Value::ResultReference { value } => map.serialize_entry(name, value)?,
                Value::BodyPart { value } => map.serialize_entry(name, value)?,
                Value::BodyPartList { value } => map.serialize_entry(name, value)?,
                Value::BodyValues { value } => map.serialize_entry(name, value)?,
                Value::Text { value } => map.serialize_entry(name, value)?,
                Value::TextList { value } => map.serialize_entry(name, value)?,
                Value::TextListMany { value } => map.serialize_entry(name, value)?,
                Value::Date { value } => map.serialize_entry(name, value)?,
                Value::DateList { value } => map.serialize_entry(name, value)?,
                Value::Addresses { value } => map.serialize_entry(name, value)?,
                Value::AddressesList { value } => map.serialize_entry(name, value)?,
                Value::GroupedAddresses { value } => map.serialize_entry(name, value)?,
                Value::GroupedAddressesList { value } => map.serialize_entry(name, value)?,
                Value::Headers { value } => map.serialize_entry(name, value)?,
                Value::Null => map.serialize_entry(name, &None::<&str>)?,
            }
        }

        map.end()
    }
}
struct EmailVisitor;

impl<'de> serde::de::Visitor<'de> for EmailVisitor {
    type Value = Email;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP Email object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut properties: VecMap<Property, Value> = VecMap::new();

        while let Some(key) = map.next_key::<Cow<str>>()? {
            match key.as_ref() {
                "keywords" => {
                    if let Some(value) = map.next_value::<Option<VecMap<Keyword, bool>>>()? {
                        properties.append(Property::Keywords, Value::Keywords { value, set: true });
                    }
                }
                "mailboxIds" => {
                    if let Some(value) =
                        map.next_value::<Option<VecMap<MaybeIdReference, bool>>>()?
                    {
                        properties
                            .append(Property::MailboxIds, Value::MailboxIds { value, set: true });
                    }
                }
                "messageId" => {
                    if let Some(value) = map.next_value::<Option<Vec<String>>>()? {
                        properties.append(Property::MessageId, Value::TextList { value });
                    }
                }
                "inReplyTo" => {
                    if let Some(value) = map.next_value::<Option<Vec<String>>>()? {
                        properties.append(Property::InReplyTo, Value::TextList { value });
                    }
                }
                "references" => {
                    if let Some(value) = map.next_value::<Option<Vec<String>>>()? {
                        properties.append(Property::References, Value::TextList { value });
                    }
                }
                "sender" => {
                    if let Some(value) = map.next_value::<Option<Vec<EmailAddress>>>()? {
                        properties.append(Property::Sender, Value::Addresses { value });
                    }
                }
                "from" => {
                    if let Some(value) = map.next_value::<Option<Vec<EmailAddress>>>()? {
                        properties.append(Property::From, Value::Addresses { value });
                    }
                }
                "to" => {
                    if let Some(value) = map.next_value::<Option<Vec<EmailAddress>>>()? {
                        properties.append(Property::To, Value::Addresses { value });
                    }
                }
                "cc" => {
                    if let Some(value) = map.next_value::<Option<Vec<EmailAddress>>>()? {
                        properties.append(Property::Cc, Value::Addresses { value });
                    }
                }
                "bcc" => {
                    if let Some(value) = map.next_value::<Option<Vec<EmailAddress>>>()? {
                        properties.append(Property::Bcc, Value::Addresses { value });
                    }
                }
                "replyTo" => {
                    if let Some(value) = map.next_value::<Option<Vec<EmailAddress>>>()? {
                        properties.append(Property::ReplyTo, Value::Addresses { value });
                    }
                }
                "subject" => {
                    if let Some(value) = map.next_value::<Option<String>>()? {
                        properties.append(Property::Subject, Value::Text { value });
                    }
                }
                "sentAt" => {
                    if let Some(value) = map.next_value::<Option<JMAPDate>>()? {
                        properties.append(Property::SentAt, Value::Date { value });
                    }
                }
                "receivedAt" => {
                    if let Some(value) = map.next_value::<Option<JMAPDate>>()? {
                        properties.append(Property::ReceivedAt, Value::Date { value });
                    }
                }
                "preview" => {
                    if let Some(value) = map.next_value::<Option<String>>()? {
                        properties.append(Property::Preview, Value::Text { value });
                    }
                }
                "textBody" => {
                    properties.append(
                        Property::TextBody,
                        Value::BodyPartList {
                            value: map.next_value()?,
                        },
                    );
                }
                "htmlBody" => {
                    properties.append(
                        Property::HtmlBody,
                        Value::BodyPartList {
                            value: map.next_value()?,
                        },
                    );
                }
                "attachments" => {
                    properties.append(
                        Property::Attachments,
                        Value::BodyPartList {
                            value: map.next_value()?,
                        },
                    );
                }
                "hasAttachment" => {
                    if let Some(value) = map.next_value::<Option<bool>>()? {
                        properties.append(Property::HasAttachment, Value::Bool { value });
                    }
                }
                "id" => {
                    if let Some(value) = map.next_value::<Option<JMAPId>>()? {
                        properties.append(Property::Id, Value::Id { value });
                    }
                }
                "blobId" => {
                    if let Some(value) = map.next_value::<Option<JMAPBlob>>()? {
                        properties.append(Property::BlobId, Value::Blob { value });
                    }
                }
                "threadId" => {
                    if let Some(value) = map.next_value::<Option<JMAPId>>()? {
                        properties.append(Property::ThreadId, Value::Id { value });
                    }
                }
                "size" => {
                    if let Some(value) = map.next_value::<Option<usize>>()? {
                        properties.append(Property::Size, Value::Size { value });
                    }
                }
                "bodyValues" => {
                    properties.append(
                        Property::BodyValues,
                        Value::BodyValues {
                            value: map.next_value()?,
                        },
                    );
                }
                "bodyStructure" => {
                    properties.append(
                        Property::BodyStructure,
                        Value::BodyPart {
                            value: map.next_value()?,
                        },
                    );
                }
                "headers" => {
                    if let Some(value) = map.next_value::<Option<Vec<EmailHeader>>>()? {
                        properties.append(Property::Headers, Value::Headers { value });
                    }
                }
                _ if key.starts_with('#') => {
                    if let Some(property) = key.get(1..) {
                        properties.append(
                            Property::parse(property),
                            Value::ResultReference {
                                value: map.next_value()?,
                            },
                        );
                    } else {
                        map.next_value::<IgnoredAny>()?;
                    }
                }
                _ if key.starts_with("header:") => {
                    if let Some(header) = HeaderProperty::parse(key.as_ref()) {
                        let header_value = match header.form {
                            HeaderForm::Raw | HeaderForm::Text => {
                                if header.all {
                                    Value::TextList {
                                        value: map.next_value()?,
                                    }
                                } else {
                                    Value::Text {
                                        value: map.next_value()?,
                                    }
                                }
                            }
                            HeaderForm::Addresses => {
                                if header.all {
                                    Value::AddressesList {
                                        value: map.next_value()?,
                                    }
                                } else {
                                    Value::Addresses {
                                        value: map.next_value()?,
                                    }
                                }
                            }
                            HeaderForm::GroupedAddresses => {
                                if header.all {
                                    Value::GroupedAddressesList {
                                        value: map.next_value()?,
                                    }
                                } else {
                                    Value::GroupedAddresses {
                                        value: map.next_value()?,
                                    }
                                }
                            }
                            HeaderForm::MessageIds | HeaderForm::URLs => {
                                if header.all {
                                    Value::TextListMany {
                                        value: map.next_value()?,
                                    }
                                } else {
                                    Value::TextList {
                                        value: map.next_value()?,
                                    }
                                }
                            }
                            HeaderForm::Date => {
                                if header.all {
                                    Value::DateList {
                                        value: map.next_value()?,
                                    }
                                } else {
                                    Value::Date {
                                        value: map.next_value()?,
                                    }
                                }
                            }
                        };
                        properties.append(Property::Header(header), header_value);
                    } else {
                        map.next_value::<IgnoredAny>()?;
                    }
                }
                _ => {
                    if let Some(pointer) = JSONPointer::parse(key.as_ref()) {
                        match pointer {
                            JSONPointer::Path(path) if path.len() == 2 => {
                                if let (
                                    Some(JSONPointer::String(property)),
                                    Some(JSONPointer::String(id)),
                                ) = (path.get(0), path.get(1))
                                {
                                    let value = map.next_value::<Option<bool>>()?.unwrap_or(false);
                                    match Property::parse(property) {
                                        Property::MailboxIds => {
                                            if let Some(id) = JMAPId::parse(id) {
                                                properties
                                                    .get_mut_or_insert_with(
                                                        Property::MailboxIds,
                                                        || Value::MailboxIds {
                                                            value: VecMap::new(),
                                                            set: false,
                                                        },
                                                    )
                                                    .get_mailbox_ids()
                                                    .unwrap()
                                                    .append(MaybeIdReference::Value(id), value);
                                            }
                                        }
                                        Property::Keywords => {
                                            properties
                                                .get_mut_or_insert_with(Property::Keywords, || {
                                                    Value::Keywords {
                                                        value: VecMap::new(),
                                                        set: false,
                                                    }
                                                })
                                                .get_keywords()
                                                .unwrap()
                                                .append(Keyword::parse(id), value);
                                        }
                                        _ => {
                                            map.next_value::<IgnoredAny>()?;
                                        }
                                    }
                                }
                            }
                            _ => {
                                map.next_value::<IgnoredAny>()?;
                            }
                        }
                    } else {
                        map.next_value::<IgnoredAny>()?;
                    }
                }
            }
        }

        Ok(Email { properties })
    }
}

impl<'de> Deserialize<'de> for Email {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(EmailVisitor)
    }
}

// EmailBodyPart de/serialization
impl Serialize for EmailBodyPart {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(self.properties.len().into())?;

        for (name, value) in &self.properties {
            match value {
                Value::Id { value } => map.serialize_entry(name, value)?,
                Value::Blob { value } => map.serialize_entry(name, value)?,
                Value::Size { value } => map.serialize_entry(name, value)?,
                Value::Bool { value } => map.serialize_entry(name, value)?,
                Value::Keywords { value, .. } => map.serialize_entry(name, value)?,
                Value::MailboxIds { value, .. } => map.serialize_entry(name, value)?,
                Value::ResultReference { value } => map.serialize_entry(name, value)?,
                Value::BodyPart { value } => map.serialize_entry(name, value)?,
                Value::BodyPartList { value } => map.serialize_entry(name, value)?,
                Value::BodyValues { value } => map.serialize_entry(name, value)?,
                Value::Text { value } => map.serialize_entry(name, value)?,
                Value::TextList { value } => map.serialize_entry(name, value)?,
                Value::TextListMany { value } => map.serialize_entry(name, value)?,
                Value::Date { value } => map.serialize_entry(name, value)?,
                Value::DateList { value } => map.serialize_entry(name, value)?,
                Value::Addresses { value } => map.serialize_entry(name, value)?,
                Value::AddressesList { value } => map.serialize_entry(name, value)?,
                Value::GroupedAddresses { value } => map.serialize_entry(name, value)?,
                Value::GroupedAddressesList { value } => map.serialize_entry(name, value)?,
                Value::Headers { value } => map.serialize_entry(name, value)?,
                Value::Null => map.serialize_entry(name, &None::<&str>)?,
            }
        }

        map.end()
    }
}
struct EmailBodyPartVisitor;

impl<'de> serde::de::Visitor<'de> for EmailBodyPartVisitor {
    type Value = EmailBodyPart;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP EmailBodyPart object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut properties: VecMap<BodyProperty, Value> = VecMap::new();

        while let Some(key) = map.next_key::<Cow<str>>()? {
            match key.as_ref() {
                "partId" => {
                    if let Some(value) = map.next_value::<Option<String>>()? {
                        properties.append(BodyProperty::PartId, Value::Text { value });
                    }
                }
                "blobId" => {
                    if let Some(value) = map.next_value::<Option<JMAPBlob>>()? {
                        properties.append(BodyProperty::BlobId, Value::Blob { value });
                    }
                }
                "size" => {
                    if let Some(value) = map.next_value::<Option<usize>>()? {
                        properties.append(BodyProperty::Size, Value::Size { value });
                    }
                }
                "name" => {
                    if let Some(value) = map.next_value::<Option<String>>()? {
                        properties.append(BodyProperty::Name, Value::Text { value });
                    }
                }
                "type" => {
                    if let Some(value) = map.next_value::<Option<String>>()? {
                        properties.append(BodyProperty::Type, Value::Text { value });
                    }
                }
                "charset" => {
                    if let Some(value) = map.next_value::<Option<String>>()? {
                        properties.append(BodyProperty::Charset, Value::Text { value });
                    }
                }
                "disposition" => {
                    if let Some(value) = map.next_value::<Option<String>>()? {
                        properties.append(BodyProperty::Disposition, Value::Text { value });
                    }
                }
                "cid" => {
                    if let Some(value) = map.next_value::<Option<String>>()? {
                        properties.append(BodyProperty::Cid, Value::Text { value });
                    }
                }
                "language" => {
                    if let Some(value) = map.next_value::<Option<Vec<String>>>()? {
                        properties.append(BodyProperty::Language, Value::TextList { value });
                    }
                }
                "location" => {
                    if let Some(value) = map.next_value::<Option<String>>()? {
                        properties.append(BodyProperty::Location, Value::Text { value });
                    }
                }
                "subParts" => {
                    if let Some(value) = map.next_value::<Option<Vec<EmailBodyPart>>>()? {
                        properties.append(BodyProperty::Subparts, Value::BodyPartList { value });
                    }
                }
                "headers" => {
                    if let Some(value) = map.next_value::<Option<Vec<EmailHeader>>>()? {
                        properties.append(BodyProperty::Headers, Value::Headers { value });
                    }
                }
                _ if key.starts_with("header:") => {
                    if let Some(header) = HeaderProperty::parse(key.as_ref()) {
                        let header_value = match header.form {
                            HeaderForm::Raw | HeaderForm::Text => {
                                if header.all {
                                    Value::TextList {
                                        value: map.next_value()?,
                                    }
                                } else {
                                    Value::Text {
                                        value: map.next_value()?,
                                    }
                                }
                            }
                            HeaderForm::Addresses => {
                                if header.all {
                                    Value::AddressesList {
                                        value: map.next_value()?,
                                    }
                                } else {
                                    Value::Addresses {
                                        value: map.next_value()?,
                                    }
                                }
                            }
                            HeaderForm::GroupedAddresses => {
                                if header.all {
                                    Value::GroupedAddressesList {
                                        value: map.next_value()?,
                                    }
                                } else {
                                    Value::GroupedAddresses {
                                        value: map.next_value()?,
                                    }
                                }
                            }
                            HeaderForm::MessageIds | HeaderForm::URLs => {
                                if header.all {
                                    Value::TextListMany {
                                        value: map.next_value()?,
                                    }
                                } else {
                                    Value::TextList {
                                        value: map.next_value()?,
                                    }
                                }
                            }
                            HeaderForm::Date => {
                                if header.all {
                                    Value::DateList {
                                        value: map.next_value()?,
                                    }
                                } else {
                                    Value::Date {
                                        value: map.next_value()?,
                                    }
                                }
                            }
                        };
                        properties.append(BodyProperty::Header(header), header_value);
                    }
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        Ok(EmailBodyPart { properties })
    }
}

impl<'de> Deserialize<'de> for EmailBodyPart {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(EmailBodyPartVisitor)
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
        Ok(Property::parse(v))
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

// Argument serializers
impl ArgumentDeserializer for GetArguments {
    fn deserialize<'x: 'y, 'y, 'z>(
        &'y mut self,
        property: &'z str,
        value: &mut impl serde::de::MapAccess<'x>,
    ) -> Result<(), String> {
        match property {
            "bodyProperties" => {
                self.body_properties = value
                    .next_value::<Option<AHashSet<BodyProperty>>>()
                    .unwrap_or_default()
                    .map(|p| p.into_iter().collect());
            }
            "fetchTextBodyValues" => {
                self.fetch_text_body_values = value.next_value().unwrap_or_default();
            }
            "fetchHTMLBodyValues" => {
                self.fetch_html_body_values = value.next_value().unwrap_or_default();
            }
            "fetchAllBodyValues" => {
                self.fetch_all_body_values = value.next_value().unwrap_or_default();
            }
            "maxBodyValueBytes" => {
                self.max_body_value_bytes = value.next_value().unwrap_or_default();
            }
            _ => {
                value
                    .next_value::<IgnoredAny>()
                    .map_err(|err| err.to_string())?;
            }
        }
        Ok(())
    }
}

// Filter deserializer
impl FilterDeserializer for Filter {
    fn deserialize<'x>(property: &str, map: &mut impl serde::de::MapAccess<'x>) -> Option<Self> {
        match property {
            "inMailbox" => Filter::InMailbox {
                value: map.next_value().ok()?,
            },
            "inMailboxOtherThan" => Filter::InMailboxOtherThan {
                value: map.next_value().ok()?,
            },
            "before" => Filter::Before {
                value: map.next_value().ok()?,
            },
            "after" => Filter::After {
                value: map.next_value().ok()?,
            },
            "minSize" => Filter::MinSize {
                value: map.next_value().ok()?,
            },
            "maxSize" => Filter::MaxSize {
                value: map.next_value().ok()?,
            },
            "allInThreadHaveKeyword" => Filter::AllInThreadHaveKeyword {
                value: map.next_value().ok()?,
            },
            "someInThreadHaveKeyword" => Filter::SomeInThreadHaveKeyword {
                value: map.next_value().ok()?,
            },
            "noneInThreadHaveKeyword" => Filter::NoneInThreadHaveKeyword {
                value: map.next_value().ok()?,
            },
            "hasKeyword" => Filter::HasKeyword {
                value: map.next_value().ok()?,
            },
            "notKeyword" => Filter::NotKeyword {
                value: map.next_value().ok()?,
            },
            "hasAttachment" => Filter::HasAttachment {
                value: map.next_value().ok()?,
            },
            "text" => Filter::Text {
                value: map.next_value().ok()?,
            },
            "from" => Filter::From {
                value: map.next_value().ok()?,
            },
            "to" => Filter::To {
                value: map.next_value().ok()?,
            },
            "cc" => Filter::Cc {
                value: map.next_value().ok()?,
            },
            "bcc" => Filter::Bcc {
                value: map.next_value().ok()?,
            },
            "subject" => Filter::Subject {
                value: map.next_value().ok()?,
            },
            "body" => Filter::Body {
                value: map.next_value().ok()?,
            },
            "header" => Filter::Header {
                value: map.next_value().ok()?,
            },

            // Non-standard
            "id" => Filter::Id {
                value: map.next_value().ok()?,
            },
            "sentBefore" => Filter::SentBefore {
                value: map.next_value().ok()?,
            },
            "sentAfter" => Filter::SentAfter {
                value: map.next_value().ok()?,
            },
            "inThread" => Filter::InThread {
                value: map.next_value().ok()?,
            },

            unsupported => {
                map.next_value::<IgnoredAny>().ok()?;
                Filter::Unsupported {
                    value: unsupported.to_string(),
                }
            }
        }
        .into()
    }
}

// EmailImport Deserialize
struct EmailImportVisitor;

impl<'de> serde::de::Visitor<'de> for EmailImportVisitor {
    type Value = EmailImport;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP get request")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut request = EmailImport {
            blob_id: JMAPBlob::default(),
            mailbox_ids: None,
            keywords: None,
            received_at: None,
        };

        while let Some(key) = map.next_key::<Cow<str>>()? {
            match key.as_ref() {
                "blobId" => {
                    request.blob_id = map.next_value()?;
                }
                "keywords" => {
                    request.keywords = map.next_value()?;
                }
                "receivedAt" => {
                    request.received_at = map.next_value()?;
                }
                "mailboxIds" => {
                    request.mailbox_ids = if request.mailbox_ids.is_none() {
                        map.next_value::<Option<VecMap<MaybeIdReference, bool>>>()?
                            .map(MaybeResultReference::Value)
                    } else {
                        map.next_value::<IgnoredAny>()?;
                        MaybeResultReference::Error("Duplicate 'mailboxIds' property.".into())
                            .into()
                    };
                }
                "#mailboxIds" => {
                    request.mailbox_ids = if request.mailbox_ids.is_none() {
                        MaybeResultReference::Reference(map.next_value()?)
                    } else {
                        map.next_value::<IgnoredAny>()?;
                        MaybeResultReference::Error("Duplicate 'mailboxIds' property.".into())
                    }
                    .into();
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        Ok(request)
    }
}

impl<'de> Deserialize<'de> for EmailImport {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(EmailImportVisitor {})
    }
}

// SearchSnippetGetRequest Deserialize
struct SearchSnippetGetRequestVisitor;

impl<'de> serde::de::Visitor<'de> for SearchSnippetGetRequestVisitor {
    type Value = SearchSnippetGetRequest;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP get request")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut request = SearchSnippetGetRequest {
            acl: None,
            account_id: JMAPId::default(),
            filter: None,
            email_ids: MaybeResultReference::Error("Missing emailIds field.".into()),
        };

        while let Some(key) = map.next_key::<Cow<str>>()? {
            match key.as_ref() {
                "accountId" => {
                    request.account_id = map.next_value()?;
                }
                "filter" => {
                    request.filter = map.next_value()?;
                }
                "emailIds" => {
                    request.email_ids = if let MaybeResultReference::Error(_) = &request.email_ids {
                        MaybeResultReference::Value(map.next_value()?)
                    } else {
                        map.next_value::<IgnoredAny>()?;
                        MaybeResultReference::Error("Duplicate 'emailIds' property.".into())
                    };
                }
                "#emailIds" => {
                    request.email_ids = if let MaybeResultReference::Error(_) = &request.email_ids {
                        MaybeResultReference::Reference(map.next_value()?)
                    } else {
                        map.next_value::<IgnoredAny>()?;
                        MaybeResultReference::Error("Duplicate 'emailIds' property.".into())
                    };
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        Ok(request)
    }
}

impl<'de> Deserialize<'de> for SearchSnippetGetRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(SearchSnippetGetRequestVisitor {})
    }
}
