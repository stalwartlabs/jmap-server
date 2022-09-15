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

use std::borrow::Cow;

use jmap::types::date::JMAPDate;
use mail_parser::{
    parsers::{
        fields::{
            address::parse_address, date::parse_date, id::parse_id,
            unstructured::parse_unstructured,
        },
        message::MessageStream,
    },
    Addr, Header, HeaderValue, RfcHeader,
};

use super::{
    schema::{HeaderForm, Value},
    HeaderName, MessageData, MimePart, MimePartType,
};

impl TryFrom<mail_parser::Addr<'_>> for super::EmailAddress {
    type Error = ();

    fn try_from(value: mail_parser::Addr<'_>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.map(|s| s.into_owned()),
            email: value
                .address
                .and_then(|addr| if addr.contains('@') { Some(addr) } else { None })
                .ok_or(())?
                .into_owned(),
        })
    }
}

impl TryFrom<mail_parser::Addr<'_>> for super::EmailAddressGroup {
    type Error = ();

    fn try_from(value: mail_parser::Addr<'_>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: None,
            addresses: vec![value.try_into()?],
        })
    }
}

impl TryFrom<mail_parser::Group<'_>> for super::EmailAddressGroup {
    type Error = ();

    fn try_from(value: mail_parser::Group<'_>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.map(|s| s.into_owned()),
            addresses: value
                .addresses
                .into_iter()
                .filter_map(|addr| addr.try_into().ok())
                .collect(),
        })
    }
}

impl TryFrom<Vec<mail_parser::Addr<'_>>> for super::EmailAddressGroup {
    type Error = ();

    fn try_from(value: Vec<mail_parser::Addr<'_>>) -> Result<Self, Self::Error> {
        let group = Self {
            name: None,
            addresses: value
                .into_iter()
                .filter_map(|addr| addr.try_into().ok())
                .collect(),
        };
        if !group.addresses.is_empty() {
            Ok(group)
        } else {
            Err(())
        }
    }
}

impl From<super::EmailAddress> for mail_builder::headers::address::Address<'_> {
    fn from(addr: super::EmailAddress) -> Self {
        mail_builder::headers::address::Address::Address(
            mail_builder::headers::address::EmailAddress {
                name: addr.name.map(|name| name.into()),
                email: addr.email.into(),
            },
        )
    }
}

impl<'x> From<&'x super::EmailAddress> for mail_builder::headers::address::Address<'x> {
    fn from(addr: &'x super::EmailAddress) -> Self {
        mail_builder::headers::address::Address::Address(
            mail_builder::headers::address::EmailAddress {
                name: addr.name.as_ref().map(|name| name.into()),
                email: Cow::from(&addr.email),
            },
        )
    }
}

impl From<super::EmailAddressGroup> for mail_builder::headers::address::Address<'_> {
    fn from(addr: super::EmailAddressGroup) -> Self {
        mail_builder::headers::address::Address::Group(
            mail_builder::headers::address::GroupedAddresses {
                name: addr.name.map(|name| name.into()),
                addresses: addr.addresses.into_iter().map(Into::into).collect(),
            },
        )
    }
}

impl<'x> From<&'x super::EmailAddressGroup> for mail_builder::headers::address::Address<'x> {
    fn from(addr: &'x super::EmailAddressGroup) -> Self {
        mail_builder::headers::address::Address::Group(
            mail_builder::headers::address::GroupedAddresses {
                name: addr.name.as_ref().map(|name| name.into()),
                addresses: addr.addresses.iter().map(Into::into).collect(),
            },
        )
    }
}

impl From<Vec<super::EmailAddress>> for super::EmailAddressGroup {
    fn from(addresses: Vec<super::EmailAddress>) -> Self {
        super::EmailAddressGroup {
            name: None,
            addresses,
        }
    }
}

impl From<super::EmailAddressGroup> for Vec<super::EmailAddress> {
    fn from(group: super::EmailAddressGroup) -> Self {
        group.addresses
    }
}

pub trait HeaderValueInto {
    fn into_address(self) -> Option<super::HeaderValue>;
    fn into_text(self) -> Option<super::HeaderValue>;
    fn into_keyword(self) -> Option<super::HeaderValue>;
    fn into_date(self) -> Option<super::HeaderValue>;
    fn into_url(self) -> Option<super::HeaderValue>;
}

impl HeaderValueInto for mail_parser::HeaderValue<'_> {
    fn into_text(self) -> Option<super::HeaderValue> {
        match self {
            HeaderValue::Text(text) => super::HeaderValue::Text(text.into_owned()).into(),
            HeaderValue::TextList(textlist) => super::HeaderValue::Text(textlist.join(", ")).into(),
            _ => None,
        }
    }

    fn into_date(self) -> Option<super::HeaderValue> {
        match self {
            HeaderValue::DateTime(datetime) => {
                super::HeaderValue::Timestamp(datetime.to_timestamp()).into()
            }
            _ => None,
        }
    }

    fn into_keyword(self) -> Option<super::HeaderValue> {
        match self {
            HeaderValue::Text(text) => super::HeaderValue::TextList(vec![text.into_owned()]).into(),
            HeaderValue::TextList(textlist) => {
                super::HeaderValue::TextList(textlist.into_iter().map(|s| s.into_owned()).collect())
                    .into()
            }
            _ => None,
        }
    }

    fn into_url(self) -> Option<super::HeaderValue> {
        match self {
            HeaderValue::Address(Addr {
                address: Some(addr),
                ..
            }) if addr.contains(':') => {
                super::HeaderValue::TextList(vec![addr.into_owned()]).into()
            }
            HeaderValue::AddressList(addrlist) => super::HeaderValue::TextList(
                addrlist
                    .into_iter()
                    .filter_map(|addr| match addr {
                        Addr {
                            address: Some(addr),
                            ..
                        } if addr.contains(':') => Some(addr.into_owned()),
                        _ => None,
                    })
                    .collect(),
            )
            .into(),
            _ => None,
        }
    }

    fn into_address(self) -> Option<super::HeaderValue> {
        match self {
            HeaderValue::Address(addr) => {
                if let Ok(addr) = addr.try_into() {
                    super::HeaderValue::Addresses(vec![addr]).into()
                } else {
                    None
                }
            }
            HeaderValue::AddressList(addrlist) => super::HeaderValue::Addresses(
                addrlist
                    .into_iter()
                    .filter_map(|addr| addr.try_into().ok())
                    .collect(),
            )
            .into(),
            HeaderValue::Group(group) => {
                if let Ok(group) = group.try_into() {
                    super::HeaderValue::GroupedAddresses(vec![group]).into()
                } else {
                    None
                }
            }
            HeaderValue::GroupList(grouplist) => super::HeaderValue::GroupedAddresses(
                grouplist
                    .into_iter()
                    .filter_map(|addr| addr.try_into().ok())
                    .collect(),
            )
            .into(),
            _ => None,
        }
    }
}

impl MimePart {
    pub fn from_headers(
        headers: Vec<Header>,
        mime_type: MimePartType,
        is_encoding_problem: bool,
        size: usize,
    ) -> Self {
        let mut mime_part = Self {
            type_: None,
            charset: None,
            name: None,
            disposition: None,
            location: None,
            language: None,
            cid: None,
            size,
            mime_type,
            is_encoding_problem,
            raw_headers: Vec::with_capacity(headers.len()),
        };

        for header in headers {
            let header_name = match header.name {
                mail_parser::HeaderName::Rfc(header_name) => {
                    mime_part.add_header(header_name, header.value);
                    HeaderName::Rfc(header_name)
                }
                mail_parser::HeaderName::Other(header_name) => {
                    HeaderName::Other(header_name.into_owned())
                }
            };
            mime_part
                .raw_headers
                .push((header_name, header.offset_start, header.offset_end));
        }

        mime_part
    }

    pub fn add_header(&mut self, header: RfcHeader, value: HeaderValue) {
        match header {
            RfcHeader::ContentType => {
                if let HeaderValue::ContentType(mut content_type) = value {
                    if &content_type.c_type == "text" {
                        if let Some(charset) = content_type.remove_attribute("charset") {
                            self.charset = charset.into_owned().into();
                        }
                    }
                    if let (Some(name), None) = (content_type.remove_attribute("name"), &self.name)
                    {
                        self.name = name.into_owned().into();
                    }
                    self.type_ = if let Some(subtype) = content_type.c_subtype {
                        format!("{}/{}", content_type.c_type, subtype)
                    } else {
                        content_type.c_type.into_owned()
                    }
                    .into();
                }
            }
            RfcHeader::ContentDisposition => {
                if let HeaderValue::ContentType(mut content_disposition) = value {
                    if let Some(name) = content_disposition.remove_attribute("filename") {
                        self.name = name.into_owned().into();
                    }
                    self.disposition = content_disposition.c_type.into_owned().into();
                }
            }
            RfcHeader::ContentId => match value {
                HeaderValue::Text(id) => {
                    self.cid = id.into_owned().into();
                }
                HeaderValue::TextList(mut ids) if !ids.is_empty() => {
                    self.cid = ids.pop().unwrap().into_owned().into();
                }
                _ => {}
            },
            RfcHeader::ContentLanguage => match value {
                HeaderValue::Text(id) => {
                    self.language = vec![id.into_owned()].into();
                }
                HeaderValue::TextList(ids) => {
                    self.language = ids
                        .into_iter()
                        .map(|id| id.into_owned())
                        .collect::<Vec<_>>()
                        .into();
                }
                _ => {}
            },
            RfcHeader::ContentLocation => match value {
                HeaderValue::Text(id) => {
                    self.location = id.into_owned().into();
                }
                HeaderValue::TextList(mut ids) => {
                    self.cid = ids.pop().unwrap().into_owned().into();
                }
                _ => {}
            },
            _ => {}
        }
    }
}

impl super::HeaderValue {
    pub fn into_timestamp(self) -> Option<i64> {
        match self {
            super::HeaderValue::Timestamp(ts) => Some(ts),
            _ => None,
        }
    }

    pub fn into_text(self) -> Option<String> {
        match self {
            super::HeaderValue::Text(text) => Some(text),
            _ => None,
        }
    }

    pub fn into_text_list(self) -> Option<Vec<String>> {
        match self {
            super::HeaderValue::TextList(textlist) => Some(textlist),
            _ => None,
        }
    }

    pub fn into_addresses(self) -> Option<Vec<super::EmailAddress>> {
        match self {
            super::HeaderValue::Addresses(addrs) => Some(addrs),
            super::HeaderValue::GroupedAddresses(group) => {
                Some(group.into_iter().flat_map(|g| g.addresses).collect())
            }
            _ => None,
        }
    }

    pub fn into_grouped_addresses(self) -> Option<Vec<super::EmailAddressGroup>> {
        match self {
            super::HeaderValue::GroupedAddresses(addrs) => Some(addrs),
            super::HeaderValue::Addresses(addrs) => Some(vec![addrs.into()]),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            super::HeaderValue::Text(text) => Some(text.as_str()),
            _ => None,
        }
    }
}

pub trait IntoForm {
    fn into_form(self, form: &HeaderForm, all: bool) -> Option<Value>;
}

impl IntoForm for Vec<mail_parser::HeaderValue<'_>> {
    fn into_form(self, form: &HeaderForm, all: bool) -> Option<Value> {
        self.into_iter()
            .filter_map(|value| match form {
                HeaderForm::Raw | HeaderForm::Text => value.into_text(),
                HeaderForm::URLs => value.into_url(),
                HeaderForm::MessageIds => value.into_keyword(),
                HeaderForm::Addresses | HeaderForm::GroupedAddresses => value.into_address(),
                HeaderForm::Date => value.into_date(),
            })
            .collect::<Vec<_>>()
            .into_form(form, all)
    }
}

impl IntoForm for Vec<super::HeaderValue> {
    fn into_form(mut self, form: &HeaderForm, all: bool) -> Option<Value> {
        if !all {
            match (form, self.pop()?) {
                (HeaderForm::Raw | HeaderForm::Text, super::HeaderValue::Text(value)) => {
                    Value::Text { value }.into()
                }
                (
                    HeaderForm::MessageIds | HeaderForm::URLs,
                    super::HeaderValue::TextList(value),
                ) => Value::TextList { value }.into(),
                (HeaderForm::Date, super::HeaderValue::Timestamp(ts)) => Value::Date {
                    value: JMAPDate::from_timestamp(ts),
                }
                .into(),
                (HeaderForm::Addresses, super::HeaderValue::Addresses(value)) => {
                    Value::Addresses { value }.into()
                }
                (HeaderForm::Addresses, super::HeaderValue::GroupedAddresses(value)) => {
                    Value::Addresses {
                        value: value.into_iter().flat_map(|g| g.addresses).collect(),
                    }
                    .into()
                }
                (HeaderForm::GroupedAddresses, super::HeaderValue::GroupedAddresses(value)) => {
                    Value::GroupedAddresses { value }.into()
                }
                (HeaderForm::GroupedAddresses, super::HeaderValue::Addresses(value)) => {
                    Value::GroupedAddresses {
                        value: vec![value.into()],
                    }
                    .into()
                }
                _ => None,
            }
        } else {
            match form {
                HeaderForm::Raw | HeaderForm::Text => Value::TextList {
                    value: self.into_iter().filter_map(|v| v.into_text()).collect(),
                }
                .into(),
                HeaderForm::MessageIds | HeaderForm::URLs => Value::TextListMany {
                    value: self
                        .into_iter()
                        .filter_map(|v| v.into_text_list())
                        .collect(),
                }
                .into(),
                HeaderForm::Date => Value::DateList {
                    value: self
                        .into_iter()
                        .filter_map(|v| v.into_timestamp().map(JMAPDate::from_timestamp))
                        .collect(),
                }
                .into(),
                HeaderForm::Addresses => Value::AddressesList {
                    value: self
                        .into_iter()
                        .filter_map(|v| v.into_addresses())
                        .collect(),
                }
                .into(),
                HeaderForm::GroupedAddresses => Value::GroupedAddressesList {
                    value: self
                        .into_iter()
                        .filter_map(|v| v.into_grouped_addresses())
                        .collect(),
                }
                .into(),
            }
        }
    }
}

impl MessageData {
    pub fn header(&mut self, header: &RfcHeader, form: &HeaderForm, all: bool) -> Option<Value> {
        if let Some(values) = self.headers.remove(header) {
            values.into_form(form, all)
        } else if all {
            Value::TextList { value: Vec::new() }.into()
        } else {
            None
        }
    }
}

impl HeaderForm {
    pub fn parse_offsets<'x>(
        &self,
        offsets: &[(usize, usize)],
        raw_message: &'x [u8],
        all: bool,
    ) -> Vec<HeaderValue<'x>> {
        offsets
            .iter()
            .skip(if !all && offsets.len() > 1 {
                offsets.len() - 1
            } else {
                0
            })
            .map(|(start, end)| {
                (raw_message
                    .get(*start..*end)
                    .map_or(HeaderValue::Empty, |bytes| match self {
                        HeaderForm::Raw => {
                            HeaderValue::Text(std::str::from_utf8(bytes).map_or_else(
                                |_| String::from_utf8_lossy(bytes).trim_end().to_string().into(),
                                |str| str.trim_end().to_string().into(),
                            ))
                        }
                        HeaderForm::Text => parse_unstructured(&mut MessageStream::new(bytes)),
                        HeaderForm::Addresses => parse_address(&mut MessageStream::new(bytes)),
                        HeaderForm::GroupedAddresses => {
                            parse_address(&mut MessageStream::new(bytes))
                        }
                        HeaderForm::MessageIds => parse_id(&mut MessageStream::new(bytes)),
                        HeaderForm::Date => parse_date(&mut MessageStream::new(bytes)),
                        HeaderForm::URLs => parse_address(&mut MessageStream::new(bytes)),
                    }))
                .into_owned()
            })
            .collect()
    }
}
