use std::borrow::Cow;

use jmap::from_timestamp;
use mail_parser::{
    parsers::{
        fields::{
            address::parse_address, date::parse_date, id::parse_id,
            unstructured::parse_unstructured,
        },
        message::MessageStream,
    },
    Addr, HeaderOffset, HeaderValue, RfcHeader, RfcHeaders,
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
    fn into_address(self) -> Vec<super::HeaderValue>;
    fn into_text(self) -> Vec<super::HeaderValue>;
    fn into_keyword(self) -> Vec<super::HeaderValue>;
    fn into_date(self) -> Vec<super::HeaderValue>;
    fn into_url(self) -> Vec<super::HeaderValue>;
}

impl HeaderValueInto for mail_parser::HeaderValue<'_> {
    fn into_text(self) -> Vec<super::HeaderValue> {
        match self {
            HeaderValue::Text(text) => vec![super::HeaderValue::Text(text.into_owned())],
            HeaderValue::TextList(textlist) => vec![super::HeaderValue::Text(textlist.join(", "))],
            HeaderValue::Collection(list) => {
                let mut result = Vec::with_capacity(list.len());
                for item in list {
                    match item {
                        HeaderValue::Text(text) => {
                            result.push(super::HeaderValue::Text(text.into_owned()))
                        }
                        HeaderValue::TextList(textlist) => {
                            result.push(super::HeaderValue::Text(textlist.join(", ")))
                        }
                        _ => (),
                    }
                }
                result
            }
            _ => Vec::with_capacity(0),
        }
    }

    fn into_date(self) -> Vec<super::HeaderValue> {
        match self {
            HeaderValue::DateTime(datetime) => {
                vec![super::HeaderValue::Timestamp(datetime.to_timestamp())]
            }
            HeaderValue::Collection(list) => {
                let mut result = Vec::with_capacity(list.len());
                for item in list {
                    if let HeaderValue::DateTime(datetime) = item {
                        result.push(super::HeaderValue::Timestamp(datetime.to_timestamp()));
                    }
                }
                result
            }
            _ => Vec::with_capacity(0),
        }
    }

    fn into_keyword(self) -> Vec<super::HeaderValue> {
        match self {
            HeaderValue::Text(text) => vec![super::HeaderValue::TextList(vec![text.into_owned()])],
            HeaderValue::TextList(textlist) => vec![super::HeaderValue::TextList(
                textlist.into_iter().map(|s| s.into_owned()).collect(),
            )],
            HeaderValue::Collection(list) => {
                let mut result = Vec::with_capacity(list.len());
                for item in list {
                    match item {
                        HeaderValue::Text(text) => {
                            result.push(super::HeaderValue::TextList(vec![text.into_owned()]))
                        }
                        HeaderValue::TextList(textlist) => {
                            result.push(super::HeaderValue::TextList(
                                textlist.into_iter().map(|s| s.into_owned()).collect(),
                            ))
                        }
                        _ => (),
                    }
                }
                result
            }
            _ => Vec::with_capacity(0),
        }
    }

    fn into_url(self) -> Vec<super::HeaderValue> {
        match self {
            HeaderValue::Address(Addr {
                address: Some(addr),
                ..
            }) if addr.contains(':') => vec![super::HeaderValue::TextList(vec![addr.into_owned()])],
            HeaderValue::AddressList(addrlist) => {
                vec![super::HeaderValue::TextList(
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
                )]
            }
            HeaderValue::Collection(list) => {
                let mut result = Vec::with_capacity(list.len());
                for item in list {
                    match item {
                        HeaderValue::Address(Addr {
                            address: Some(addr),
                            ..
                        }) if addr.contains(':') => {
                            result.push(super::HeaderValue::TextList(vec![addr.into_owned()]))
                        }
                        HeaderValue::AddressList(addrlist) => {
                            result.push(super::HeaderValue::TextList(
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
                            ));
                        }
                        _ => (),
                    }
                }

                result
            }
            _ => Vec::with_capacity(0),
        }
    }

    fn into_address(self) -> Vec<super::HeaderValue> {
        match self {
            HeaderValue::Address(addr) => {
                if let Ok(addr) = addr.try_into() {
                    vec![super::HeaderValue::Addresses(vec![addr])]
                } else {
                    Vec::with_capacity(0)
                }
            }
            HeaderValue::AddressList(addrlist) => {
                vec![super::HeaderValue::Addresses(
                    addrlist
                        .into_iter()
                        .filter_map(|addr| addr.try_into().ok())
                        .collect(),
                )]
            }
            HeaderValue::Group(group) => {
                if let Ok(group) = group.try_into() {
                    vec![super::HeaderValue::GroupedAddresses(vec![group])]
                } else {
                    Vec::with_capacity(0)
                }
            }
            HeaderValue::GroupList(grouplist) => {
                vec![super::HeaderValue::GroupedAddresses(
                    grouplist
                        .into_iter()
                        .filter_map(|addr| addr.try_into().ok())
                        .collect(),
                )]
            }
            HeaderValue::Collection(list) => {
                let convert_to_group = list
                    .iter()
                    .any(|item| matches!(item, HeaderValue::Group(_) | HeaderValue::GroupList(_)));

                let mut result = Vec::with_capacity(list.len());
                for item in list {
                    match item {
                        HeaderValue::Address(addr) => {
                            if convert_to_group {
                                if let Ok(group) = addr.try_into() {
                                    result.push(super::HeaderValue::GroupedAddresses(vec![group]));
                                }
                            } else if let Ok(addr) = addr.try_into() {
                                result.push(super::HeaderValue::Addresses(vec![addr]));
                            }
                        }
                        HeaderValue::AddressList(addrlist) => {
                            if convert_to_group {
                                if let Ok(group) = addrlist.try_into() {
                                    result.push(super::HeaderValue::GroupedAddresses(vec![group]));
                                }
                            } else {
                                result.push(super::HeaderValue::Addresses(
                                    addrlist
                                        .into_iter()
                                        .filter_map(|addr| addr.try_into().ok())
                                        .collect(),
                                ));
                            }
                        }
                        HeaderValue::Group(group) => {
                            if let Ok(group) = group.try_into() {
                                result.push(super::HeaderValue::GroupedAddresses(vec![group]));
                            }
                        }
                        HeaderValue::GroupList(grouplist) => {
                            result.push(super::HeaderValue::GroupedAddresses(
                                grouplist
                                    .into_iter()
                                    .filter_map(|addr| addr.try_into().ok())
                                    .collect(),
                            ));
                        }
                        _ => (),
                    }
                }
                result
            }
            _ => Vec::with_capacity(0),
        }
    }
}

impl MimePart {
    pub fn from_headers(
        rfc_headers: RfcHeaders,
        raw_headers: Vec<(HeaderName, HeaderOffset)>,
        mime_type: MimePartType,
        is_encoding_problem: bool,
        size: usize,
    ) -> Self {
        let mut headers = Self {
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
            raw_headers,
        };

        for (header, value) in rfc_headers {
            headers.add_header(header, value);
        }

        headers
    }

    /*pub fn empty(is_html: bool, size: usize) -> Self {
        Self {
            type_: if is_html {
                "text/html".to_string()
            } else {
                "text/plain".to_string()
            }
            .into(),
            charset: None,
            name: None,
            disposition: None,
            location: None,
            language: None,
            cid: None,
            size,
        }
    }*/

    pub fn add_header(&mut self, header: RfcHeader, value: HeaderValue) {
        match header {
            RfcHeader::ContentType => {
                if let HeaderValue::ContentType(content_type) = value {
                    if let Some(mut attributes) = content_type.attributes {
                        if content_type.c_type == "text" {
                            if let Some(charset) = attributes.remove("charset") {
                                self.charset = charset.into_owned().into();
                            }
                        }
                        if let (Some(name), None) = (attributes.remove("name"), &self.name) {
                            self.name = name.into_owned().into();
                        }
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
                    self.disposition = content_disposition.c_type.into_owned().into();

                    if let Some(name) = content_disposition
                        .attributes
                        .as_mut()
                        .and_then(|attrs| attrs.remove("filename"))
                    {
                        self.name = name.into_owned().into();
                    }
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

impl IntoForm for mail_parser::HeaderValue<'_> {
    fn into_form(self, form: &HeaderForm, all: bool) -> Option<Value> {
        match form {
            HeaderForm::Raw | HeaderForm::Text => self.into_text(),
            HeaderForm::URLs => self.into_url(),
            HeaderForm::MessageIds => self.into_keyword(),
            HeaderForm::Addresses | HeaderForm::GroupedAddresses => self.into_address(),
            HeaderForm::Date => self.into_date(),
        }
        .into_form(form, all)
    }
}

impl IntoForm for super::HeaderValue {
    fn into_form(self, form: &HeaderForm, _all: bool) -> Option<Value> {
        match (form, self) {
            (HeaderForm::Raw | HeaderForm::Text, super::HeaderValue::Text(value)) => {
                Value::Text { value }.into()
            }
            (HeaderForm::MessageIds | HeaderForm::URLs, super::HeaderValue::TextList(value)) => {
                Value::TextList { value }.into()
            }
            (HeaderForm::Date, super::HeaderValue::Timestamp(ts)) => Value::Date {
                value: from_timestamp(ts),
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
    }
}

impl IntoForm for Vec<super::HeaderValue> {
    fn into_form(mut self, form: &HeaderForm, all: bool) -> Option<Value> {
        if !all {
            return self.pop()?.into_form(form, false);
        }

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
                    .filter_map(|v| v.into_timestamp().map(from_timestamp))
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

impl MessageData {
    pub fn header(&mut self, header: &RfcHeader, form: &HeaderForm, all: bool) -> Option<Value> {
        if let Some(header) = self.headers.remove(header) {
            header.into_form(form, all)
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
        offsets: &[&HeaderOffset],
        raw_message: &'x [u8],
        all: bool,
    ) -> HeaderValue<'x> {
        let mut header_values: Vec<HeaderValue> = offsets
            .iter()
            .skip(if !all && offsets.len() > 1 {
                offsets.len() - 1
            } else {
                0
            })
            .map(|offset| {
                (raw_message
                    .get(offset.start..offset.end)
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
            .collect();

        if all {
            HeaderValue::Collection(header_values)
        } else {
            header_values.pop().unwrap_or_default()
        }
    }
}
