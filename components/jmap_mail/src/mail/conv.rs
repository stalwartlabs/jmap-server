use mail_parser::{Addr, HeaderValue, RfcHeader, RfcHeaders};

use super::MimeHeaders;

impl TryFrom<mail_parser::Addr<'_>> for super::EmailAddress {
    type Error = ();

    fn try_from(value: mail_parser::Addr<'_>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.map(|s| s.into_owned()),
            email: value
                .address
                .and_then(|addr| if addr.contains('@') { Some(addr) } else { None })
                .ok_or_else(|| ())?
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
                    match item {
                        HeaderValue::DateTime(datetime) => {
                            result.push(super::HeaderValue::Timestamp(datetime.to_timestamp()));
                        }
                        _ => (),
                    }
                }
                result
            }
            _ => Vec::with_capacity(0),
        }
    }

    fn into_keyword(self) -> Vec<super::HeaderValue> {
        match self {
            HeaderValue::Text(text) => vec![super::HeaderValue::Keywords(vec![text.into_owned()])],
            HeaderValue::TextList(textlist) => vec![super::HeaderValue::Keywords(
                textlist.into_iter().map(|s| s.into_owned()).collect(),
            )],
            HeaderValue::Collection(list) => {
                let mut result = Vec::with_capacity(list.len());
                for item in list {
                    match item {
                        HeaderValue::Text(text) => {
                            result.push(super::HeaderValue::Keywords(vec![text.into_owned()]))
                        }
                        HeaderValue::TextList(textlist) => {
                            result.push(super::HeaderValue::Keywords(
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
            }) if addr.contains(':') => vec![super::HeaderValue::Urls(vec![addr.into_owned()])],
            HeaderValue::AddressList(addrlist) => {
                vec![super::HeaderValue::Urls(
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
                            result.push(super::HeaderValue::Urls(vec![addr.into_owned()]))
                        }
                        HeaderValue::AddressList(addrlist) => {
                            result.push(super::HeaderValue::Urls(
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

impl MimeHeaders {
    pub fn from_headers(rfc_headers: RfcHeaders, size: usize) -> Self {
        let mut headers = Self {
            type_: None,
            charset: None,
            name: None,
            disposition: None,
            location: None,
            language: None,
            cid: None,
            size,
        };

        for (header, value) in rfc_headers {
            headers.add_header(header, value);
        }

        headers
    }

    pub fn empty(is_html: bool, size: usize) -> Self {
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
    }

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
