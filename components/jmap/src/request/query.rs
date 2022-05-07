use std::collections::HashMap;

use store::{AccountId, JMAPId};

use crate::{
    protocol::{json::JSONValue, response::Response},
    MethodError,
};

#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub account_id: AccountId,
    pub filter: JSONValue,
    pub sort: Option<Vec<Comparator>>,
    pub position: i64,
    pub anchor: Option<JMAPId>,
    pub anchor_offset: i64,
    pub limit: usize,
    pub calculate_total: bool,
    pub arguments: HashMap<String, JSONValue>,
}

#[derive(Debug, Clone)]
pub struct Comparator {
    pub property: String,
    pub is_ascending: bool,
    pub collation: Option<String>,
    pub arguments: HashMap<String, JSONValue>,
}

impl QueryRequest {
    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = QueryRequest {
            account_id: AccountId::MAX,
            filter: JSONValue::Null,
            sort: None,
            position: 0,
            anchor: None,
            anchor_offset: 0,
            limit: 0,
            calculate_total: false,
            arguments: HashMap::new(),
        };

        invocation.parse_arguments(response, |name, value| {
            match name.as_str() {
                "accountId" => request.account_id = value.parse_document_id()?,
                "filter" => request.filter = value,
                "sort" => {
                    if let JSONValue::Array(sort) = value {
                        let mut result = Vec::with_capacity(sort.len());
                        for comparator in sort {
                            result.push(comparator.parse_comparator()?);
                        }
                        request.sort = Some(result);
                    }
                }
                "position" => request.position = value.parse_int(false)?.unwrap(),
                "anchor" => request.anchor = value.parse_jmap_id(true)?,
                "anchorOffset" => request.anchor_offset = value.parse_int(false)?.unwrap(),
                "limit" => request.limit = value.parse_unsigned_int(false)?.unwrap() as usize,
                "calculateTotal" => request.calculate_total = value.parse_bool()?,
                _ => {
                    request.arguments.insert(name, value);
                }
            }
            Ok(())
        })?;

        Ok(request)
    }
}

impl JSONValue {
    pub fn parse_comparator(self) -> crate::Result<Comparator> {
        let mut comparator = self.unwrap_object().ok_or_else(|| {
            MethodError::InvalidArguments("Comparator is not an object.".to_string())
        })?;

        Ok(Comparator {
            property: comparator
                .remove("property")
                .and_then(|v| v.unwrap_string())
                .ok_or_else(|| {
                    MethodError::InvalidArguments(
                        "Comparator has no 'property' parameter.".to_string(),
                    )
                })?,
            is_ascending: comparator
                .remove("isAscending")
                .and_then(|v| v.unwrap_bool())
                .unwrap_or(true),
            collation: comparator
                .remove("collation")
                .and_then(|v| v.unwrap_string()),
            arguments: comparator,
        })
    }
}
