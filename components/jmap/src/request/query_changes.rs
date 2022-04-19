use std::collections::HashMap;

use store::AccountId;

use crate::{
    id::state::JMAPState,
    protocol::{json::JSONValue, response::Response},
};

use super::query::Comparator;

#[derive(Debug, Clone)]
pub struct QueryChangesRequest {
    pub account_id: AccountId,
    pub filter: JSONValue,
    pub sort: Option<Vec<Comparator>>,
    pub since_query_state: JMAPState,
    pub max_changes: usize,
    pub up_to_id: JSONValue,
    pub calculate_total: bool,
    pub arguments: HashMap<String, JSONValue>,
}

impl QueryChangesRequest {
    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = QueryChangesRequest {
            account_id: 1, //TODO
            filter: JSONValue::Null,
            sort: None,
            since_query_state: JMAPState::Initial,
            max_changes: 0,
            up_to_id: JSONValue::Null,
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
                "sinceQueryState" => {
                    request.since_query_state = value.parse_jmap_state(false)?.unwrap()
                }
                "maxChanges" => {
                    request.max_changes = value.parse_unsigned_int(true)?.unwrap() as usize
                }
                "upToId" => request.up_to_id = value,
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
