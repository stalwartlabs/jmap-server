use std::collections::HashMap;

use store::{AccountId, JMAPId};

use crate::protocol::{json::JSONValue, response::Response};

#[derive(Debug, Clone)]
pub struct GetRequest {
    pub account_id: AccountId,
    pub ids: Option<Vec<JMAPId>>,
    pub properties: JSONValue,
    pub arguments: HashMap<String, JSONValue>,
}

impl GetRequest {
    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = GetRequest {
            account_id: 1, //TODO
            ids: None,
            properties: JSONValue::Null,
            arguments: HashMap::new(),
        };

        invocation.parse_arguments(response, |name, value| {
            match name.as_str() {
                "accountId" => request.account_id = value.parse_document_id()?,
                "ids" => request.ids = value.parse_array_items(true)?,
                "properties" => request.properties = value,
                _ => {
                    request.arguments.insert(name, value);
                }
            }
            Ok(())
        })?;

        Ok(request)
    }
}
