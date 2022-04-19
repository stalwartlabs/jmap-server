use std::collections::HashMap;

use store::AccountId;

use crate::{
    id::state::JMAPState,
    protocol::{json::JSONValue, response::Response},
};

#[derive(Debug, Clone)]
pub struct ChangesRequest {
    pub account_id: AccountId,
    pub since_state: JMAPState,
    pub max_changes: usize,
    pub arguments: HashMap<String, JSONValue>,
}

impl ChangesRequest {
    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = ChangesRequest {
            account_id: 1, //TODO
            since_state: JMAPState::Initial,
            max_changes: 0,
            arguments: HashMap::new(),
        };

        invocation.parse_arguments(response, |name, value| {
            match name.as_str() {
                "accountId" => request.account_id = value.parse_document_id()?,
                "sinceQueryState" => request.since_state = value.parse_jmap_state(false)?.unwrap(),
                "maxChanges" => {
                    request.max_changes = value.parse_unsigned_int(true)?.unwrap() as usize
                }
                _ => {
                    request.arguments.insert(name, value);
                }
            }
            Ok(())
        })?;

        Ok(request)
    }
}
