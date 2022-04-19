use std::collections::HashMap;

use store::AccountId;

use crate::{
    id::state::JMAPState,
    protocol::{json::JSONValue, response::Response},
};

#[derive(Debug, Clone)]
pub struct ImportRequest {
    pub account_id: AccountId,
    pub if_in_state: Option<JMAPState>,
    pub arguments: HashMap<String, JSONValue>,
}

impl ImportRequest {
    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = ImportRequest {
            account_id: 1, //TODO
            if_in_state: None,
            arguments: HashMap::new(),
        };

        invocation.parse_arguments(response, |name, value| {
            match name.as_str() {
                "accountId" => request.account_id = value.parse_document_id()?,
                "ifInState" => request.if_in_state = value.parse_jmap_state(true)?,
                _ => {
                    request.arguments.insert(name, value);
                }
            }
            Ok(())
        })?;

        Ok(request)
    }
}
