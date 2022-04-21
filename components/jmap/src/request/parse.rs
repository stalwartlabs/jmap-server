use std::collections::HashMap;

use store::AccountId;

use crate::{
    id::blob::BlobId,
    protocol::{json::JSONValue, response::Response},
};

#[derive(Debug, Clone)]
pub struct ParseRequest {
    pub account_id: AccountId,
    pub blob_ids: Vec<BlobId>,
    pub arguments: HashMap<String, JSONValue>,
}

impl ParseRequest {
    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = ParseRequest {
            account_id: 1, //TODO
            arguments: HashMap::new(),
            blob_ids: Vec::new(),
        };

        invocation.parse_arguments(response, |name, value| {
            match name.as_str() {
                "accountId" => request.account_id = value.parse_document_id()?,
                "blobIds" => request.blob_ids = value.parse_array_items::<BlobId>(false)?.unwrap(),
                _ => {
                    request.arguments.insert(name, value);
                }
            }
            Ok(())
        })?;

        Ok(request)
    }
}
