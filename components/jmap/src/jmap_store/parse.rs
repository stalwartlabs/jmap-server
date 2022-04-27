use std::collections::HashMap;

use store::{AccountId, JMAPId, JMAPStore, Store};

use crate::{
    error::method::MethodError,
    id::{blob::JMAPBlob, JMAPIdSerialize},
    protocol::json::JSONValue,
    request::parse::ParseRequest,
};

use super::blob::{InnerBlobFnc, JMAPBlobStore};

#[derive(Default)]
pub struct ParseResult {
    pub account_id: AccountId,
    pub parsed: HashMap<String, JSONValue>,
    pub not_parsable: Vec<JSONValue>,
    pub not_found: Vec<JSONValue>,
}

pub trait ParseObject<'y, T>: Sized
where
    T: for<'x> Store<'x> + 'static,
{
    fn new(store: &'y JMAPStore<T>, request: &mut ParseRequest) -> crate::Result<Self>;
    fn parse_blob(&self, blob_id: JMAPBlob, blob: Vec<u8>) -> crate::Result<Option<JSONValue>>;
    fn inner_blob_fnc() -> InnerBlobFnc;
}

pub trait JMAPParse<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn parse<'y, 'z: 'y, U>(&'z self, request: ParseRequest) -> crate::Result<ParseResult>
    where
        U: ParseObject<'y, T>;
}

impl<T> JMAPParse<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn parse<'y, 'z: 'y, U>(&'z self, mut request: ParseRequest) -> crate::Result<ParseResult>
    where
        U: ParseObject<'y, T>,
    {
        if request.blob_ids.len() > self.config.mail_parse_max_items {
            return Err(MethodError::RequestTooLarge);
        }
        let object = U::new(self, &mut request)?;

        let mut parsed = HashMap::new();
        let mut not_parsable = Vec::new();
        let mut not_found = Vec::new();

        for blob_id in request.blob_ids {
            let blob_id_str = blob_id.to_jmap_string();
            if let Some(blob) =
                self.download_blob(request.account_id, &blob_id, U::inner_blob_fnc())?
            {
                if let Some(result) = object.parse_blob(blob_id, blob)? {
                    parsed.insert(blob_id_str, result);
                } else {
                    not_parsable.push(blob_id_str.into());
                }
            } else {
                not_found.push(blob_id_str.into());
            }
        }

        Ok(ParseResult {
            account_id: request.account_id,
            parsed,
            not_parsable,
            not_found,
        })
    }
}

impl From<ParseResult> for JSONValue {
    fn from(parse_result: ParseResult) -> Self {
        let mut result = HashMap::with_capacity(4);
        result.insert(
            "accountId".to_string(),
            (parse_result.account_id as JMAPId).to_jmap_string().into(),
        );
        result.insert("parsed".to_string(), parse_result.parsed.into());
        result.insert("notParsable".to_string(), parse_result.not_parsable.into());
        result.insert("notFound".to_string(), parse_result.not_found.into());
        result.into()
    }
}
