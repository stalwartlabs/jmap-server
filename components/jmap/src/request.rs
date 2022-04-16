use std::collections::HashMap;

use serde::Serialize;
use store::{chrono::DateTime, AccountId, DocumentId, JMAPId};

use crate::{
    changes::JMAPState,
    id::{BlobId, JMAPIdSerialize},
    json::JSONValue,
    query::Comparator,
    JMAPError,
};

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
pub struct Request {
    pub using: Vec<String>,
    #[serde(rename(deserialize = "methodCalls"))]
    pub method_calls: Vec<(String, JSONValue, String)>,
    #[serde(rename(deserialize = "createdIds"))]
    pub created_ids: Option<HashMap<String, String>>,
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub struct Response {
    #[serde(rename(serialize = "methodResponses"))]
    pub method_responses: Vec<(String, JSONValue, String)>,
    #[serde(rename(serialize = "sessionState"))]
    #[serde(serialize_with = "serialize_hex")]
    pub session_state: u64,
}

#[derive(Debug, Clone)]
pub struct SetRequest {
    pub account_id: AccountId,
    pub if_in_state: Option<JMAPState>,
    pub create: JSONValue,
    pub update: JSONValue,
    pub destroy: JSONValue,
    pub arguments: HashMap<String, JSONValue>,
}

#[derive(Debug, Clone)]
pub struct GetRequest {
    pub account_id: AccountId,
    pub ids: Option<Vec<JMAPId>>,
    pub properties: JSONValue,
    pub arguments: HashMap<String, JSONValue>,
}

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
pub struct ChangesRequest {
    pub account_id: AccountId,
    pub since_state: JMAPState,
    pub max_changes: usize,
    pub arguments: HashMap<String, JSONValue>,
}

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

#[derive(Debug, Clone)]
pub struct ImportRequest {
    pub account_id: AccountId,
    pub if_in_state: Option<JMAPState>,
    pub arguments: HashMap<String, JSONValue>,
}

#[derive(Debug, Clone)]
pub struct ParseRequest {
    pub account_id: AccountId,
    pub arguments: HashMap<String, JSONValue>,
}

#[derive(Debug)]
pub enum Object {
    Core,
    Mailbox,
    Thread,
    Email,
    SearchSnippet,
    Identity,
    EmailSubmission,
    VacationResponse,
    PushSubscription,
}

pub enum Method {
    Echo(JSONValue),
    Get(GetRequest),
    Set(SetRequest),
    Query(QueryRequest),
    QueryChanges(QueryChangesRequest),
    Changes(ChangesRequest),
    Import(ImportRequest),
    Parse(ParseRequest),
}

pub struct Invocation {
    pub obj: Object,
    pub call: Method,
    pub account_id: AccountId,
}

pub trait JSONArgumentParser: Sized {
    fn parse_argument(argument: JSONValue) -> crate::Result<Self>;
}

impl JSONArgumentParser for JMAPId {
    fn parse_argument(argument: JSONValue) -> crate::Result<Self> {
        argument
            .to_jmap_id()
            .ok_or_else(|| JMAPError::InvalidArguments("Failed to parse JMAP Id.".to_string()))
    }
}

impl JSONArgumentParser for DocumentId {
    fn parse_argument(argument: JSONValue) -> crate::Result<Self> {
        argument
            .to_jmap_id()
            .map(|id| id as DocumentId)
            .ok_or_else(|| JMAPError::InvalidArguments("Failed to parse JMAP Id.".to_string()))
    }
}

impl JSONArgumentParser for BlobId {
    fn parse_argument(argument: JSONValue) -> crate::Result<Self> {
        argument
            .parse_blob_id(false)?
            .ok_or_else(|| JMAPError::InvalidArguments("Failed to parse Blob Id.".to_string()))
    }
}

impl JSONValue {
    fn eval_result_reference(&self, response: &Response) -> crate::Result<JSONValue> {
        if let JSONValue::Object(obj) = self {
            let result_of = obj
                .get("resultOf")
                .ok_or_else(|| JMAPError::InvalidArguments("resultOf key missing.".to_string()))?
                .to_string()
                .ok_or_else(|| {
                    JMAPError::InvalidArguments("resultOf key is not a string.".to_string())
                })?;
            let name = obj
                .get("name")
                .ok_or_else(|| JMAPError::InvalidArguments("name key missing.".to_string()))?
                .to_string()
                .ok_or_else(|| {
                    JMAPError::InvalidArguments("name key is not a string.".to_string())
                })?;
            let path = obj
                .get("path")
                .ok_or_else(|| JMAPError::InvalidArguments("path key missing.".to_string()))?
                .to_string()
                .ok_or_else(|| {
                    JMAPError::InvalidArguments("path key is not a string.".to_string())
                })?;

            for (method_name, result, call_id) in &response.method_responses {
                if name == method_name && call_id == result_of {
                    return result.eval(path);
                }
            }

            Err(JMAPError::InvalidArguments(format!(
                "No methodResponse found with name '{}' and call id '{}'.",
                name, result_of
            )))
        } else {
            Err(JMAPError::InvalidArguments(
                "ResultReference is not an object".to_string(),
            ))
        }
    }

    pub fn parse_document_id(self) -> crate::Result<DocumentId> {
        self.to_jmap_id()
            .map(|id| id as DocumentId)
            .ok_or_else(|| JMAPError::InvalidArguments("Failed to parse JMAP Id.".to_string()))
    }

    pub fn parse_jmap_id(self, optional: bool) -> crate::Result<Option<JMAPId>> {
        match self {
            JSONValue::String(string) => Ok(Some(JMAPId::from_jmap_string(&string).ok_or_else(
                || JMAPError::InvalidArguments("Failed to parse JMAP Id.".to_string()),
            )?)),
            JSONValue::Null if optional => Ok(None),
            _ => Err(JMAPError::InvalidArguments("Expected string.".to_string())),
        }
    }

    pub fn parse_jmap_state(self, optional: bool) -> crate::Result<Option<JMAPState>> {
        match self {
            JSONValue::String(string) => {
                Ok(Some(JMAPState::from_jmap_string(&string).ok_or_else(
                    || JMAPError::InvalidArguments("Failed to parse JMAP state.".to_string()),
                )?))
            }
            JSONValue::Null if optional => Ok(None),
            _ => Err(JMAPError::InvalidArguments("Expected string.".to_string())),
        }
    }

    pub fn parse_blob_id(self, optional: bool) -> crate::Result<Option<BlobId>> {
        match self {
            JSONValue::String(string) => Ok(Some(BlobId::from_jmap_string(&string).ok_or_else(
                || JMAPError::InvalidArguments("Failed to parse blobId.".to_string()),
            )?)),
            JSONValue::Null if optional => Ok(None),
            _ => Err(JMAPError::InvalidArguments("Expected string.".to_string())),
        }
    }

    pub fn parse_unsigned_int(self, optional: bool) -> crate::Result<Option<u64>> {
        match self {
            JSONValue::Number(number) => Ok(Some(number.to_unsigned_int())),
            JSONValue::Null if optional => Ok(None),
            _ => Err(JMAPError::InvalidArguments(
                "Expected unsigned integer.".to_string(),
            )),
        }
    }

    pub fn parse_int(self, optional: bool) -> crate::Result<Option<i64>> {
        match self {
            JSONValue::Number(number) => Ok(Some(number.to_int())),
            JSONValue::Null if optional => Ok(None),
            _ => Err(JMAPError::InvalidArguments("Expected integer.".to_string())),
        }
    }

    pub fn parse_string(self) -> crate::Result<String> {
        self.unwrap_string()
            .ok_or_else(|| JMAPError::InvalidArguments("Expected string.".to_string()))
    }

    pub fn parse_bool(self) -> crate::Result<bool> {
        self.to_bool()
            .ok_or_else(|| JMAPError::InvalidArguments("Expected boolean.".to_string()))
    }

    pub fn parse_utc_date(self, optional: bool) -> crate::Result<Option<i64>> {
        match self {
            JSONValue::String(date_time) => Ok(Some(
                DateTime::parse_from_rfc3339(&date_time)
                    .map_err(|_| {
                        JMAPError::InvalidArguments(format!(
                            "Failed to parse UTC Date '{}'",
                            date_time
                        ))
                    })?
                    .timestamp(),
            )),
            JSONValue::Null if optional => Ok(None),
            _ => Err(JMAPError::InvalidArguments(
                "Expected UTC date.".to_string(),
            )),
        }
    }

    pub fn parse_array_items<T>(self, optional: bool) -> crate::Result<Option<Vec<T>>>
    where
        T: JSONArgumentParser,
    {
        match self {
            JSONValue::Array(items) => {
                if !items.is_empty() {
                    let mut result = Vec::with_capacity(items.len());
                    for item in items {
                        result.push(T::parse_argument(item)?);
                    }
                    Ok(Some(result))
                } else if optional {
                    Ok(None)
                } else {
                    Err(JMAPError::InvalidArguments(
                        "Expected array with at least one item.".to_string(),
                    ))
                }
            }
            JSONValue::Null if optional => Ok(None),
            _ => Err(JMAPError::InvalidArguments("Expected array.".to_string())),
        }
    }

    fn parse_arguments<T>(self, response: &Response, mut parse_fnc: T) -> crate::Result<()>
    where
        T: FnMut(String, JSONValue) -> crate::Result<()>,
    {
        for (arg_name, arg_value) in self
            .unwrap_object()
            .ok_or_else(|| JMAPError::InvalidArguments("Expected object.".to_string()))?
            .into_iter()
        {
            if arg_name.starts_with('#') {
                parse_fnc(
                    arg_name
                        .get(1..)
                        .ok_or_else(|| {
                            JMAPError::InvalidArguments(
                                "Failed to parse argument name.".to_string(),
                            )
                        })?
                        .to_string(),
                    arg_value.eval_result_reference(response)?,
                )?;
            } else {
                parse_fnc(arg_name, arg_value)?;
            }
        }

        Ok(())
    }
}

impl Invocation {
    pub fn parse(name: &str, arguments: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut name_parts = name.split('/');
        let obj = match name_parts.next().ok_or_else(|| {
            JMAPError::InvalidArguments(format!("Failed to parse method name: {}.", name))
        })? {
            "Core" => Object::Core,
            "Mailbox" => Object::Mailbox,
            "Thread" => Object::Thread,
            "Email" => Object::Email,
            "SearchSnippet" => Object::SearchSnippet,
            "Identity" => Object::Identity,
            "EmailSubmission" => Object::EmailSubmission,
            "VacationResponse" => Object::VacationResponse,
            "PushSubscription" => Object::PushSubscription,
            _ => {
                return Err(JMAPError::UnknownMethod(format!(
                    "Unknown object: {}",
                    name
                )))
            }
        };

        let (account_id, call) = match name_parts.next().ok_or_else(|| {
            JMAPError::InvalidArguments(format!("Failed to parse method name: {}.", name))
        })? {
            "get" => {
                let r = GetRequest::parse(arguments, response)?;
                (r.account_id, Method::Get(r))
            }
            "set" => {
                let r = SetRequest::parse(arguments, response)?;
                (r.account_id, Method::Set(r))
            }
            "query" => {
                let r = QueryRequest::parse(arguments, response)?;
                (r.account_id, Method::Query(r))
            }
            "queryChanges" => {
                let r = QueryChangesRequest::parse(arguments, response)?;
                (r.account_id, Method::QueryChanges(r))
            }
            "changes" => {
                let r = ChangesRequest::parse(arguments, response)?;
                (r.account_id, Method::Changes(r))
            }
            "import" => {
                let r = ImportRequest::parse(arguments, response)?;
                (r.account_id, Method::Import(r))
            }
            "parse" => {
                let r = ParseRequest::parse(arguments, response)?;
                (r.account_id, Method::Parse(r))
            }
            "echo" => (0, Method::Echo(arguments)),
            _ => {
                return Err(JMAPError::UnknownMethod(format!(
                    "Unknown method: {}",
                    name
                )))
            }
        };

        Ok(Invocation {
            obj,
            call,
            account_id,
        })
    }
}

impl GetRequest {
    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = GetRequest {
            account_id: 0,
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

impl SetRequest {
    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = SetRequest {
            account_id: 0,
            if_in_state: None,
            create: JSONValue::Null,
            update: JSONValue::Null,
            destroy: JSONValue::Null,
            arguments: HashMap::new(),
        };

        invocation.parse_arguments(response, |name, value| {
            match name.as_str() {
                "accountId" => request.account_id = value.parse_document_id()?,
                "ifInState" => request.if_in_state = value.parse_jmap_state(true)?,
                "create" => request.create = value,
                "update" => request.update = value,
                "destroy" => request.destroy = value,
                _ => {
                    request.arguments.insert(name, value);
                }
            }
            Ok(())
        })?;

        Ok(request)
    }
}

impl QueryRequest {
    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = QueryRequest {
            account_id: 0,
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

impl QueryChangesRequest {
    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = QueryChangesRequest {
            account_id: 0,
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

impl ChangesRequest {
    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = ChangesRequest {
            account_id: 0,
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

impl ImportRequest {
    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = ImportRequest {
            account_id: 0,
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

impl ParseRequest {
    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = ParseRequest {
            account_id: 0,
            arguments: HashMap::new(),
        };

        invocation.parse_arguments(response, |name, value| {
            match name.as_str() {
                "accountId" => request.account_id = value.parse_document_id()?,
                _ => {
                    request.arguments.insert(name, value);
                }
            }
            Ok(())
        })?;

        Ok(request)
    }
}

impl Response {
    pub fn new(session_state: u64, capacity: usize) -> Self {
        Response {
            session_state,
            method_responses: Vec::with_capacity(capacity),
        }
    }

    pub fn push_response(&mut self, name: String, call_id: String, response: JSONValue) {
        self.method_responses.push((name, response, call_id));
    }

    pub fn push_error(&mut self, call_id: String, error: JMAPError) {
        self.method_responses
            .push(("error".to_string(), error.into(), call_id));
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, iter::FromIterator};

    use crate::{json::JSONValue, request::Request};

    #[test]
    fn parse_request() {
        assert_eq!(
            serde_json::from_slice::<Request>(
                br#"{
                "using": [ "urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail" ],
                "methodCalls": [
                  [ "method1", {
                    "arg1": "arg1data",
                    "arg2": "arg2data"
                  }, "c1" ],
                  [ "method2", {
                    "arg1": "arg1data"
                  }, "c2" ],
                  [ "method3", {}, "c3" ]
                ]
              }"#,
            )
            .unwrap(),
            Request {
                using: vec![
                    "urn:ietf:params:jmap:core".to_string(),
                    "urn:ietf:params:jmap:mail".to_string()
                ],
                method_calls: vec![
                    (
                        "method1".to_string(),
                        HashMap::from_iter([
                            (
                                "arg2".to_string(),
                                JSONValue::String("arg2data".to_string())
                            ),
                            (
                                "arg1".to_string(),
                                JSONValue::String("arg1data".to_string())
                            )
                        ])
                        .into(),
                        "c1".to_string()
                    ),
                    (
                        "method2".to_string(),
                        HashMap::from_iter([(
                            "arg1".to_string(),
                            JSONValue::String("arg1data".to_string())
                        )])
                        .into(),
                        "c2".to_string()
                    ),
                    (
                        "method3".to_string(),
                        HashMap::new().into(),
                        "c3".to_string()
                    )
                ],
                created_ids: None
            }
        );
    }
}

pub fn serialize_hex<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    format!("{:x}", value).serialize(serializer)
}
