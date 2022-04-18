use std::collections::HashMap;

use serde::Serialize;
use store::{chrono::DateTime, AccountId, DocumentId, JMAPConfig, JMAPId};

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
    #[serde(rename(deserialize = "createdIds"))]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub created_ids: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct SetRequest {
    pub account_id: AccountId,
    pub if_in_state: Option<JMAPState>,
    pub create: Vec<(String, JSONValue)>,
    pub update: HashMap<String, JSONValue>,
    pub destroy: Vec<JSONValue>,
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

#[derive(Debug)]
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

#[derive(Debug)]
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
    pub fn parse(
        name: &str,
        arguments: JSONValue,
        response: &Response,
        config: &JMAPConfig,
    ) -> crate::Result<Self> {
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
                if r.create.len() + r.update.len() + r.destroy.len() > config.max_objects_in_set {
                    return Err(JMAPError::RequestTooLarge);
                }
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

    pub fn is_set(&self) -> bool {
        matches!(self.call, Method::Set(_))
    }
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

impl SetRequest {
    fn map_id_references(
        child_id: &str,
        property: &mut JSONValue,
        response: &Response,
        mut graph: Option<&mut HashMap<String, Vec<String>>>,
    ) {
        match property {
            JSONValue::String(id_ref) if id_ref.starts_with('#') => {
                if let Some(parent_id) = id_ref.get(1..) {
                    if let Some(id) = response.created_ids.get(parent_id) {
                        *id_ref = id.to_string();
                    } else if let Some(graph) = graph.as_mut() {
                        graph
                            .entry(child_id.to_string())
                            .or_insert_with(Vec::new)
                            .push(parent_id.to_string());
                    }
                }
            }
            JSONValue::Array(array) => {
                for array_item in array {
                    if let JSONValue::String(id_ref) = array_item {
                        if id_ref.starts_with('#') {
                            if let Some(parent_id) = id_ref.get(1..) {
                                if let Some(id) = response.created_ids.get(parent_id) {
                                    *id_ref = id.to_string();
                                } else if let Some(graph) = graph.as_mut() {
                                    graph
                                        .entry(child_id.to_string())
                                        .or_insert_with(Vec::new)
                                        .push(parent_id.to_string());
                                }
                            }
                        }
                    }
                }
            }
            JSONValue::Object(object) => {
                let mut rename_keys = HashMap::with_capacity(object.len());
                for key in object.keys() {
                    if key.starts_with('#') {
                        if let Some(parent_id) = key.get(1..) {
                            if let Some(id) = response.created_ids.get(parent_id) {
                                rename_keys.insert(key.to_string(), id.to_string());
                            } else if let Some(graph) = graph.as_mut() {
                                graph
                                    .entry(child_id.to_string())
                                    .or_insert_with(Vec::new)
                                    .push(parent_id.to_string());
                            }
                        }
                    }
                }
                for (rename_from_key, rename_to_key) in rename_keys {
                    let value = object.remove(&rename_from_key).unwrap();
                    object.insert(rename_to_key, value);
                }
            }
            _ => (),
        }
    }

    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = SetRequest {
            account_id: 1, //TODO
            if_in_state: None,
            create: Vec::with_capacity(0),
            update: HashMap::with_capacity(0),
            destroy: Vec::with_capacity(0),
            arguments: HashMap::new(),
        };

        invocation.parse_arguments(response, |name, value| {
            match name.as_str() {
                "accountId" => request.account_id = value.parse_document_id()?,
                "ifInState" => request.if_in_state = value.parse_jmap_state(true)?,
                "create" => {
                    // Order create objects by reference
                    if let Some(mut objects) = value.unwrap_object() {
                        let mut create = Vec::with_capacity(objects.len());
                        let mut graph = HashMap::with_capacity(objects.len());

                        for (child_id, object) in objects.iter_mut() {
                            if let Some(properties) = object.to_object_mut() {
                                for (property_id, property) in properties {
                                    if property_id.ends_with("Id") || property_id.ends_with("Ids") {
                                        SetRequest::map_id_references(
                                            child_id,
                                            property,
                                            response,
                                            Some(&mut graph),
                                        );
                                    }
                                }
                            }
                        }

                        // Topological sort
                        if !graph.is_empty() {
                            let mut it_stack = Vec::new();
                            let keys = graph.keys().cloned().collect::<Vec<_>>();
                            let mut it = keys.iter();

                            'main: loop {
                                while let Some(from_id) = it.next() {
                                    if let Some(to_ids) = graph.get(from_id) {
                                        it_stack.push((it, from_id));
                                        if it_stack.len() > 1000 {
                                            return Err(JMAPError::InvalidArguments(
                                                "Cyclical references are not allowed.".to_string(),
                                            ));
                                        }
                                        it = to_ids.iter();
                                        continue;
                                    } else if let Some(object) = objects.remove(from_id) {
                                        create.push((from_id.to_string(), object));
                                        if objects.is_empty() {
                                            break 'main;
                                        }
                                    }
                                }

                                if let Some((prev_it, from_id)) = it_stack.pop() {
                                    it = prev_it;
                                    if let Some(object) = objects.remove(from_id) {
                                        create.push((from_id.to_string(), object));
                                        if objects.is_empty() {
                                            break 'main;
                                        }
                                    }
                                } else {
                                    break;
                                }
                            }
                        }

                        for (user_id, object) in objects {
                            create.push((user_id, object));
                        }
                        request.create = create;
                    }
                }
                "update" => {
                    if let Some(mut objects) = value.unwrap_object() {
                        for object in objects.values_mut() {
                            if let Some(properties) = object.to_object_mut() {
                                for (property_id, property) in properties {
                                    if property_id.ends_with("Id") || property_id.ends_with("Ids") {
                                        SetRequest::map_id_references(
                                            property_id,
                                            property,
                                            response,
                                            None,
                                        );
                                    }
                                }
                            }
                        }
                        request.update = objects;
                    }
                }
                "destroy" => {
                    if let Some(mut array_items) = value.unwrap_array() {
                        for item in &mut array_items {
                            SetRequest::map_id_references("", item, response, None);
                        }
                        request.destroy = array_items;
                    }
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

impl QueryRequest {
    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = QueryRequest {
            account_id: 1, //TODO
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

impl ParseRequest {
    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = ParseRequest {
            account_id: 1, //TODO
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
    pub fn new(session_state: u64, created_ids: HashMap<String, String>, capacity: usize) -> Self {
        Response {
            session_state,
            created_ids,
            method_responses: Vec::with_capacity(capacity),
        }
    }

    pub fn push_response(
        &mut self,
        name: String,
        call_id: String,
        response: JSONValue,
        add_created_ids: bool,
    ) {
        if add_created_ids {
            if let Some(obj) = response
                .to_object()
                .and_then(|o| o.get("created"))
                .and_then(|o| o.to_object())
            {
                for (user_id, obj) in obj {
                    if let Some(id) = obj
                        .to_object()
                        .and_then(|o| o.get("id"))
                        .and_then(|id| id.to_string())
                    {
                        self.created_ids.insert(user_id.to_string(), id.to_string());
                    }
                }
            }
        }

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

pub fn serialize_hex<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    format!("{:x}", value).serialize(serializer)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use store::{config::EnvSettings, JMAPConfig};

    use crate::{
        json::JSONValue,
        request::{Method, Request},
        JMAPError,
    };

    use super::{Invocation, Response};

    #[test]
    fn map_sort_created_ids() {
        let request = serde_json::from_slice::<Request>(
            br##"{
                    "using": [
                        "urn:ietf:params:jmap:core",
                        "urn:ietf:params:jmap:mail"
                    ],
                    "methodCalls": [
                        [
                            "Mailbox/set",
                            {
                                "accountId": "i01",
                                "create": {
                                    "a": {
                                        "name": "Folder a",
                                        "parentId": "#b"
                                    },
                                    "b": {
                                        "name": "Folder b",
                                        "parentId": "#c"
                                    },
                                    "c": {
                                        "name": "Folder c",
                                        "parentId": "#d"
                                    },
                                    "d": {
                                        "name": "Folder d",
                                        "parentId": "#e"
                                    },
                                    "e": {
                                        "name": "Folder e",
                                        "parentId": "#f"
                                    },
                                    "f": {
                                        "name": "Folder f",
                                        "parentId": "#g"
                                    },
                                    "g": {
                                        "name": "Folder g",
                                        "parentId": null
                                    }
                                }
                            },
                            "fulltree"
                        ],
                        [
                            "Mailbox/set",
                            {
                                "accountId": "i01",
                                "create": {
                                    "a1": {
                                        "name": "Folder a1",
                                        "parentId": null
                                    },
                                    "b2": {
                                        "name": "Folder b2",
                                        "parentId": "#a1"
                                    },
                                    "c3": {
                                        "name": "Folder c3",
                                        "parentId": "#a1"
                                    },
                                    "d4": {
                                        "name": "Folder d4",
                                        "parentId": "#b2"
                                    },
                                    "e5": {
                                        "name": "Folder e5",
                                        "parentId": "#b2"
                                    },
                                    "f6": {
                                        "name": "Folder f6",
                                        "parentId": "#d4"
                                    },
                                    "g7": {
                                        "name": "Folder g7",
                                        "parentId": "#e5"
                                    }
                                }
                            },
                            "fulltree2"
                        ],
                        [
                            "Mailbox/set",
                            {
                                "accountId": "i01",
                                "create": {
                                    "z": {
                                        "name": "Folder Z",
                                        "parentId": "#x"
                                    },
                                    "y": {
                                        "name": null
                                    },
                                    "x": {
                                        "name": "Folder X"
                                    }
                                }
                            },
                            "xyz"
                        ],
                        [
                            "Mailbox/set",
                            {
                                "accountId": "i01",
                                "create": {
                                    "a": {
                                        "name": "Folder a",
                                        "parentId": "#b"
                                    },
                                    "b": {
                                        "name": "Folder b",
                                        "parentId": "#c"
                                    },
                                    "c": {
                                        "name": "Folder c",
                                        "parentId": "#d"
                                    },
                                    "d": {
                                        "name": "Folder d",
                                        "parentId": "#a"
                                    }
                                }
                            },
                            "circular"
                        ]
                    ]
                }"##,
        )
        .unwrap();

        let response = Response::new(
            1234,
            request.created_ids.unwrap_or_default(),
            request.method_calls.len(),
        );
        let config = JMAPConfig::from(&EnvSettings {
            args: HashMap::new(),
        });

        for (test_num, (name, arguments, _)) in request.method_calls.into_iter().enumerate() {
            match Invocation::parse(&name, arguments, &response, &config) {
                Ok(invocation) => {
                    assert!((0..3).contains(&test_num), "Unexpected invocation");

                    if let Method::Set(set) = invocation.call {
                        if test_num == 0 {
                            assert_eq!(
                                set.create.into_iter().map(|b| b.0).collect::<Vec<_>>(),
                                ["g", "f", "e", "d", "c", "b", "a"]
                                    .iter()
                                    .map(|i| i.to_string())
                                    .collect::<Vec<_>>()
                            );
                        } else if test_num == 1 {
                            let mut pending_ids = vec!["a1", "b2", "d4", "e5", "f6", "c3", "g7"];

                            for (id, _) in &set.create {
                                match id.as_str() {
                                    "a1" => (),
                                    "b2" | "c3" => assert!(!pending_ids.contains(&"a1")),
                                    "d4" | "e5" => assert!(!pending_ids.contains(&"b2")),
                                    "f6" => assert!(!pending_ids.contains(&"d4")),
                                    "g7" => assert!(!pending_ids.contains(&"e5")),
                                    _ => panic!("Unexpected ID"),
                                }
                                pending_ids.retain(|i| i != id);
                            }

                            if !pending_ids.is_empty() {
                                panic!(
                                    "Unexpected order: {:?}",
                                    all_ids = set
                                        .create
                                        .iter()
                                        .map(|b| b.0.to_string())
                                        .collect::<Vec<_>>()
                                );
                            }
                        } else if test_num == 2 {
                            assert_eq!(
                                set.create.into_iter().map(|b| b.0).collect::<Vec<_>>(),
                                ["x", "z", "y"]
                                    .iter()
                                    .map(|i| i.to_string())
                                    .collect::<Vec<_>>()
                            );
                        }
                    } else {
                        panic!("Expected SetRequest");
                    };
                }
                Err(err) => {
                    assert_eq!(test_num, 3);
                    assert!(matches!(err, JMAPError::InvalidArguments(_)));
                }
            }
        }

        let request = serde_json::from_slice::<Request>(
            br##"{
                "using": [
                    "urn:ietf:params:jmap:core",
                    "urn:ietf:params:jmap:mail"
                ],
                "methodCalls": [
                    [
                        "Mailbox/set",
                        {
                            "accountId": "i01",
                            "create": {
                                "a": {
                                    "name": "a",
                                    "parentId": "#x"
                                },
                                "b": {
                                    "name": "b",
                                    "parentId": "#y"
                                },
                                "c": {
                                    "name": "c",
                                    "parentId": "#z"
                                }
                            }
                        },
                        "ref1"
                    ],
                    [
                        "Mailbox/set",
                        {
                            "accountId": "i01",
                            "create": {
                                "a1": {
                                    "name": "a1",
                                    "parentId": "#a"
                                },
                                "b2": {
                                    "name": "b2",
                                    "parentId": "#b"
                                },
                                "c3": {
                                    "name": "c3",
                                    "parentId": "#c"
                                }
                            }
                        },
                        "red2"
                    ]
                ],
                "createdIds": {
                    "x": "i01",
                    "y": "i02",
                    "z": "i03"
                }
            }"##,
        )
        .unwrap();

        let mut response = Response::new(
            1234,
            request.created_ids.unwrap_or_default(),
            request.method_calls.len(),
        );

        let mut invocations = request.method_calls.into_iter();
        let (name, arguments, _) = invocations.next().unwrap();
        let invocation = Invocation::parse(&name, arguments, &response, &config).unwrap();
        if let Method::Set(set) = invocation.call {
            let create: JSONValue = set.create.into_iter().collect::<HashMap<_, _>>().into();
            assert_eq!(create.eval_unwrap_string("/a/parentId"), "i01");
            assert_eq!(create.eval_unwrap_string("/b/parentId"), "i02");
            assert_eq!(create.eval_unwrap_string("/c/parentId"), "i03");
        } else {
            panic!("Expected SetRequest");
        };

        response.push_response(
            "test".to_string(),
            "test".to_string(),
            serde_json::from_slice::<JSONValue>(
                br##"{
                "created": {
                    "a": {
                        "id": "i05"
                    },
                    "b": {
                        "id": "i06"
                    },
                    "c": {
                        "id": "i07"
                    }
                }
            }"##,
            )
            .unwrap(),
            true,
        );

        let (name, arguments, _) = invocations.next().unwrap();
        let invocation = Invocation::parse(&name, arguments, &response, &config).unwrap();
        if let Method::Set(set) = invocation.call {
            let create: JSONValue = set.create.into_iter().collect::<HashMap<_, _>>().into();
            assert_eq!(create.eval_unwrap_string("/a1/parentId"), "i05");
            assert_eq!(create.eval_unwrap_string("/b2/parentId"), "i06");
            assert_eq!(create.eval_unwrap_string("/c3/parentId"), "i07");
        } else {
            panic!("Expected SetRequest");
        };
    }
}
