use std::fmt::Display;

use store::{AccountId, JMAPConfig};

use crate::{
    error::method::MethodError,
    request::{
        changes::ChangesRequest, get::GetRequest, import::ImportRequest, parse::ParseRequest,
        query::QueryRequest, query_changes::QueryChangesRequest, set::SetRequest,
    },
};

use super::{json::JSONValue, response::Response};

#[derive(Debug)]
pub struct Invocation {
    pub obj: Object,
    pub call: Method,
    pub account_id: AccountId,
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

impl Invocation {
    pub fn parse(
        name: &str,
        arguments: JSONValue,
        response: &Response,
        config: &JMAPConfig,
    ) -> crate::Result<Self> {
        let mut name_parts = name.split('/');
        let obj = match name_parts.next().ok_or_else(|| {
            MethodError::InvalidArguments(format!("Failed to parse method name: {}.", name))
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
                return Err(MethodError::UnknownMethod(format!(
                    "Unknown object: {}",
                    name
                )))
            }
        };

        let (account_id, call) = match name_parts.next().ok_or_else(|| {
            MethodError::InvalidArguments(format!("Failed to parse method name: {}.", name))
        })? {
            "get" => {
                let r = GetRequest::parse(arguments, response)?;
                (r.account_id, Method::Get(r))
            }
            "set" => {
                let r = SetRequest::parse(arguments, response)?;
                if r.create.len() + r.update.len() + r.destroy.len() > config.max_objects_in_set {
                    return Err(MethodError::RequestTooLarge);
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
                return Err(MethodError::UnknownMethod(format!(
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

impl Display for Object {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Object::Core => write!(f, "Core"),
            Object::Mailbox => write!(f, "Mailbox"),
            Object::Thread => write!(f, "Thread"),
            Object::Email => write!(f, "Email"),
            Object::SearchSnippet => write!(f, "SearchSnippet"),
            Object::Identity => write!(f, "Identity"),
            Object::EmailSubmission => write!(f, "EmailSubmission"),
            Object::VacationResponse => write!(f, "VacationResponse"),
            Object::PushSubscription => write!(f, "PushSubscription"),
        }
    }
}

impl Display for Method {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Method::Echo(..) => write!(f, "echo"),
            Method::Get(..) => write!(f, "get"),
            Method::Set(..) => write!(f, "set"),
            Method::Query(..) => write!(f, "query"),
            Method::QueryChanges(..) => write!(f, "queryChanges"),
            Method::Changes(..) => write!(f, "changes"),
            Method::Import(..) => write!(f, "import"),
            Method::Parse(..) => write!(f, "parse"),
        }
    }
}

impl Display for Invocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.obj, self.call)
    }
}
