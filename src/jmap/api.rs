use actix_web::http::header::ContentType;
use actix_web::http::StatusCode;
use actix_web::{web, HttpResponse};

use jmap::error::method::MethodError;
use jmap::error::request::{RequestError, RequestLimitError};
use jmap::jmap_store::changes::{ChangesResult, JMAPChanges};
use jmap::jmap_store::get::{GetResult, JMAPGet};
use jmap::jmap_store::import::{ImportResult, JMAPImport};
use jmap::jmap_store::parse::{JMAPParse, ParseResult};
use jmap::jmap_store::query::{JMAPQuery, QueryResult};
use jmap::jmap_store::query_changes::{JMAPQueryChanges, QueryChangesResult};
use jmap::jmap_store::set::{JMAPSet, SetResult};
use jmap::protocol::invocation::{Invocation, Method, Object};
use jmap::protocol::json::JSONValue;
use jmap::protocol::request::Request;
use jmap::protocol::response::Response;
use jmap_mail::identity::changes::ChangesIdentity;
use jmap_mail::identity::get::GetIdentity;
use jmap_mail::identity::set::SetIdentity;
use jmap_mail::mail::changes::ChangesMail;
use jmap_mail::mail::get::GetMail;
use jmap_mail::mail::import::ImportMail;
use jmap_mail::mail::parse::ParseMail;
use jmap_mail::mail::query::QueryMail;
use jmap_mail::mail::set::SetMail;
use jmap_mail::mailbox::changes::ChangesMailbox;
use jmap_mail::mailbox::get::GetMailbox;
use jmap_mail::mailbox::query::QueryMailbox;
use jmap_mail::mailbox::set::SetMailbox;
use jmap_mail::thread::changes::ChangesThread;
use jmap_mail::thread::get::GetThread;
use store::log::ChangeId;
use store::tracing::debug;
use store::Store;

use super::server::JMAPServer;

pub async fn handle_jmap_request<T>(
    request: web::Bytes,
    core: web::Data<JMAPServer<T>>,
) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    let (status_code, body) = if request.len() < core.store.config.max_size_request {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::from_slice::<JSONValue>(&request).unwrap())
                .unwrap()
        );

        match serde_json::from_slice::<Request>(&request) {
            Ok(request) => {
                if request.method_calls.len() < core.store.config.max_calls_in_request {
                    (StatusCode::OK, {
                        let result = handle_method_calls(request, core).await;
                        println!("{}", serde_json::to_string_pretty(&result).unwrap());
                        result.to_json()
                    })
                } else {
                    (
                        StatusCode::BAD_REQUEST,
                        RequestError::limit(RequestLimitError::CallsIn).to_json(),
                    )
                }
            }
            Err(err) => {
                debug!("Failed to parse request: {}", err);

                (
                    StatusCode::BAD_REQUEST,
                    RequestError::not_request().to_json(),
                )
            }
        }
    } else {
        (
            StatusCode::BAD_REQUEST,
            RequestError::limit(RequestLimitError::Size).to_json(),
        )
    };

    HttpResponse::build(status_code)
        .insert_header(ContentType::json())
        .body(body)
}

pub async fn handle_method_calls<T>(request: Request, core: web::Data<JMAPServer<T>>) -> Response
where
    T: for<'x> Store<'x> + 'static,
{
    let include_created_ids = request.created_ids.is_some();
    let mut response = Response::new(
        1234,
        request.created_ids.unwrap_or_default(),
        request.method_calls.len(),
    );

    let total_method_calls = request.method_calls.len();
    for (call_num, (name, arguments, call_id)) in request.method_calls.into_iter().enumerate() {
        match Invocation::parse(&name, arguments, &response, &core.store.config) {
            Ok(invocation) => {
                let is_set = invocation.is_set();

                match handle_method_call(invocation, &core).await {
                    Ok(result) => {
                        // Add result
                        let mut change_id = result.change_id;
                        response.push_response(
                            name,
                            call_id.clone(),
                            result.result,
                            is_set && (include_created_ids || call_num < total_method_calls - 1),
                        );

                        // Execute next invocation
                        if let Some(next_invocation) = result.next_invocation {
                            let name = next_invocation.to_string();
                            match handle_method_call(next_invocation, &core).await {
                                Ok(result) => {
                                    if result.change_id.is_some() {
                                        change_id = result.change_id;
                                    }
                                    response.push_response(name, call_id, result.result, false);
                                }
                                Err(err) => {
                                    response.push_error(call_id, err);
                                }
                            }
                        }

                        // Wait for cluster
                        if core.store.config.is_in_cluster && core.is_leader() {
                            if let Some(_change_id) = change_id {
                                //Todo wait for changes to propagate
                            }
                        }
                    }
                    Err(err) => {
                        response.push_error(call_id, err);
                    }
                }
            }
            Err(err) => response.push_error(call_id, err),
        }
    }

    if !include_created_ids {
        response.created_ids.clear();
    }

    response
}

pub struct InvocationResult {
    result: JSONValue,
    next_invocation: Option<Invocation>,
    change_id: Option<ChangeId>,
}

pub async fn handle_method_call<T>(
    invocation: Invocation,
    core: &web::Data<JMAPServer<T>>,
) -> jmap::Result<InvocationResult>
where
    T: for<'x> Store<'x> + 'static,
{
    let store = core.store.clone();
    core.spawn_jmap_request(move || {
        Ok(match (invocation.obj, invocation.call) {
            (Object::Email, Method::Get(request)) => store.get::<GetMail<T>>(request)?.into(),
            (Object::Email, Method::Set(request)) => store.set::<SetMail>(request)?.into(),
            (Object::Email, Method::Query(request)) => store.query::<QueryMail<T>>(request)?.into(),
            (Object::Email, Method::QueryChanges(request)) => store
                .query_changes::<ChangesMail, QueryMail<T>>(request)?
                .into(),
            (Object::Email, Method::Changes(request)) => {
                store.changes::<ChangesMail>(request)?.into()
            }
            (Object::Email, Method::Import(request)) => {
                store.import::<ImportMail<T>>(request)?.into()
            }
            (Object::Email, Method::Parse(request)) => store.parse::<ParseMail>(request)?.into(),
            (Object::Thread, Method::Get(request)) => store.get::<GetThread<T>>(request)?.into(),
            (Object::Thread, Method::Changes(request)) => {
                store.changes::<ChangesThread>(request)?.into()
            }
            (Object::Mailbox, Method::Get(request)) => store.get::<GetMailbox<T>>(request)?.into(),
            (Object::Mailbox, Method::Set(request)) => store.set::<SetMailbox>(request)?.into(),
            (Object::Mailbox, Method::Query(request)) => {
                store.query::<QueryMailbox<T>>(request)?.into()
            }
            (Object::Mailbox, Method::QueryChanges(request)) => store
                .query_changes::<ChangesMailbox, QueryMailbox<T>>(request)?
                .into(),
            (Object::Mailbox, Method::Changes(request)) => {
                store.changes::<ChangesMailbox>(request)?.into()
            }
            (Object::Identity, Method::Get(request)) => {
                store.get::<GetIdentity<T>>(request)?.into()
            }
            (Object::Identity, Method::Set(request)) => store.set::<SetIdentity>(request)?.into(),
            (Object::Identity, Method::Changes(request)) => {
                store.changes::<ChangesIdentity>(request)?.into()
            }
            (Object::Core, Method::Echo(arguments)) => InvocationResult::new(arguments),
            _ => {
                return Err(MethodError::ServerUnavailable);
            }
        })
    })
    .await
}

impl InvocationResult {
    pub fn new(result: JSONValue) -> Self {
        Self {
            result,
            next_invocation: None,
            change_id: None,
        }
    }
}

impl From<SetResult> for InvocationResult {
    fn from(mut result: SetResult) -> Self {
        InvocationResult {
            next_invocation: Option::take(&mut result.next_invocation),
            change_id: if result.new_state != result.old_state {
                result.new_state.get_change_id()
            } else {
                None
            },
            result: result.into(),
        }
    }
}

impl From<GetResult> for InvocationResult {
    fn from(result: GetResult) -> Self {
        InvocationResult::new(result.into())
    }
}

impl From<ChangesResult> for InvocationResult {
    fn from(result: ChangesResult) -> Self {
        InvocationResult::new(result.into())
    }
}

impl From<QueryResult> for InvocationResult {
    fn from(result: QueryResult) -> Self {
        InvocationResult::new(result.into())
    }
}

impl From<QueryChangesResult> for InvocationResult {
    fn from(result: QueryChangesResult) -> Self {
        InvocationResult::new(result.into())
    }
}

impl From<ImportResult> for InvocationResult {
    fn from(result: ImportResult) -> Self {
        InvocationResult {
            change_id: if result.new_state != result.old_state {
                result.new_state.get_change_id()
            } else {
                None
            },
            result: result.into(),
            next_invocation: None,
        }
    }
}

impl From<ParseResult> for InvocationResult {
    fn from(result: ParseResult) -> Self {
        InvocationResult::new(result.into())
    }
}
