use actix_web::http::header::ContentType;
use actix_web::http::StatusCode;
use actix_web::{web, HttpResponse};

use jmap::json::JSONValue;
use jmap::request::{Invocation, Method, Object, Request, Response};
use jmap::{JMAPError, RequestError, RequestLimitError};
use jmap_mail::changes::JMAPMailChanges;
use jmap_mail::get::JMAPMailGet;
use jmap_mail::import::JMAPMailImport;
use jmap_mail::mailbox::JMAPMailMailbox;
use jmap_mail::parse::JMAPMailParse;
use jmap_mail::query::JMAPMailQuery;
use jmap_mail::set::JMAPMailSet;
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
        match serde_json::from_slice::<Request>(&request) {
            Ok(request) => {
                if request.method_calls.len() < core.store.config.max_calls_in_request {
                    (
                        StatusCode::OK,
                        handle_method_calls(request, core).await.to_json(),
                    )
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
    let mut responses = Response::new(1234, request.method_calls.len());

    for (name, arguments, call_id) in request.method_calls {
        match Invocation::parse(&name, arguments, &responses) {
            Ok(invocation) => match handle_method_call(invocation, &core).await {
                Ok(response) => responses.push_response(name, call_id, response),
                Err(err) => responses.push_error(call_id, err),
            },
            Err(err) => responses.push_error(call_id, err),
        }
    }

    responses
}

pub async fn handle_method_call<T>(
    invocation: Invocation,
    core: &web::Data<JMAPServer<T>>,
) -> jmap::Result<JSONValue>
where
    T: for<'x> Store<'x> + 'static,
{
    let store = core.store.clone();
    core.spawn_jmap_request(move || match (invocation.obj, invocation.call) {
        (Object::Email, Method::Get(request)) => store.mail_get(request),
        (Object::Email, Method::Set(request)) => store.mail_set(request),
        (Object::Email, Method::Query(request)) => store.mail_query(request),
        (Object::Email, Method::QueryChanges(request)) => store.mail_query_changes(request),
        (Object::Email, Method::Changes(request)) => store.mail_changes(request),
        (Object::Email, Method::Import(request)) => store.mail_import(request),
        (Object::Email, Method::Parse(request)) => store.mail_parse(request),
        (Object::Mailbox, Method::Get(request)) => store.mailbox_get(request),
        (Object::Mailbox, Method::Set(request)) => store.mailbox_set(request),
        (Object::Mailbox, Method::Query(request)) => store.mailbox_query(request),
        (Object::Mailbox, Method::QueryChanges(request)) => store.mailbox_query_changes(request),
        (Object::Mailbox, Method::Changes(request)) => store.mailbox_changes(request),
        (Object::Core, Method::Echo(arguments)) => Ok(arguments),
        _ => Err(JMAPError::AccountNotFound),
    })
    .await
}
