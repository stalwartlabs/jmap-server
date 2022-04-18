use actix_web::http::header::ContentType;
use actix_web::http::StatusCode;
use actix_web::{web, HttpResponse};

use jmap::json::JSONValue;
use jmap::request::{Invocation, Method, Object, Request, Response};
use jmap::{JMAPError, RequestError, RequestLimitError};
use jmap_mail::mail::changes::JMAPMailChanges;
use jmap_mail::mail::get::JMAPMailGet;
use jmap_mail::mail::import::JMAPMailImport;
use jmap_mail::mail::parse::JMAPMailParse;
use jmap_mail::mail::query::JMAPMailQuery;
use jmap_mail::mail::query_changes::JMAPMailQueryChanges;
use jmap_mail::mail::set::JMAPMailSet;

use jmap_mail::mailbox::changes::JMAPMailMailboxChanges;
use jmap_mail::mailbox::get::JMAPMailMailboxGet;
use jmap_mail::mailbox::query::JMAPMailMailboxQuery;
use jmap_mail::mailbox::query_changes::JMAPMailMailboxQueryChanges;
use jmap_mail::mailbox::set::JMAPMailMailboxSet;
use jmap_mail::thread::changes::JMAPMailThreadChanges;
use jmap_mail::thread::get::JMAPMailThreadGet;
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
                    Ok(result) => response.push_response(
                        name,
                        call_id,
                        result,
                        is_set && (include_created_ids || call_num < total_method_calls - 1),
                    ),
                    Err(err) => response.push_error(call_id, err),
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
        (Object::Thread, Method::Get(request)) => store.thread_get(request),
        (Object::Thread, Method::Changes(request)) => store.thread_changes(request),
        (Object::Mailbox, Method::Get(request)) => store.mailbox_get(request),
        (Object::Mailbox, Method::Set(request)) => store.mailbox_set(request),
        (Object::Mailbox, Method::Query(request)) => store.mailbox_query(request),
        (Object::Mailbox, Method::QueryChanges(request)) => store.mailbox_query_changes(request),
        (Object::Mailbox, Method::Changes(request)) => store.mailbox_changes(request),
        (Object::Core, Method::Echo(arguments)) => Ok(arguments),
        _ => Err(JMAPError::ServerUnavailable),
    })
    .await
}
