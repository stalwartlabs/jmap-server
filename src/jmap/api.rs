use actix_web::http::header::ContentType;
use actix_web::http::StatusCode;
use actix_web::{web, HttpResponse};

use jmap::error::method::MethodError;
use jmap::error::request::{RequestError, RequestLimitError};
use jmap::jmap_store::changes::JMAPChanges;
use jmap::jmap_store::get::JMAPGet;
use jmap::jmap_store::import::JMAPImport;
use jmap::jmap_store::parse::JMAPParse;
use jmap::jmap_store::query::JMAPQuery;
use jmap::jmap_store::query_changes::JMAPQueryChanges;
use jmap::jmap_store::set::JMAPSet;
use jmap::protocol::invocation::{Invocation, Method, Object};
use jmap::protocol::json::JSONValue;
use jmap::protocol::request::Request;
use jmap::protocol::response::Response;
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
            (Object::Core, Method::Echo(arguments)) => arguments,
            _ => {
                return Err(MethodError::ServerUnavailable);
            }
        })
    })
    .await
}
