use actix_web::{
    http::{header::ContentType, StatusCode},
    web, HttpResponse,
};
use jmap::{
    error::request::{RequestError, RequestLimitError},
    protocol::{json::JSONValue, request::Request},
};
use store::{tracing::debug, Store};

use crate::{api::invocation::handle_method_calls, JMAPServer};

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
