use std::collections::HashMap;

use actix_web::{
    http::{header::ContentType, StatusCode},
    web, HttpResponse,
};
use jmap::types::jmap::JMAPId;
use store::{tracing::debug, Store};

use crate::{
    api::{invocation::handle_method_calls, RequestError, RequestLimitError},
    authorization::Session,
    JMAPServer,
};

use super::method;

#[derive(Debug, serde::Deserialize)]
pub struct Request {
    pub using: Vec<String>,

    #[serde(rename = "methodCalls")]
    pub method_calls: Vec<method::Call<method::Request>>,

    #[serde(rename = "createdIds")]
    pub created_ids: Option<HashMap<String, JMAPId>>,
}

pub async fn handle_jmap_request<T>(
    request: web::Bytes,
    core: web::Data<JMAPServer<T>>,
    session: Session,
) -> Result<HttpResponse, RequestError>
where
    T: for<'x> Store<'x> + 'static,
{
    if request.len() < core.store.config.max_size_request {
        /* println!(
            "{}",
            serde_json::to_string_pretty(
                &serde_json::from_slice::<serde_json::Value>(&request).unwrap()
            )
            .unwrap()
        );*/

        match serde_json::from_slice::<Request>(&request) {
            Ok(request) => {
                if request.method_calls.len() < core.store.config.max_calls_in_request {
                    let result = handle_method_calls(request, core, session).await;
                    //println!("{}", serde_json::to_string_pretty(&result).unwrap());

                    Ok(HttpResponse::build(StatusCode::OK)
                        .insert_header(ContentType::json())
                        .json(result))
                } else {
                    Err(RequestError::limit(RequestLimitError::CallsIn))
                }
            }
            Err(err) => {
                debug!("Failed to parse request: {}", err);

                Err(RequestError::not_request())
            }
        }
    } else {
        Err(RequestError::limit(RequestLimitError::Size))
    }
}
