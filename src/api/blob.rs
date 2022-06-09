use actix_web::http::header::ContentType;
use actix_web::HttpRequest;
use actix_web::{http::StatusCode, web, HttpResponse};
use jmap::jmap_store::blob::JMAPBlobStore;
use jmap::types::jmap::JMAPId;

use jmap::{error::problem_details::ProblemDetails, types::blob::JMAPBlob};
use jmap_mail::mail::parse::get_message_part;
use reqwest::header::CONTENT_TYPE;
use store::{tracing::error, Store};

use crate::JMAPServer;

#[derive(serde::Deserialize)]
pub struct Params {
    accept: String,
}

pub async fn handle_jmap_download<T>(
    path: web::Path<(JMAPId, JMAPBlob, String)>,
    params: web::Query<Params>,
    core: web::Data<JMAPServer<T>>,
) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    let (account_id, blob_id, filename) = path.into_inner();

    let store = core.store.clone();
    let error = match core
        .spawn_worker(move || {
            store.blob_jmap_get(account_id.get_document_id(), &blob_id, get_message_part)
        })
        .await
    {
        Ok(Some(bytes)) => {
            return HttpResponse::build(StatusCode::OK)
                .insert_header(("Content-Type", params.into_inner().accept))
                .insert_header((
                    "Content-Disposition",
                    format!("attachment; filename=\"{}\"", filename), //TODO escape filename
                ))
                .insert_header(("Cache-Control", "private, immutable, max-age=31536000"))
                .body(bytes);
        }
        Ok(None) => ProblemDetails::not_found(),
        Err(err) => {
            error!("Blob download failed: {:?}", err);
            ProblemDetails::internal_server_error()
        }
    };

    HttpResponse::build(StatusCode::from_u16(error.status).unwrap())
        .insert_header(("Content-Type", "application/problem+json"))
        .body(error.to_json())
}

#[derive(Debug, serde::Serialize)]
struct UploadResponse {
    #[serde(rename(serialize = "accountId"))]
    account_id: JMAPId,
    #[serde(rename(serialize = "blobId"))]
    blob_id: JMAPBlob,
    #[serde(rename(serialize = "type"))]
    c_type: String,
    size: usize,
}

pub async fn handle_jmap_upload<T>(
    path: web::Path<(JMAPId,)>,
    request: HttpRequest,
    bytes: web::Bytes,
    core: web::Data<JMAPServer<T>>,
) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    let (account_id,) = path.into_inner();
    let store = core.store.clone();
    let size = bytes.len();
    let error = match core
        .spawn_worker(move || store.blob_store_ephimeral(account_id.get_document_id(), &bytes))
        .await
    {
        Ok(blob_id) => {
            return HttpResponse::build(StatusCode::OK)
                .insert_header(ContentType::json())
                .json(UploadResponse {
                    account_id,
                    blob_id,
                    c_type: request
                        .headers()
                        .get(CONTENT_TYPE)
                        .and_then(|h| h.to_str().ok())
                        .unwrap_or("application/octet-stream")
                        .to_string(),
                    size,
                });
        }
        Err(err) => {
            error!("Blob upload failed: {:?}", err);
            ProblemDetails::internal_server_error()
        }
    };

    println!("{:?}", error);

    HttpResponse::build(StatusCode::from_u16(error.status).unwrap())
        .insert_header(("Content-Type", "application/problem+json"))
        .body(error.to_json())
}
