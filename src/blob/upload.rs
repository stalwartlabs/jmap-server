use actix_web::{
    http::{header::ContentType, StatusCode},
    web, HttpRequest, HttpResponse,
};
use jmap::{
    error::problem_details::ProblemDetails,
    types::{blob::JMAPBlob, jmap::JMAPId},
    jmap_store::blob::JMAPBlobStore,
};
use reqwest::header::CONTENT_TYPE;
use store::{tracing::error, Store};

use crate::JMAPServer;

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

impl UploadResponse {
    pub fn to_json(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
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
                .body(
                    UploadResponse {
                        account_id,
                        blob_id,
                        c_type: request
                            .headers()
                            .get(CONTENT_TYPE)
                            .and_then(|h| h.to_str().ok())
                            .unwrap_or("application/octet-stream")
                            .to_string(),
                        size,
                    }
                    .to_json(),
                );
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
