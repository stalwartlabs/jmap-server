use actix_web::{
    http::{header::ContentType, StatusCode},
    web, HttpRequest, HttpResponse,
};
use jmap::{
    error::problem_details::ProblemDetails, id::JMAPIdSerialize, jmap_store::blob::JMAPBlobStore,
};
use reqwest::header::CONTENT_TYPE;
use store::{tracing::error, AccountId, JMAPId, Store};

use crate::JMAPServer;

#[derive(Debug, serde::Serialize)]
struct UploadResponse {
    #[serde(rename(serialize = "accountId"))]
    account_id: String,
    #[serde(rename(serialize = "blobId"))]
    blob_id: String,
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
    path: web::Path<(String,)>,
    request: HttpRequest,
    bytes: web::Bytes,
    core: web::Data<JMAPServer<T>>,
) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    let error = if let Some(account_id) = JMAPId::from_jmap_string(&path.0) {
        let store = core.store.clone();
        let size = bytes.len();
        match core
            .spawn_worker(move || store.blob_store_ephimeral(account_id as AccountId, &bytes))
            .await
        {
            Ok(blob_id) => {
                return HttpResponse::build(StatusCode::OK)
                    .insert_header(ContentType::json())
                    .body(
                        UploadResponse {
                            account_id: path.into_inner().0,
                            blob_id: blob_id.to_jmap_string(),
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
        }
    } else {
        ProblemDetails::invalid_parameters()
    };

    println!("{:?}", error);

    HttpResponse::build(StatusCode::from_u16(error.status).unwrap())
        .insert_header(("Content-Type", "application/problem+json"))
        .body(error.to_json())
}
