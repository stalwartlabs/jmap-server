use actix_web::http::header::ContentType;
use actix_web::HttpRequest;
use actix_web::{http::StatusCode, web, HttpResponse};
use jmap::types::jmap::JMAPId;

use jmap::types::blob::JMAPBlob;
use jmap_mail::mail::parse::get_message_part;
use jmap_mail::mail::sharing::JMAPShareMail;
use reqwest::header::CONTENT_TYPE;
use store::core::collection::Collection;
use store::{tracing::error, Store};

use crate::authorization::auth::Authorized;
use crate::JMAPServer;

use super::ProblemDetails;

#[derive(serde::Deserialize)]
pub struct Params {
    accept: String,
}

pub async fn handle_jmap_download<T>(
    path: web::Path<(JMAPId, JMAPBlob, String)>,
    params: web::Query<Params>,
    core: web::Data<JMAPServer<T>>,
    session: Authorized,
) -> Result<HttpResponse, ProblemDetails>
where
    T: for<'x> Store<'x> + 'static,
{
    // Enforce rate limits
    let _req = session.assert_concurrent_requests(core.store.config.max_concurrent_requests)?;
    let session = session.clone();

    // Enforce access control
    let (id, blob_id, filename) = path.into_inner();
    let account_id = id.get_document_id();

    let store = core.store.clone();
    match core
        .spawn_worker(move || {
            if !session.is_owner(account_id) {
                if let Some(shared_ids) = store
                    .mail_shared_messages(account_id, session.member_of())?
                    .as_ref()
                {
                    if !store.blob_document_has_access(
                        &blob_id.id,
                        account_id,
                        Collection::Mail,
                        shared_ids,
                    )? {
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            }

            let bytes = store.blob_get(&blob_id.id)?;
            Ok(
                if let (Some(bytes), Some(inner_id)) = (&bytes, blob_id.inner_id) {
                    get_message_part(bytes, inner_id).map(|bytes| bytes.into_owned())
                } else {
                    bytes
                },
            )
        })
        .await
    {
        Ok(Some(bytes)) => {
            Ok(HttpResponse::build(StatusCode::OK)
                .insert_header(("Content-Type", params.into_inner().accept))
                .insert_header((
                    "Content-Disposition",
                    format!("attachment; filename=\"{}\"", filename), //TODO escape filename
                ))
                .insert_header(("Cache-Control", "private, immutable, max-age=31536000"))
                .body(bytes))
        }
        Ok(None) => Err(ProblemDetails::not_found()),
        Err(err) => {
            error!("Blob download failed: {:?}", err);
            Err(ProblemDetails::internal_server_error())
        }
    }
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
    session: Authorized,
) -> Result<HttpResponse, ProblemDetails>
where
    T: for<'x> Store<'x> + 'static,
{
    // Enforce rate limits
    let _req = session.assert_concurrent_requests(core.store.config.max_concurrent_requests)?;

    // Enforce access control
    let (id,) = path.into_inner();
    let account_id = id.get_document_id();
    session.assert_is_owner(account_id)?;

    let store = core.store.clone();
    let size = bytes.len();
    match core
        .spawn_worker(move || {
            let blob_id = store.blob_store(&bytes)?;
            store.blob_link_ephimeral(&blob_id, account_id)?;
            Ok(JMAPBlob::new(blob_id))
        })
        .await
    {
        Ok(blob_id) => Ok(HttpResponse::build(StatusCode::OK)
            .insert_header(ContentType::json())
            .json(UploadResponse {
                account_id: id,
                blob_id,
                c_type: request
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|h| h.to_str().ok())
                    .unwrap_or("application/octet-stream")
                    .to_string(),
                size,
            })),
        Err(err) => {
            error!("Blob upload failed: {:?}", err);
            Err(ProblemDetails::internal_server_error())
        }
    }
}
