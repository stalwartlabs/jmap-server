use super::RequestError;
use crate::authorization::Session;
use crate::JMAPServer;
use actix_web::http::header::ContentType;
use actix_web::HttpRequest;
use actix_web::{http::StatusCode, web, HttpResponse};
use jmap::request::ACLEnforce;
use jmap::types::blob::JMAPBlob;
use jmap::types::jmap::JMAPId;
use jmap_mail::mail::get::{BlobResult, JMAPGetMail};
use jmap_sharing::principal::account::JMAPAccountStore;
use reqwest::header::CONTENT_TYPE;
use store::{tracing::error, Store};

#[derive(serde::Deserialize)]
pub struct Params {
    accept: String,
}

pub async fn handle_jmap_download<T>(
    path: web::Path<(JMAPId, JMAPBlob, String)>,
    params: web::Query<Params>,
    core: web::Data<JMAPServer<T>>,
    session: Session,
) -> Result<HttpResponse, RequestError>
where
    T: for<'x> Store<'x> + 'static,
{
    // Enforce access control
    let (id, blob_id, filename) = path.into_inner();
    let account_id = id.get_document_id();

    let store = core.store.clone();
    match core
        .spawn_worker(move || {
            store.mail_blob_get(
                account_id,
                &store.get_acl_token(session.account_id())?,
                &blob_id,
            )
        })
        .await
    {
        Ok(BlobResult::Blob(bytes)) => {
            Ok(HttpResponse::build(StatusCode::OK)
                .insert_header(("Content-Type", params.into_inner().accept))
                .insert_header((
                    "Content-Disposition",
                    format!("attachment; filename=\"{}\"", filename), //TODO escape filename
                ))
                .insert_header(("Cache-Control", "private, immutable, max-age=31536000"))
                .body(bytes))
        }
        Ok(BlobResult::NotFound) => Err(RequestError::not_found()),
        Ok(BlobResult::Unauthorized) => Err(RequestError::forbidden()),
        Err(err) => {
            error!("Blob download failed: {:?}", err);
            Err(RequestError::internal_server_error())
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
    session: Session,
) -> Result<HttpResponse, RequestError>
where
    T: for<'x> Store<'x> + 'static,
{
    let (id,) = path.into_inner();
    let account_id = id.get_document_id();

    let store = core.store.clone();
    let size = bytes.len();
    match core
        .spawn_worker(move || {
            Ok(
                if store
                    .get_acl_token(session.account_id())?
                    .is_member(account_id)
                {
                    let blob_id = store.blob_store(&bytes)?;
                    store.blob_link_ephimeral(&blob_id, account_id)?;
                    JMAPBlob::new(blob_id).into()
                } else {
                    None
                },
            )
        })
        .await
    {
        Ok(Some(blob_id)) => Ok(HttpResponse::build(StatusCode::OK)
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
        Ok(None) => Err(RequestError::forbidden()),
        Err(err) => {
            error!("Blob upload failed: {:?}", err);
            Err(RequestError::internal_server_error())
        }
    }
}
