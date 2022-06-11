use actix_web::http::header::ContentType;
use actix_web::HttpRequest;
use actix_web::{http::StatusCode, web, HttpResponse};
use jmap::principal::account::JMAPAccountStore;
use jmap::types::jmap::JMAPId;

use jmap::types::blob::JMAPBlob;
use jmap_mail::mail::parse::get_message_part;
use jmap_mail::mail::sharing::JMAPShareMail;
use reqwest::header::CONTENT_TYPE;
use store::core::collection::Collection;
use store::{tracing::error, Store};

use crate::authorization::Session;
use crate::JMAPServer;

use super::RequestError;

#[derive(serde::Deserialize)]
pub struct Params {
    accept: String,
}

enum Response {
    Blob(Vec<u8>),
    Unauthorized,
    NotFound,
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
            let member_of = store.get_member_accounts(session.account_id())?;
            if !member_of.contains(&account_id) {
                if let Some(shared_ids) =
                    store.mail_shared_messages(account_id, &member_of)?.as_ref()
                {
                    if !store.blob_document_has_access(
                        &blob_id.id,
                        account_id,
                        Collection::Mail,
                        shared_ids,
                    )? {
                        return Ok(Response::Unauthorized);
                    }
                } else {
                    return Ok(Response::Unauthorized);
                }
            }

            let bytes = store.blob_get(&blob_id.id)?;
            Ok(
                if let (Some(bytes), Some(inner_id)) = (&bytes, blob_id.inner_id) {
                    get_message_part(bytes, inner_id).map(|bytes| bytes.into_owned())
                } else {
                    bytes
                }
                .map(Response::Blob)
                .unwrap_or(Response::NotFound),
            )
        })
        .await
    {
        Ok(Response::Blob(bytes)) => {
            Ok(HttpResponse::build(StatusCode::OK)
                .insert_header(("Content-Type", params.into_inner().accept))
                .insert_header((
                    "Content-Disposition",
                    format!("attachment; filename=\"{}\"", filename), //TODO escape filename
                ))
                .insert_header(("Cache-Control", "private, immutable, max-age=31536000"))
                .body(bytes))
        }
        Ok(Response::NotFound) => Err(RequestError::not_found()),
        Ok(Response::Unauthorized) => Err(RequestError::forbidden()),
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
                    .get_member_accounts(session.account_id())?
                    .contains(&account_id)
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
