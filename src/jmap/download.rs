use actix_web::{http::StatusCode, web, HttpResponse};
use jmap::{
    blob::JMAPBlobStore,
    id::{BlobId, JMAPIdSerialize},
    ProblemDetails,
};
use jmap_mail::parse::get_message_blob;
use store::{tracing::error, AccountId, JMAPId, Store};

use super::server::JMAPServer;

#[derive(serde::Deserialize)]
pub struct Params {
    accept: String,
}

pub async fn handle_jmap_download<T>(
    path: web::Path<(String, String, String)>,
    params: web::Query<Params>,
    core: web::Data<JMAPServer<T>>,
) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    let error = if let (Some(account_id), Some(blob_id)) = (
        JMAPId::from_jmap_string(&path.0),
        BlobId::from_jmap_string(&path.1),
    ) {
        let account_id = account_id as AccountId;
        if blob_id.owner_id() == account_id {
            let store = core.store.clone();
            match core
                .spawn_worker(move || store.download_blob(account_id, &blob_id, get_message_blob))
                .await
            {
                Ok(Some(bytes)) => {
                    return HttpResponse::build(StatusCode::OK)
                        .insert_header(("Content-Type", params.into_inner().accept))
                        .insert_header((
                            "Content-Disposition",
                            format!("attachment; filename=\"{}\"", path.2), //TODO escape filename
                        ))
                        .insert_header(("Cache-Control", "private, immutable, max-age=31536000"))
                        .body(bytes);
                }
                Ok(None) => ProblemDetails::not_found(),
                Err(err) => {
                    error!("Blob download failed: {:?}", err);
                    ProblemDetails::internal_server_error()
                }
            }
        } else {
            ProblemDetails::forbidden()
        }
    } else {
        ProblemDetails::invalid_parameters()
    };

    HttpResponse::build(StatusCode::from_u16(error.status).unwrap())
        .insert_header(("Content-Type", "application/problem+json"))
        .body(error.to_json())
}
