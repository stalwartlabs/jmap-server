use actix_web::{
    http::{header::ContentType, StatusCode},
    web, HttpResponse,
};
use jmap::types::type_state::TypeState;
use jmap_mail::mail::ingest::{JMAPMailIngest, Status};
use store::{
    log::changes::ChangeId,
    tracing::{debug, error},
    Store,
};

use crate::{
    services::{email_delivery, state_change::StateChange},
    JMAPServer,
};

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
pub struct Params {
    from: Option<String>,
    to: String,
    api_key: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Dsn {
    to: String,
    status: DeliveryStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum DeliveryStatus {
    #[serde(rename = "success")]
    Success,
    #[serde(rename = "failure")]
    Failure,
    #[serde(rename = "temporary_failure")]
    TemporaryFailure,
}

pub async fn handle_ingest<T>(
    params: web::Query<Params>,
    bytes: web::Bytes,
    core: web::Data<JMAPServer<T>>,
) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    // Validate API key
    if core.store.config.api_key.is_empty() || core.store.config.api_key != params.api_key {
        debug!("Invalid API key");
        return HttpResponse::Unauthorized().finish();
    }

    // Ingest message
    let store = core.store.clone();
    let results = core
        .spawn_worker(move || Ok(store.mail_ingest(vec![1], bytes.to_vec())))
        .await
        .unwrap();

    // Prepare response
    let mut response = Vec::with_capacity(params.to.len());
    let mut change_id = ChangeId::MAX;
    let mut status_code = StatusCode::OK;

    for result in results {
        match result {
            Status::Success {
                account_id,
                changes,
                vacation_response,
            } => {
                // Send vacation response
                if let Some(vacation_response) = vacation_response {
                    if let Err(err) = core
                        .notify_email_delivery(email_delivery::Event::vacation_response(
                            vacation_response.from,
                            vacation_response.to,
                            vacation_response.message,
                        ))
                        .await
                    {
                        error!(
                            "No e-mail delivery configured or something else happened: {}",
                            err
                        );
                    }
                }

                // Update the change id
                change_id = changes.change_id;

                // Publish state change
                if let Err(err) = core
                    .publish_state_change(StateChange::new(
                        account_id,
                        changes
                            .collections
                            .into_iter()
                            .filter_map(|c| {
                                Some((
                                    match TypeState::try_from(c).ok()? {
                                        TypeState::Email => TypeState::EmailDelivery,
                                        ts => ts,
                                    },
                                    change_id,
                                ))
                            })
                            .collect(),
                    ))
                    .await
                {
                    error!("Failed to publish state change: {}", err);
                }

                response.push(Dsn {
                    to: "jdoe@example.com".to_string(), //TODO
                    status: DeliveryStatus::Success,
                    reason: None,
                });
            }
            Status::Failure {
                account_id,
                permanent,
                reason,
            } => {
                response.push(Dsn {
                    to: "jdoe@example.com".to_string(),
                    status: if permanent {
                        DeliveryStatus::Failure
                    } else {
                        status_code = StatusCode::SERVICE_UNAVAILABLE;
                        DeliveryStatus::TemporaryFailure
                    },
                    reason: reason.into(),
                });
            }
        }
    }

    // Commit change
    if change_id != ChangeId::MAX && core.is_in_cluster() && !core.commit_index(change_id).await {
        response = response
            .into_iter()
            .map(|r| {
                if let DeliveryStatus::Success = r.status {
                    Dsn {
                        to: r.to,
                        status: DeliveryStatus::TemporaryFailure,
                        reason: "Failed to commit changes to cluster.".to_string().into(),
                    }
                } else {
                    r
                }
            })
            .collect();
        status_code = StatusCode::SERVICE_UNAVAILABLE;
    }

    HttpResponse::build(status_code)
        .insert_header(ContentType::json())
        .json(response)
}
