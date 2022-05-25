use actix_web::web;

use jmap::{
    error::method::MethodError,
    push_subscription::{get::JMAPGetPushSubscription, set::JMAPSetPushSubscription},
};
use jmap_mail::{
    email_submission::{
        changes::JMAPEmailSubmissionChanges, get::JMAPGetEmailSubmission,
        query::JMAPEmailSubmissionQuery, set::JMAPSetEmailSubmission,
    },
    identity::{changes::JMAPIdentityChanges, get::JMAPGetIdentity, set::JMAPSetIdentity},
    mail::{
        changes::JMAPMailChanges, get::JMAPGetMail, import::JMAPMailImport, parse::JMAPMailParse,
        query::JMAPMailQuery, set::JMAPSetMail,
    },
    mailbox::{
        changes::JMAPMailboxChanges, get::JMAPGetMailbox, query::JMAPMailboxQuery,
        set::JMAPSetMailbox,
    },
    thread::{changes::JMAPThreadChanges, get::JMAPGetThread},
    vacation_response::{get::JMAPGetVacationResponse, set::JMAPSetVacationResponse},
};
use store::{tracing::error, Store};

use crate::JMAPServer;

use super::{method, request::Request, response::Response};

pub async fn handle_method_calls<T>(request: Request, core: web::Data<JMAPServer<T>>) -> Response
where
    T: for<'x> Store<'x> + 'static,
{
    let include_created_ids = request.created_ids.is_some();
    let mut response = Response::new(
        1234, //TODO
        request.created_ids.unwrap_or_default(),
        request.method_calls.len(),
    );

    for call in request.method_calls.into_iter() {
        let call_id = call.id;
        let mut call_method = call.method;

        loop {
            // Prepare request
            if let Err(err) = call_method.prepare_request(&response) {
                response.push_error(call_id, err);
                break;
            }

            // Execute request
            match handle_method_call(call_method, &core).await {
                Ok(mut method_response) => {
                    let next_call_method = match method_response.changes() {
                        method::Changes::Item {
                            created_ids,
                            change_id,
                            state_change,
                            next_call,
                        } => {
                            // Commit change
                            if core.is_in_cluster() && !core.commit_index(change_id).await {
                                response.push_error(call_id, MethodError::ServerPartialFail);
                                break;
                            }

                            // Broadcast change to subscribers
                            if let Some(state_change) = state_change {
                                if let Err(err) = core.publish_state_change(state_change).await {
                                    error!("Failed to publish state change: {}", err);
                                    response.push_error(call_id, MethodError::ServerPartialFail);
                                    break;
                                }
                            }

                            // Add created ids to response
                            if let Some(created_ids) = created_ids {
                                response.created_ids.extend(created_ids);
                            }

                            // Add response
                            response.push_response(call_id.clone(), method_response);

                            next_call
                        }
                        method::Changes::Subscription {
                            account_id,
                            change_id,
                        } => {
                            // Commit change
                            if core.is_in_cluster() && !core.commit_index(change_id).await {
                                response.push_error(call_id, MethodError::ServerPartialFail);
                                break;
                            }

                            // Broadcast change to subscribers
                            if let Err(err) = core.update_push_subscriptions(account_id).await {
                                error!("Failed to publish state change: {}", err);
                                response.push_error(call_id, MethodError::ServerPartialFail);
                                break;
                            }

                            None
                        }
                        method::Changes::None => None,
                    };

                    // Process next call
                    if let Some(next_call_method) = next_call_method {
                        call_method = next_call_method;
                    } else {
                        break;
                    }
                }
                Err(err) => {
                    response.push_error(call_id, err);
                    break;
                }
            }
        }
    }

    if !include_created_ids {
        response.created_ids.clear();
    }

    response
}

pub async fn handle_method_call<T>(
    call: method::Request,
    core: &web::Data<JMAPServer<T>>,
) -> jmap::Result<method::Response>
where
    T: for<'x> Store<'x> + 'static,
{
    let store = core.store.clone();
    core.spawn_jmap_request(move || {
        Ok(match call {
            method::Request::GetPushSubscription(request) => {
                method::Response::GetPushSubscription(store.push_subscription_get(request)?)
            }
            method::Request::SetPushSubscription(request) => {
                method::Response::SetPushSubscription(store.push_subscription_set(request)?)
            }
            method::Request::GetMailbox(request) => {
                method::Response::GetMailbox(store.mailbox_get(request)?)
            }
            method::Request::ChangesMailbox(request) => {
                method::Response::ChangesMailbox(store.mailbox_changes(request)?)
            }
            method::Request::QueryMailbox(request) => {
                method::Response::QueryMailbox(store.mailbox_query(request)?)
            }
            method::Request::QueryChangesMailbox(request) => {
                method::Response::QueryChangesMailbox(store.mailbox_query_changes(request)?)
            }
            method::Request::SetMailbox(request) => {
                method::Response::SetMailbox(store.mailbox_set(request)?)
            }
            method::Request::GetThread(request) => {
                method::Response::GetThread(store.thread_get(request)?)
            }
            method::Request::ChangesThread(request) => {
                method::Response::ChangesThread(store.thread_changes(request)?)
            }
            method::Request::GetEmail(request) => {
                method::Response::GetEmail(store.mail_get(request)?)
            }
            method::Request::ChangesEmail(request) => {
                method::Response::ChangesEmail(store.mail_changes(request)?)
            }
            method::Request::QueryEmail(request) => {
                method::Response::QueryEmail(store.mail_query(request)?)
            }
            method::Request::QueryChangesEmail(request) => {
                method::Response::QueryChangesEmail(store.mail_query_changes(request)?)
            }
            method::Request::SetEmail(request) => {
                method::Response::SetEmail(store.mail_set(request)?)
            }
            /*method::Request::CopyEmail(request) => {
                method::Response::CopyEmail(store.mail_copy(request)?)
            }*/
            method::Request::ImportEmail(request) => {
                method::Response::ImportEmail(store.mail_import(request)?)
            }
            method::Request::ParseEmail(request) => {
                method::Response::ParseEmail(store.mail_parse(request)?)
            }
            method::Request::GetIdentity(request) => {
                method::Response::GetIdentity(store.identity_get(request)?)
            }
            method::Request::ChangesIdentity(request) => {
                method::Response::ChangesIdentity(store.identity_changes(request)?)
            }
            method::Request::SetIdentity(request) => {
                method::Response::SetIdentity(store.identity_set(request)?)
            }
            method::Request::GetEmailSubmission(request) => {
                method::Response::GetEmailSubmission(store.email_submission_get(request)?)
            }
            method::Request::ChangesEmailSubmission(request) => {
                method::Response::ChangesEmailSubmission(store.email_submission_changes(request)?)
            }
            method::Request::QueryEmailSubmission(request) => {
                method::Response::QueryEmailSubmission(store.email_submission_query(request)?)
            }
            method::Request::QueryChangesEmailSubmission(request) => {
                method::Response::QueryChangesEmailSubmission(
                    store.email_submission_query_changes(request)?,
                )
            }
            method::Request::SetEmailSubmission(request) => {
                method::Response::SetEmailSubmission(store.email_submission_set(request)?)
            }
            method::Request::GetVacationResponse(request) => {
                method::Response::GetVacationResponse(store.vacation_response_get(request)?)
            }
            method::Request::SetVacationResponse(request) => {
                method::Response::SetVacationResponse(store.vacation_response_set(request)?)
            }
            method::Request::Echo(payload) => method::Response::Echo(payload),
            method::Request::Error(err) => return Err(err),
        })
    })
    .await
}
