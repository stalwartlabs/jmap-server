/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use super::{blob::JMAPBlobCopy, method, request::Request, response::Response};
use crate::{authorization::Session, services::email_delivery, JMAPServer};
use actix_web::web;
use jmap::{
    error::method::MethodError,
    push_subscription::{get::JMAPGetPushSubscription, set::JMAPSetPushSubscription},
    request::ACLEnforce,
    SUPERUSER_ID,
};
use jmap_mail::{
    email_submission::{
        changes::JMAPEmailSubmissionChanges, get::JMAPGetEmailSubmission,
        query::JMAPEmailSubmissionQuery, set::JMAPSetEmailSubmission,
    },
    identity::{changes::JMAPIdentityChanges, get::JMAPGetIdentity, set::JMAPSetIdentity},
    mail::{
        changes::JMAPMailChanges, copy::JMAPCopyMail, get::JMAPGetMail, import::JMAPMailImport,
        parse::JMAPMailParse, query::JMAPMailQuery, search_snippet::JMAPMailSearchSnippet,
        set::JMAPSetMail,
    },
    mailbox::{
        changes::JMAPMailboxChanges, get::JMAPGetMailbox, query::JMAPMailboxQuery,
        set::JMAPSetMailbox,
    },
    thread::{changes::JMAPThreadChanges, get::JMAPGetThread},
    vacation_response::{get::JMAPGetVacationResponse, set::JMAPSetVacationResponse},
};
use jmap_sharing::principal::{
    account::JMAPAccountStore, get::JMAPGetPrincipal, query::JMAPPrincipalQuery,
    set::JMAPSetPrincipal,
};
use store::{core::collection::Collection, tracing::error, AccountId, Store};

pub async fn handle_method_calls<T>(
    request: Request,
    core: web::Data<JMAPServer<T>>,
    session: Session,
) -> Response
where
    T: for<'x> Store<'x> + 'static,
{
    let include_created_ids = request.created_ids.is_some();
    let mut response = Response::new(
        session.state(),
        request.created_ids.unwrap_or_default(),
        request.method_calls.len(),
    );

    for call in request.method_calls.into_iter() {
        let call_id = call.id;
        let mut call_method = call.method;

        loop {
            // Make sure this node is up to date to handle the request.
            if !core.is_leader() && !core.is_up_to_date() {
                response.push_error(call_id, MethodError::ServerUnavailable);
                break;
            }

            // Prepare request
            if let Err(err) = call_method.prepare_request(&response) {
                response.push_error(call_id, err);
                break;
            }

            // Execute request
            match handle_method_call(call_method, &core, session.account_id()).await {
                Ok(mut method_response) => {
                    let next_call_method = match method_response.changes() {
                        method::Changes::Item {
                            created_ids,
                            change_id,
                            state_change,
                            next_call,
                        } => {
                            // Commit change
                            if core.is_in_cluster()
                                && (!core.is_leader() || !core.commit_index(change_id).await)
                            {
                                response.push_error(call_id, MethodError::ServerPartialFail);
                                break;
                            }

                            // Broadcast change to subscribers
                            if let Some(state_change) = state_change {
                                if let Err(err) = core.publish_state_change(state_change).await {
                                    error!("Failed to publish state change: {}", err);
                                }
                            }

                            // Notify E-mail delivery service of changes
                            match &method_response {
                                method::Response::SetEmailSubmission(submission_response) => {
                                    if let Err(err) = core
                                        .notify_email_delivery(
                                            email_delivery::Event::new_submission(
                                                submission_response.account_id(),
                                                created_ids
                                                    .as_ref()
                                                    .map(|created_ids| {
                                                        created_ids
                                                            .values()
                                                            .map(|id| id.get_document_id())
                                                            .collect()
                                                    })
                                                    .unwrap_or_default(),
                                                submission_response
                                                    .updated
                                                    .keys()
                                                    .map(|id| id.get_document_id())
                                                    .collect(),
                                            ),
                                        )
                                        .await
                                    {
                                        error!("No e-mail delivery configured or something else happened: {}", err);
                                    }
                                }
                                method::Response::SetPrincipal(_) => {
                                    core.notify_email_delivery(email_delivery::Event::Reload)
                                        .await
                                        .ok();
                                }
                                _ => {}
                            }

                            // Add created ids to response
                            if let Some(created_ids) = created_ids {
                                response.created_ids.extend(created_ids);
                            }

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

                    // Add response
                    response.push_response(call_id.clone(), method_response);

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
    account_id: AccountId,
) -> jmap::Result<method::Response>
where
    T: for<'x> Store<'x> + 'static,
{
    let store = core.store.clone();
    core.spawn_jmap_request(move || {
        Ok(match call {
            method::Request::CopyBlob(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_has_access(request.account_id.get_document_id(), Collection::Mail)?
                    .assert_has_access(request.from_account_id.get_document_id(), Collection::Mail)?
                    .into();
                method::Response::CopyBlob(store.copy_blob(request)?)
            }
            method::Request::GetPushSubscription(mut request) => {
                request.account_id = account_id.into();
                request.acl = store.get_acl_token(account_id)?.into();
                method::Response::GetPushSubscription(store.push_subscription_get(request)?)
            }
            method::Request::SetPushSubscription(mut request) => {
                request.account_id = account_id.into();
                request.acl = store.get_acl_token(account_id)?.into();
                method::Response::SetPushSubscription(store.push_subscription_set(request)?)
            }
            method::Request::GetMailbox(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_has_access(request.account_id.get_document_id(), Collection::Mailbox)?
                    .into();
                method::Response::GetMailbox(store.mailbox_get(request)?)
            }
            method::Request::ChangesMailbox(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_has_access(request.account_id.get_document_id(), Collection::Mailbox)?
                    .into();
                method::Response::ChangesMailbox(store.mailbox_changes(request)?)
            }
            method::Request::QueryMailbox(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_has_access(request.account_id.get_document_id(), Collection::Mailbox)?
                    .into();
                method::Response::QueryMailbox(store.mailbox_query(request)?)
            }
            method::Request::QueryChangesMailbox(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_has_access(request.account_id.get_document_id(), Collection::Mailbox)?
                    .into();
                method::Response::QueryChangesMailbox(store.mailbox_query_changes(request)?)
            }
            method::Request::SetMailbox(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_has_access(request.account_id.get_document_id(), Collection::Mailbox)?
                    .into();
                method::Response::SetMailbox(store.mailbox_set(request)?)
            }
            method::Request::GetThread(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_has_access(request.account_id.get_document_id(), Collection::Mail)?
                    .into();
                method::Response::GetThread(store.thread_get(request)?)
            }
            method::Request::ChangesThread(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_has_access(request.account_id.get_document_id(), Collection::Mail)?
                    .into();
                method::Response::ChangesThread(store.thread_changes(request)?)
            }
            method::Request::GetEmail(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_has_access(request.account_id.get_document_id(), Collection::Mail)?
                    .into();
                method::Response::GetEmail(store.mail_get(request)?)
            }
            method::Request::ChangesEmail(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_has_access(request.account_id.get_document_id(), Collection::Mail)?
                    .into();
                method::Response::ChangesEmail(store.mail_changes(request)?)
            }
            method::Request::QueryEmail(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_has_access(request.account_id.get_document_id(), Collection::Mail)?
                    .into();
                method::Response::QueryEmail(store.mail_query(request)?)
            }
            method::Request::QueryChangesEmail(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_has_access(request.account_id.get_document_id(), Collection::Mail)?
                    .into();
                method::Response::QueryChangesEmail(store.mail_query_changes(request)?)
            }
            method::Request::SetEmail(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_has_access(request.account_id.get_document_id(), Collection::Mail)?
                    .into();
                method::Response::SetEmail(store.mail_set(request)?)
            }
            method::Request::CopyEmail(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_has_access(request.account_id.get_document_id(), Collection::Mailbox)?
                    .assert_has_access(request.from_account_id.get_document_id(), Collection::Mail)?
                    .into();
                method::Response::CopyEmail(store.mail_copy(request)?)
            }
            method::Request::ImportEmail(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_has_access(request.account_id.get_document_id(), Collection::Mail)?
                    .into();
                method::Response::ImportEmail(store.mail_import(request)?)
            }
            method::Request::ParseEmail(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_has_access(request.account_id.get_document_id(), Collection::Mail)?
                    .into();
                method::Response::ParseEmail(store.mail_parse(request)?)
            }
            method::Request::GetSearchSnippet(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_has_access(request.account_id.get_document_id(), Collection::Mail)?
                    .into();
                method::Response::GetSearchSnippet(store.mail_search_snippet(request)?)
            }
            method::Request::GetIdentity(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_is_member(request.account_id.get_document_id())?
                    .into();
                method::Response::GetIdentity(store.identity_get(request)?)
            }
            method::Request::ChangesIdentity(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_is_member(request.account_id.get_document_id())?
                    .into();
                method::Response::ChangesIdentity(store.identity_changes(request)?)
            }
            method::Request::SetIdentity(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_is_member(request.account_id.get_document_id())?
                    .into();
                method::Response::SetIdentity(store.identity_set(request)?)
            }
            method::Request::GetEmailSubmission(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_is_member(request.account_id.get_document_id())?
                    .into();
                method::Response::GetEmailSubmission(store.email_submission_get(request)?)
            }
            method::Request::ChangesEmailSubmission(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_is_member(request.account_id.get_document_id())?
                    .into();
                method::Response::ChangesEmailSubmission(store.email_submission_changes(request)?)
            }
            method::Request::QueryEmailSubmission(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_is_member(request.account_id.get_document_id())?
                    .into();
                method::Response::QueryEmailSubmission(store.email_submission_query(request)?)
            }
            method::Request::QueryChangesEmailSubmission(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_is_member(request.account_id.get_document_id())?
                    .into();
                method::Response::QueryChangesEmailSubmission(
                    store.email_submission_query_changes(request)?,
                )
            }
            method::Request::SetEmailSubmission(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_is_member(request.account_id.get_document_id())?
                    .into();
                method::Response::SetEmailSubmission(store.email_submission_set(request)?)
            }
            method::Request::GetVacationResponse(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_is_member(request.account_id.get_document_id())?
                    .into();
                method::Response::GetVacationResponse(store.vacation_response_get(request)?)
            }
            method::Request::SetVacationResponse(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_is_member(request.account_id.get_document_id())?
                    .into();
                method::Response::SetVacationResponse(store.vacation_response_set(request)?)
            }
            method::Request::GetPrincipal(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_is_member(SUPERUSER_ID)?
                    .into();
                method::Response::GetPrincipal(store.principal_get(request)?)
            }
            method::Request::QueryPrincipal(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_is_member(SUPERUSER_ID)?
                    .into();
                method::Response::QueryPrincipal(store.principal_query(request)?)
            }
            method::Request::SetPrincipal(mut request) => {
                request.acl = store
                    .get_acl_token(account_id)?
                    .assert_is_member(SUPERUSER_ID)?
                    .into();
                method::Response::SetPrincipal(store.principal_set(request)?)
            }
            method::Request::Echo(payload) => method::Response::Echo(payload),
            method::Request::Error(err) => return Err(err),
        })
    })
    .await
}
