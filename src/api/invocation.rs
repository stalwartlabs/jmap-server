use actix_web::web;
use jmap::{
    error::method::MethodError,
    jmap_store::{
        changes::{ChangesResult, JMAPChanges},
        get::{GetResult, JMAPGet},
        import::{ImportResult, JMAPImport},
        parse::{JMAPParse, ParseResult},
        query::{JMAPQuery, QueryResult},
        query_changes::{JMAPQueryChanges, QueryChangesResult},
        set::{JMAPSet, SetResult},
    },
    protocol::{
        invocation::{Invocation, Method, Object},
        json::JSONValue,
        request::Request,
        response::Response,
    },
    push_subscription::{get::GetPushSubscription, set::SetPushSubscription},
};
use jmap_mail::{
    identity::{changes::ChangesIdentity, get::GetIdentity, set::SetIdentity},
    mail::{
        changes::ChangesMail, get::GetMail, import::ImportMail, parse::ParseMail, query::QueryMail,
        set::SetMail,
    },
    mailbox::{changes::ChangesMailbox, get::GetMailbox, query::QueryMailbox, set::SetMailbox},
    thread::{changes::ChangesThread, get::GetThread},
};
use store::{tracing::error, Store};

use crate::{state::StateChange, JMAPServer};

pub struct InvocationResult {
    result: JSONValue,
    next_invocation: Option<Invocation>,
    change: Option<StateChange>,
}

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

    let total_method_calls = request.method_calls.len();
    for (call_num, (name, arguments, call_id)) in request.method_calls.into_iter().enumerate() {
        match Invocation::parse(&name, arguments, &response, &core.store.config) {
            Ok(mut invocation) => {
                let is_set = invocation.update_set_flags(core.is_in_cluster());
                let account_id = invocation.account_id;

                match handle_method_call(invocation, &core).await {
                    Ok(result) => {
                        // Add result
                        if let Some(state_change) = result.change {
                            // Commit change
                            if core.is_in_cluster() && !core.commit_index(state_change.id).await {
                                response.push_error(call_id, MethodError::ServerPartialFail);
                                continue;
                            }

                            // Broadcast change to subscribers
                            if let Err(err) =
                                core.publish_state_change(account_id, state_change).await
                            {
                                error!("Failed to publish state change: {}", err);
                                response.push_error(call_id, MethodError::ServerPartialFail);
                                continue;
                            }
                        }
                        response.push_response(
                            name,
                            call_id.clone(),
                            result.result,
                            is_set && (include_created_ids || call_num < total_method_calls - 1),
                        );

                        // Execute next invocation
                        if let Some(next_invocation) = result.next_invocation {
                            let name = next_invocation.to_string();
                            match handle_method_call(next_invocation, &core).await {
                                Ok(result) => {
                                    if let Some(state_change) = result.change {
                                        // Commit change
                                        if core.is_in_cluster()
                                            && !core.commit_index(state_change.id).await
                                        {
                                            response.push_error(
                                                call_id,
                                                MethodError::ServerPartialFail,
                                            );
                                            continue;
                                        }

                                        // Broadcast change to subscribers
                                        if let Err(err) = core
                                            .publish_state_change(account_id, state_change)
                                            .await
                                        {
                                            error!("Failed to publish state change: {}", err);
                                            response.push_error(
                                                call_id,
                                                MethodError::ServerPartialFail,
                                            );
                                            continue;
                                        }
                                    }
                                    response.push_response(name, call_id, result.result, false);
                                }
                                Err(err) => {
                                    response.push_error(call_id, err);
                                }
                            }
                        }
                    }
                    Err(err) => {
                        response.push_error(call_id, err);
                    }
                }
            }
            Err(err) => response.push_error(call_id, err),
        }
    }

    if !include_created_ids {
        response.created_ids.clear();
    }

    response
}

pub async fn handle_method_call<T>(
    invocation: Invocation,
    core: &web::Data<JMAPServer<T>>,
) -> jmap::Result<InvocationResult>
where
    T: for<'x> Store<'x> + 'static,
{
    let store = core.store.clone();
    core.spawn_jmap_request(move || {
        Ok(match (invocation.obj, invocation.call) {
            (Object::Email, Method::Get(request)) => store.get::<GetMail<T>>(request)?.into(),
            (Object::Email, Method::Set(request)) => store.set::<SetMail>(request)?.into(),
            (Object::Email, Method::Query(request)) => store.query::<QueryMail<T>>(request)?.into(),
            (Object::Email, Method::QueryChanges(request)) => store
                .query_changes::<ChangesMail, QueryMail<T>>(request)?
                .into(),
            (Object::Email, Method::Changes(request)) => {
                store.changes::<ChangesMail>(request)?.into()
            }
            (Object::Email, Method::Import(request)) => {
                store.import::<ImportMail<T>>(request)?.into()
            }
            (Object::Email, Method::Parse(request)) => store.parse::<ParseMail>(request)?.into(),
            (Object::Thread, Method::Get(request)) => store.get::<GetThread<T>>(request)?.into(),
            (Object::Thread, Method::Changes(request)) => {
                store.changes::<ChangesThread>(request)?.into()
            }
            (Object::Mailbox, Method::Get(request)) => store.get::<GetMailbox<T>>(request)?.into(),
            (Object::Mailbox, Method::Set(request)) => store.set::<SetMailbox>(request)?.into(),
            (Object::Mailbox, Method::Query(request)) => {
                store.query::<QueryMailbox<T>>(request)?.into()
            }
            (Object::Mailbox, Method::QueryChanges(request)) => store
                .query_changes::<ChangesMailbox, QueryMailbox<T>>(request)?
                .into(),
            (Object::Mailbox, Method::Changes(request)) => {
                store.changes::<ChangesMailbox>(request)?.into()
            }
            (Object::Identity, Method::Get(request)) => {
                store.get::<GetIdentity<T>>(request)?.into()
            }
            (Object::Identity, Method::Set(request)) => store.set::<SetIdentity>(request)?.into(),
            (Object::Identity, Method::Changes(request)) => {
                store.changes::<ChangesIdentity>(request)?.into()
            }
            (Object::PushSubscription, Method::Get(request)) => store
                .get::<GetPushSubscription<T>>(request)?
                .no_account_id()
                .into(),
            (Object::PushSubscription, Method::Set(request)) => store
                .set::<SetPushSubscription>(request)?
                .no_account_id()
                .into(),
            (Object::Core, Method::Echo(arguments)) => InvocationResult::new(arguments),
            _ => {
                return Err(MethodError::ServerUnavailable);
            }
        })
    })
    .await
}

impl InvocationResult {
    pub fn new(result: JSONValue) -> Self {
        Self {
            result,
            next_invocation: None,
            change: None,
        }
    }
}

impl From<SetResult> for InvocationResult {
    fn from(mut result: SetResult) -> Self {
        InvocationResult {
            next_invocation: Option::take(&mut result.next_invocation),
            change: if result.new_state != result.old_state {
                StateChange {
                    collection: result.collection,
                    account_id: result.account_id,
                    id: result.new_state.get_change_id(),
                }
                .into()
            } else {
                None
            },
            result: result.into(),
        }
    }
}

impl From<GetResult> for InvocationResult {
    fn from(result: GetResult) -> Self {
        InvocationResult::new(result.into())
    }
}

impl From<ChangesResult> for InvocationResult {
    fn from(result: ChangesResult) -> Self {
        InvocationResult::new(result.into())
    }
}

impl From<QueryResult> for InvocationResult {
    fn from(result: QueryResult) -> Self {
        InvocationResult::new(result.into())
    }
}

impl From<QueryChangesResult> for InvocationResult {
    fn from(result: QueryChangesResult) -> Self {
        InvocationResult::new(result.into())
    }
}

impl From<ImportResult> for InvocationResult {
    fn from(result: ImportResult) -> Self {
        InvocationResult {
            next_invocation: None,
            change: if result.new_state != result.old_state {
                StateChange {
                    collection: result.collection,
                    account_id: result.account_id,
                    id: result.new_state.get_change_id(),
                }
                .into()
            } else {
                None
            },
            result: result.into(),
        }
    }
}

impl From<ParseResult> for InvocationResult {
    fn from(result: ParseResult) -> Self {
        InvocationResult::new(result.into())
    }
}
