use std::{collections::HashMap, fmt};

use jmap::{
    error::method::MethodError,
    push_subscription::schema::PushSubscription,
    request::{
        changes::{ChangesRequest, ChangesResponse},
        get::{GetRequest, GetResponse},
        query::{QueryRequest, QueryResponse},
        query_changes::{QueryChangesRequest, QueryChangesResponse},
        set::{SetRequest, SetResponse},
        Method, ResultReference,
    },
    types::jmap::JMAPId,
    types::{json_pointer::JSONPointerEval, type_state::TypeState},
};

use jmap_mail::{
    email_submission::schema::EmailSubmission,
    identity::schema::Identity,
    mail::{
        import::{EmailImportRequest, EmailImportResponse},
        parse::{EmailParseRequest, EmailParseResponse},
        schema::Email,
    },
    mailbox::schema::Mailbox,
    thread::schema::Thread,
    vacation_response::schema::VacationResponse,
};
use serde::{de::Visitor, ser::SerializeSeq, Deserialize, Serialize};
use store::{log::changes::ChangeId, AccountId};

use crate::services::state_change::StateChange;

use super::response;

#[derive(Debug)]
pub struct Call<T> {
    pub id: String,
    pub method: T,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Changes {
    Item {
        created_ids: Option<HashMap<String, JMAPId>>,
        change_id: ChangeId,
        state_change: Option<StateChange>,
        next_call: Option<Request>,
    },
    Subscription {
        account_id: AccountId,
        change_id: ChangeId,
    },
    None,
}

#[derive(Debug)]
pub enum Request {
    //CopyBlob(CopyBlobResponse),
    GetPushSubscription(GetRequest<PushSubscription>),
    SetPushSubscription(SetRequest<PushSubscription>),
    GetMailbox(GetRequest<Mailbox>),
    ChangesMailbox(ChangesRequest),
    QueryMailbox(QueryRequest<Mailbox>),
    QueryChangesMailbox(QueryChangesRequest<Mailbox>),
    SetMailbox(SetRequest<Mailbox>),
    GetThread(GetRequest<Thread>),
    ChangesThread(ChangesRequest),
    GetEmail(GetRequest<Email>),
    ChangesEmail(ChangesRequest),
    QueryEmail(QueryRequest<Email>),
    QueryChangesEmail(QueryChangesRequest<Email>),
    SetEmail(SetRequest<Email>),
    //CopyEmail(CopyRequest<Email>),
    ImportEmail(EmailImportRequest),
    ParseEmail(EmailParseRequest),
    //GetSearchSnippet(SearchSnippetGetResponse),
    GetIdentity(GetRequest<Identity>),
    ChangesIdentity(ChangesRequest),
    SetIdentity(SetRequest<Identity>),
    GetEmailSubmission(GetRequest<EmailSubmission>),
    ChangesEmailSubmission(ChangesRequest),
    QueryEmailSubmission(QueryRequest<EmailSubmission>),
    QueryChangesEmailSubmission(QueryChangesRequest<EmailSubmission>),
    SetEmailSubmission(SetRequest<EmailSubmission>),
    GetVacationResponse(GetRequest<VacationResponse>),
    SetVacationResponse(SetRequest<VacationResponse>),
    Echo(serde_json::Value),
    Error(MethodError),
}

#[derive(Debug)]
pub enum Response {
    //CopyBlob(CopyBlobResponse),
    GetPushSubscription(GetResponse<PushSubscription>),
    SetPushSubscription(SetResponse<PushSubscription>),
    GetMailbox(GetResponse<Mailbox>),
    ChangesMailbox(ChangesResponse<Mailbox>),
    QueryMailbox(QueryResponse),
    QueryChangesMailbox(QueryChangesResponse),
    SetMailbox(SetResponse<Mailbox>),
    GetThread(GetResponse<Thread>),
    ChangesThread(ChangesResponse<Thread>),
    GetEmail(GetResponse<Email>),
    ChangesEmail(ChangesResponse<Email>),
    QueryEmail(QueryResponse),
    QueryChangesEmail(QueryChangesResponse),
    SetEmail(SetResponse<Email>),
    //CopyEmail(CopyResponse<Email>),
    ImportEmail(EmailImportResponse),
    ParseEmail(EmailParseResponse),
    //GetSearchSnippet(SearchSnippetGetResponse),
    GetIdentity(GetResponse<Identity>),
    ChangesIdentity(ChangesResponse<Identity>),
    SetIdentity(SetResponse<Identity>),
    GetEmailSubmission(GetResponse<EmailSubmission>),
    ChangesEmailSubmission(ChangesResponse<EmailSubmission>),
    QueryEmailSubmission(QueryResponse),
    QueryChangesEmailSubmission(QueryChangesResponse),
    SetEmailSubmission(SetResponse<EmailSubmission>),
    GetVacationResponse(GetResponse<VacationResponse>),
    SetVacationResponse(SetResponse<VacationResponse>),
    Echo(serde_json::Value),
    Error(MethodError),
}

impl Request {
    pub fn prepare_request(&mut self, response: &response::Response) -> jmap::Result<()> {
        let mut eval_result_ref = |rr: &ResultReference| -> Option<Vec<u64>> {
            for r in &response.method_responses {
                if r.id == rr.result_of {
                    match (&rr.name, &r.method) {
                        (Method::GetMailbox, Response::GetMailbox(response)) => {
                            return response.eval_json_pointer(&rr.path);
                        }
                        (Method::ChangesMailbox, Response::ChangesMailbox(response)) => {
                            return response.eval_json_pointer(&rr.path);
                        }
                        (Method::QueryMailbox, Response::QueryMailbox(response)) => {
                            return response.eval_json_pointer(&rr.path);
                        }
                        (Method::QueryChangesMailbox, Response::QueryChangesMailbox(response)) => {
                            return response.eval_json_pointer(&rr.path);
                        }
                        (Method::GetThread, Response::GetThread(response)) => {
                            return response.eval_json_pointer(&rr.path);
                        }
                        (Method::ChangesThread, Response::ChangesThread(response)) => {
                            return response.eval_json_pointer(&rr.path);
                        }
                        (Method::GetEmail, Response::GetEmail(response)) => {
                            return response.eval_json_pointer(&rr.path);
                        }
                        (Method::ChangesEmail, Response::ChangesEmail(response)) => {
                            return response.eval_json_pointer(&rr.path);
                        }
                        (Method::QueryEmail, Response::QueryEmail(response)) => {
                            return response.eval_json_pointer(&rr.path);
                        }
                        (Method::QueryChangesEmail, Response::QueryChangesEmail(response)) => {
                            return response.eval_json_pointer(&rr.path);
                        }
                        (Method::GetIdentity, Response::GetIdentity(response)) => {
                            return response.eval_json_pointer(&rr.path);
                        }
                        (Method::ChangesIdentity, Response::ChangesIdentity(response)) => {
                            return response.eval_json_pointer(&rr.path);
                        }
                        (Method::GetEmailSubmission, Response::GetEmailSubmission(response)) => {
                            return response.eval_json_pointer(&rr.path);
                        }
                        (
                            Method::ChangesEmailSubmission,
                            Response::ChangesEmailSubmission(response),
                        ) => {
                            return response.eval_json_pointer(&rr.path);
                        }
                        (
                            Method::QueryEmailSubmission,
                            Response::QueryEmailSubmission(response),
                        ) => {
                            return response.eval_json_pointer(&rr.path);
                        }
                        (
                            Method::QueryChangesEmailSubmission,
                            Response::QueryChangesEmailSubmission(response),
                        ) => {
                            return response.eval_json_pointer(&rr.path);
                        }
                        _ => {
                            break;
                        }
                    }
                }
            }

            None
        };

        match self {
            Request::GetMailbox(request) => {
                request.eval_result_references(&mut eval_result_ref)?;
            }
            Request::GetThread(request) => {
                request.eval_result_references(&mut eval_result_ref)?;
            }
            Request::GetEmail(request) => {
                request.eval_result_references(&mut eval_result_ref)?;
            }
            Request::GetIdentity(request) => {
                request.eval_result_references(&mut eval_result_ref)?;
            }
            Request::GetEmailSubmission(request) => {
                request.eval_result_references(&mut eval_result_ref)?;
            }
            Request::SetMailbox(request) => {
                request.eval_references(&mut eval_result_ref, &response.created_ids)?;
            }
            Request::SetEmail(request) => {
                request.eval_references(&mut eval_result_ref, &response.created_ids)?;
            }
            Request::ImportEmail(request) => {
                request.eval_references(&mut eval_result_ref, &response.created_ids)?;
            }
            Request::SetIdentity(request) => {
                request.eval_references(&mut eval_result_ref, &response.created_ids)?;
            }
            Request::SetEmailSubmission(request) => {
                request.eval_references(&mut eval_result_ref, &response.created_ids)?;
            }
            Request::GetPushSubscription(request) => {
                request.account_id = JMAPId::new(1).into(); //TODO remove
            }
            Request::SetPushSubscription(request) => {
                request.account_id = JMAPId::new(1).into(); //TODO remove
            }
            _ => (),
        }
        Ok(())
    }
}

impl Response {
    pub fn changes(&mut self) -> Changes {
        match self {
            Response::SetMailbox(response) => {
                if let Some(change_id) = response.has_changes() {
                    Changes::Item {
                        created_ids: response.created_ids(),
                        change_id,
                        state_change: response
                            .state_changes()
                            .map(|s| StateChange::new(response.account_id(), s)),
                        next_call: None,
                    }
                } else {
                    Changes::None
                }
            }
            Response::SetEmail(response) => {
                if let Some(change_id) = response.has_changes() {
                    Changes::Item {
                        created_ids: response.created_ids(),
                        change_id,
                        state_change: response
                            .state_changes()
                            .map(|s| StateChange::new(response.account_id(), s)),
                        next_call: None,
                    }
                } else {
                    Changes::None
                }
            }
            /*Response::CopyEmail(response) => {
                response.account_id;
                Changes::None
            }*/
            Response::ImportEmail(response) => {
                if let Some(change_id) = response.has_changes() {
                    Changes::Item {
                        created_ids: response.created_ids(),
                        change_id,
                        state_change: StateChange::new(
                            response.account_id(),
                            vec![
                                (TypeState::Email, change_id),
                                (TypeState::Mailbox, change_id),
                                (TypeState::Thread, change_id),
                            ],
                        )
                        .into(),
                        next_call: None,
                    }
                } else {
                    Changes::None
                }
            }
            Response::SetIdentity(response) => {
                if let Some(change_id) = response.has_changes() {
                    Changes::Item {
                        created_ids: response.created_ids(),
                        change_id,
                        state_change: response
                            .state_changes()
                            .map(|s| StateChange::new(response.account_id(), s)),
                        next_call: None,
                    }
                } else {
                    Changes::None
                }
            }
            Response::SetEmailSubmission(response) => {
                if let Some(change_id) = response.has_changes() {
                    Changes::Item {
                        created_ids: response.created_ids(),
                        change_id,
                        state_change: response
                            .state_changes()
                            .map(|s| StateChange::new(response.account_id(), s)),
                        next_call: response.next_call.take().map(Request::SetEmail),
                    }
                } else {
                    Changes::None
                }
            }
            Response::SetVacationResponse(response) => {
                if let Some(change_id) = response.has_changes() {
                    Changes::Item {
                        created_ids: None,
                        change_id,
                        state_change: None,
                        next_call: None,
                    }
                } else {
                    Changes::None
                }
            }
            Response::SetPushSubscription(response) => {
                let changes = if let Some(change_id) = response.has_changes() {
                    Changes::Subscription {
                        account_id: response.account_id(),
                        change_id,
                    }
                } else {
                    Changes::None
                };
                response.account_id = None;
                response.new_state = None;
                response.old_state = None;
                changes
            }
            Response::GetPushSubscription(response) => {
                response.account_id = None;
                Changes::None
            }
            _ => Changes::None,
        }
    }
}

impl<'de> Deserialize<'de> for Call<Request> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(CallVisitor)
    }
}

struct CallVisitor;

impl<'de> Visitor<'de> for CallVisitor {
    type Value = Call<Request>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP method request")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let method_name = seq
            .next_element::<&str>()?
            .ok_or_else(|| serde::de::Error::custom("Expected a method name."))?;

        let method = match match_method(&mut seq, method_name) {
            Ok(request) => request,
            Err(err) => match err {
                MatchError::Parse(err) => Request::Error(MethodError::InvalidArguments(format!(
                    "Failed to parse method: {}",
                    err
                ))),
                MatchError::Eof => {
                    return Err(serde::de::Error::custom("Expected a method request."))
                }
            },
        };

        let id = seq
            .next_element::<String>()?
            .ok_or_else(|| serde::de::Error::custom("Expected method call id."))?;

        Ok(Call { method, id })
    }
}

enum MatchError {
    Parse(String),
    Eof,
}

fn match_method<'de, A>(seq: &mut A, name: &str) -> Result<Request, MatchError>
where
    A: serde::de::SeqAccess<'de>,
{
    Ok(match name {
        "Email/get" => Request::GetEmail(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "Email/changes" => Request::ChangesEmail(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "Email/query" => Request::QueryEmail(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "Email/queryChanges" => Request::QueryChangesEmail(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "Email/set" => Request::SetEmail(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        /*"Email/copy" => Request::CopyEmail(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or( MatchError::Eof)?,
        ),*/
        "Email/import" => Request::ImportEmail(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "Email/parse" => Request::ParseEmail(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "Mailbox/get" => Request::GetMailbox(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "Mailbox/changes" => Request::ChangesMailbox(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "Mailbox/query" => Request::QueryMailbox(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "Mailbox/queryChanges" => Request::QueryChangesMailbox(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "Mailbox/set" => Request::SetMailbox(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "Thread/get" => Request::GetThread(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "Thread/changes" => Request::ChangesThread(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        /*"SearchSnippet/get" => Request::GetSearchSnippet(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or( MatchError::Eof)?,
        ),*/
        "Identity/get" => Request::GetIdentity(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "Identity/changes" => Request::ChangesIdentity(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "Identity/set" => Request::SetIdentity(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "EmailSubmission/get" => Request::GetEmailSubmission(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "EmailSubmission/changes" => Request::ChangesEmailSubmission(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "EmailSubmission/query" => Request::QueryEmailSubmission(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "EmailSubmission/queryChanges" => Request::QueryChangesEmailSubmission(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "EmailSubmission/set" => Request::SetEmailSubmission(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "VacationResponse/get" => Request::GetVacationResponse(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "VacationResponse/set" => Request::SetVacationResponse(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "PushSubscription/get" => Request::GetPushSubscription(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        "PushSubscription/set" => Request::SetPushSubscription(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        /*"Blob/copy" => Request::CopyBlob(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or( MatchError::Eof)?,
        ),*/
        "Core/echo" => Request::Echo(
            seq.next_element()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?,
        ),
        _ => {
            seq.next_element::<serde_json::Value>()
                .map_err(|err| MatchError::Parse(err.to_string()))?
                .ok_or(MatchError::Eof)?;
            Request::Error(MethodError::UnknownMethod(name.to_string()))
        }
    })
}

impl Serialize for Call<Response> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(3.into())?;

        match &self.method {
            Response::GetPushSubscription(response) => {
                seq.serialize_element("PushSubscription/get")?;
                seq.serialize_element(response)?;
            }
            Response::SetPushSubscription(response) => {
                seq.serialize_element("PushSubscription/set")?;
                seq.serialize_element(response)?;
            }
            Response::GetMailbox(response) => {
                seq.serialize_element("Mailbox/get")?;
                seq.serialize_element(response)?;
            }
            Response::ChangesMailbox(response) => {
                seq.serialize_element("Mailbox/changes")?;
                seq.serialize_element(response)?;
            }
            Response::QueryMailbox(response) => {
                seq.serialize_element("Mailbox/query")?;
                seq.serialize_element(response)?;
            }
            Response::QueryChangesMailbox(response) => {
                seq.serialize_element("Mailbox/queryChanges")?;
                seq.serialize_element(response)?;
            }
            Response::SetMailbox(response) => {
                seq.serialize_element("Mailbox/set")?;
                seq.serialize_element(response)?;
            }
            Response::GetThread(response) => {
                seq.serialize_element("Thread/get")?;
                seq.serialize_element(response)?;
            }
            Response::ChangesThread(response) => {
                seq.serialize_element("Thread/changes")?;
                seq.serialize_element(response)?;
            }
            Response::GetEmail(response) => {
                seq.serialize_element("Email/get")?;
                seq.serialize_element(response)?;
            }
            Response::ChangesEmail(response) => {
                seq.serialize_element("Email/changes")?;
                seq.serialize_element(response)?;
            }
            Response::QueryEmail(response) => {
                seq.serialize_element("Email/query")?;
                seq.serialize_element(response)?;
            }
            Response::QueryChangesEmail(response) => {
                seq.serialize_element("Email/queryChanges")?;
                seq.serialize_element(response)?;
            }
            Response::SetEmail(response) => {
                seq.serialize_element("Email/set")?;
                seq.serialize_element(response)?;
            }
            /*Response::CopyEmail(response) => {
                seq.serialize_element("Email/copy")?;
                seq.serialize_element(response)?;
            }*/
            Response::ImportEmail(response) => {
                seq.serialize_element("Email/import")?;
                seq.serialize_element(response)?;
            }
            Response::ParseEmail(response) => {
                seq.serialize_element("Email/parse")?;
                seq.serialize_element(response)?;
            }
            Response::GetIdentity(response) => {
                seq.serialize_element("Identity/get")?;
                seq.serialize_element(response)?;
            }
            Response::ChangesIdentity(response) => {
                seq.serialize_element("Identity/changes")?;
                seq.serialize_element(response)?;
            }
            Response::SetIdentity(response) => {
                seq.serialize_element("Identity/set")?;
                seq.serialize_element(response)?;
            }
            Response::GetEmailSubmission(response) => {
                seq.serialize_element("EmailSubmission/get")?;
                seq.serialize_element(response)?;
            }
            Response::ChangesEmailSubmission(response) => {
                seq.serialize_element("EmailSubmission/changes")?;
                seq.serialize_element(response)?;
            }
            Response::QueryEmailSubmission(response) => {
                seq.serialize_element("EmailSubmission/query")?;
                seq.serialize_element(response)?;
            }
            Response::QueryChangesEmailSubmission(response) => {
                seq.serialize_element("EmailSubmission/queryChanges")?;
                seq.serialize_element(response)?;
            }
            Response::SetEmailSubmission(response) => {
                seq.serialize_element("EmailSubmission/set")?;
                seq.serialize_element(response)?;
            }
            Response::GetVacationResponse(response) => {
                seq.serialize_element("VacationResponse/get")?;
                seq.serialize_element(response)?;
            }
            Response::SetVacationResponse(response) => {
                seq.serialize_element("VacationResponse/set")?;
                seq.serialize_element(response)?;
            }
            Response::Echo(response) => {
                seq.serialize_element("Core/echo")?;
                seq.serialize_element(response)?;
            }
            Response::Error(response) => {
                seq.serialize_element("error")?;
                seq.serialize_element(response)?;
            }
        }
        seq.serialize_element(&self.id)?;
        seq.end()
    }
}
