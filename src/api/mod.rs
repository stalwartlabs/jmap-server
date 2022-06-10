use actix_web::error;
use actix_web::http::header;
use actix_web::{http::StatusCode, HttpResponse};
use jmap::types::{jmap::JMAPId, state::JMAPState, type_state::TypeState};
use std::borrow::Cow;
use std::{collections::HashMap, fmt::Display};

pub mod blob;
pub mod ingest;
pub mod invocation;
pub mod method;
pub mod request;
pub mod response;
pub mod session;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum StateChangeType {
    StateChange,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct StateChangeResponse {
    #[serde(rename = "@type")]
    pub type_: StateChangeType,
    pub changed: HashMap<JMAPId, HashMap<TypeState, JMAPState>>,
}

impl StateChangeResponse {
    pub fn new() -> Self {
        Self {
            type_: StateChangeType::StateChange,
            changed: HashMap::new(),
        }
    }
}

impl Default for StateChangeResponse {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, serde::Serialize)]
pub struct ProblemDetails {
    #[serde(rename(serialize = "type"))]
    p_type: Cow<'static, str>,
    pub status: u16,
    title: Cow<'static, str>,
    detail: Cow<'static, str>,
}

impl ProblemDetails {
    pub fn new(
        status: u16,
        title: impl Into<Cow<'static, str>>,
        detail: impl Into<Cow<'static, str>>,
    ) -> Self {
        ProblemDetails {
            p_type: "about:blank".into(),
            status,
            title: title.into(),
            detail: detail.into(),
        }
    }

    pub fn internal_server_error() -> Self {
        ProblemDetails::new(
            500,
            "Internal Server Error",
            concat!(
                "There was a problem while processing your request. ",
                "Please contact the system administrator."
            ),
        )
    }

    pub fn invalid_parameters() -> Self {
        ProblemDetails::new(
            400,
            "Invalid Parameters",
            "One or multiple parameters could not be parsed.",
        )
    }

    pub fn forbidden() -> Self {
        ProblemDetails::new(
            403,
            "Forbidden",
            "You do not have enough permissions to access this resource.",
        )
    }

    pub fn too_many_requests() -> Self {
        ProblemDetails::new(
            429,
            "Too Many Requests",
            "Your request has been rate limited. Please try again in a few seconds.",
        )
    }

    pub fn not_found() -> Self {
        ProblemDetails::new(
            404,
            "Not Found",
            "The requested resource does not exist on this server.",
        )
    }

    pub fn unauthorized() -> Self {
        ProblemDetails::new(401, "Unauthorized", "You have to authenticate first.")
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(&self).unwrap_or_default()
    }
}

impl Display for ProblemDetails {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.detail)
    }
}

impl error::ResponseError for ProblemDetails {
    fn error_response(&self) -> HttpResponse {
        let mut response = HttpResponse::build(self.status_code());
        response.insert_header(("Content-Type", "application/problem+json"));
        if self.status == 401 {
            response.insert_header((header::WWW_AUTHENTICATE, "Basic realm=\"Stalwart JMAP\""));
        }
        response.body(serde_json::to_string(&self).unwrap_or_default())
    }

    fn status_code(&self) -> StatusCode {
        StatusCode::from_u16(self.status).unwrap()
    }
}
