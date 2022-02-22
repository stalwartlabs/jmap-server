use std::fmt::Display;

use actix_web::{
    error,
    http::{header, StatusCode},
    HttpResponse, HttpResponseBuilder,
};

#[derive(Debug)]
pub struct JMAPServerError {
    pub error: String,
    pub code: StatusCode,
}

impl std::fmt::Display for JMAPServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl error::ResponseError for JMAPServerError {
    fn error_response(&self) -> HttpResponse {
        HttpResponseBuilder::new(self.code)
            .insert_header(header::ContentType::json())
            .body(self.error.clone())
    }

    fn status_code(&self) -> StatusCode {
        self.code
    }
}

impl From<&str> for JMAPServerError {
    fn from(error: &str) -> Self {
        JMAPServerError {
            error: format!("{{\"reason\":\"{}\"}}", error),
            code: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<String> for JMAPServerError {
    fn from(error: String) -> Self {
        JMAPServerError {
            error: format!("{{\"reason\":\"{}\"}}", error),
            code: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
