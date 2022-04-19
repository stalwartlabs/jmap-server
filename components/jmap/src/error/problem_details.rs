#[derive(Debug, serde::Serialize)]
pub struct ProblemDetails {
    #[serde(rename(serialize = "type"))]
    p_type: String,
    pub status: u16,
    title: String,
    detail: String,
}

impl ProblemDetails {
    pub fn new(status: u16, title: impl Into<String>, detail: impl Into<String>) -> Self {
        ProblemDetails {
            p_type: "about:blank".to_string(),
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

    pub fn not_found() -> Self {
        ProblemDetails::new(
            404,
            "Not Found",
            "The requested resource does not exist on this server.",
        )
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}
