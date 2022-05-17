#[derive(Debug, Clone, Copy, serde::Serialize)]
pub enum RequestLimitError {
    #[serde(rename(serialize = "maxSizeRequest"))]
    Size,
    #[serde(rename(serialize = "maxCallsInRequest"))]
    CallsIn,
    #[serde(rename(serialize = "maxConcurrentRequests"))]
    Concurrent,
}

#[derive(Debug, serde::Serialize)]
pub enum RequestErrorType {
    #[serde(rename(serialize = "urn:ietf:params:jmap:error:unknownCapability"))]
    UnknownCapability,
    #[serde(rename(serialize = "urn:ietf:params:jmap:error:notJSON"))]
    NotJSON,
    #[serde(rename(serialize = "urn:ietf:params:jmap:error:notRequest"))]
    NotRequest,
    #[serde(rename(serialize = "urn:ietf:params:jmap:error:limit"))]
    Limit,
}

#[derive(Debug, serde::Serialize)]
pub struct RequestError {
    #[serde(rename(serialize = "type"))]
    pub error: RequestErrorType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<RequestLimitError>,
    pub status: u32,
    pub detail: String,
}

impl RequestError {
    pub fn unknown_capability(capability: &str) -> RequestError {
        RequestError {
            error: RequestErrorType::UnknownCapability,
            limit: None,
            status: 400,
            detail: format!(
                concat!(
                    "The Request object used capability ",
                    "'{}', which is not supported",
                    "by this server."
                ),
                capability
            ),
        }
    }

    pub fn not_json() -> RequestError {
        RequestError {
            error: RequestErrorType::NotJSON,
            limit: None,
            status: 400,
            detail: "The Request object is not a valid JSON object.".to_string(),
        }
    }

    pub fn not_request() -> RequestError {
        RequestError {
            error: RequestErrorType::NotRequest,
            limit: None,
            status: 400,
            detail: "The Request object is not a valid JMAP request.".to_string(),
        }
    }

    pub fn limit(limit: RequestLimitError) -> RequestError {
        RequestError {
            error: RequestErrorType::Limit,
            limit: Some(limit),
            status: 400,
            detail: match limit {
                RequestLimitError::Size => concat!(
                    "The request is larger than the server ",
                    "is willing to process."
                )
                .to_string(),
                RequestLimitError::CallsIn => concat!(
                    "The request exceeds the maximum number ",
                    "of calls in a single request."
                )
                .to_string(),
                RequestLimitError::Concurrent => concat!(
                    "The request exceeds the maximum number ",
                    "of concurrent requests."
                )
                .to_string(),
            },
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}
