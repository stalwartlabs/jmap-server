use std::iter::FromIterator;

use crate::{api::response::serialize_hex, authorization};
use actix_web::{
    http::{header::ContentType, StatusCode},
    web, HttpResponse,
};
use jmap::{principal::schema::Type, request::ACLEnforce, types::jmap::JMAPId, URI};
use jmap_mail::mail::sharing::JMAPShareMail;
use jmap_sharing::principal::account::JMAPAccountStore;
use store::{
    config::{env_settings::EnvSettings, jmap::JMAPConfig},
    core::{acl::ACL, vec_map::VecMap},
    Store,
};

use crate::JMAPServer;

use super::RequestError;

#[derive(Debug, Clone, serde::Serialize)]
pub struct Session {
    #[serde(rename(serialize = "capabilities"))]
    capabilities: VecMap<URI, Capabilities>,
    #[serde(rename(serialize = "accounts"))]
    accounts: VecMap<JMAPId, Account>,
    #[serde(rename(serialize = "primaryAccounts"))]
    primary_accounts: VecMap<URI, JMAPId>,
    #[serde(rename(serialize = "username"))]
    username: String,
    #[serde(rename(serialize = "apiUrl"))]
    api_url: String,
    #[serde(rename(serialize = "downloadUrl"))]
    download_url: String,
    #[serde(rename(serialize = "uploadUrl"))]
    upload_url: String,
    #[serde(rename(serialize = "eventSourceUrl"))]
    event_source_url: String,
    #[serde(rename(serialize = "state"))]
    #[serde(serialize_with = "serialize_hex")]
    state: u32,
    #[serde(skip)]
    base_url: String,
}

#[derive(Debug, Clone, serde::Serialize)]
struct Account {
    #[serde(rename(serialize = "name"))]
    name: String,
    #[serde(rename(serialize = "isPersonal"))]
    is_personal: bool,
    #[serde(rename(serialize = "isReadOnly"))]
    is_read_only: bool,
    #[serde(rename(serialize = "accountCapabilities"))]
    account_capabilities: VecMap<URI, Capabilities>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(untagged)]
#[allow(dead_code)]
enum Capabilities {
    Core(CoreCapabilities),
    Mail(MailCapabilities),
    Submission(SubmissionCapabilities),
    VacationResponse(VacationResponseCapabilities),
    WebSocket(WebSocketCapabilities),
}

#[derive(Debug, Clone, serde::Serialize)]
struct CoreCapabilities {
    #[serde(rename(serialize = "maxSizeUpload"))]
    max_size_upload: usize,
    #[serde(rename(serialize = "maxConcurrentUpload"))]
    max_concurrent_upload: usize,
    #[serde(rename(serialize = "maxSizeRequest"))]
    max_size_request: usize,
    #[serde(rename(serialize = "maxConcurrentRequests"))]
    max_concurrent_requests: usize,
    #[serde(rename(serialize = "maxCallsInRequest"))]
    max_calls_in_request: usize,
    #[serde(rename(serialize = "maxObjectsInGet"))]
    max_objects_in_get: usize,
    #[serde(rename(serialize = "maxObjectsInSet"))]
    max_objects_in_set: usize,
    #[serde(rename(serialize = "collationAlgorithms"))]
    collation_algorithms: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct WebSocketCapabilities {
    #[serde(rename(serialize = "url"))]
    url: String,
    #[serde(rename(serialize = "supportsPush"))]
    supports_push: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
struct MailCapabilities {
    #[serde(rename(serialize = "maxMailboxesPerEmail"))]
    max_mailboxes_per_email: Option<usize>,
    #[serde(rename(serialize = "maxMailboxDepth"))]
    max_mailbox_depth: usize,
    #[serde(rename(serialize = "maxSizeMailboxName"))]
    max_size_mailbox_name: usize,
    #[serde(rename(serialize = "maxSizeAttachmentsPerEmail"))]
    max_size_attachments_per_email: usize,
    #[serde(rename(serialize = "emailQuerySortOptions"))]
    email_query_sort_options: Vec<String>,
    #[serde(rename(serialize = "mayCreateTopLevelMailbox"))]
    may_create_top_level_mailbox: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
struct SubmissionCapabilities {
    #[serde(rename(serialize = "maxDelayedSend"))]
    max_delayed_send: usize,
    #[serde(rename(serialize = "submissionExtensions"))]
    submission_extensions: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct VacationResponseCapabilities {}

impl Session {
    pub fn new(settings: &EnvSettings, config: &JMAPConfig) -> Session {
        let hostname = settings.get("hostname").unwrap();
        let base_url = format!(
            "{}://{}",
            if settings.contains_key("cert-path") {
                "https"
            } else {
                "http"
            },
            hostname
        );

        Session {
            capabilities: VecMap::from_iter([
                (URI::Core, Capabilities::Core(CoreCapabilities::new(config))),
                (URI::Mail, Capabilities::Mail(MailCapabilities::new(config))),
                (
                    URI::WebSocket,
                    Capabilities::WebSocket(WebSocketCapabilities::new(&base_url)),
                ),
            ]),
            accounts: VecMap::new(),
            primary_accounts: VecMap::new(),
            username: "".to_string(),
            api_url: format!("{}/jmap/", base_url),
            download_url: format!(
                "{}/jmap/download/{{accountId}}/{{blobId}}/{{name}}?accept={{type}}",
                base_url
            ),
            upload_url: format!("{}/jmap/upload/{{accountId}}/", base_url),
            event_source_url: format!(
                "{}/jmap/eventsource/?types={{types}}&closeafter={{closeafter}}&ping={{ping}}",
                base_url
            ),
            base_url,
            state: 0,
        }
    }

    pub fn set_primary_account(
        &mut self,
        account_id: JMAPId,
        username: String,
        name: String,
        capabilities: Option<&[URI]>,
    ) {
        self.username = username;

        if let Some(capabilities) = capabilities {
            for capability in capabilities {
                self.primary_accounts.append(capability.clone(), account_id);
            }
        } else {
            for capability in self.capabilities.keys() {
                self.primary_accounts.append(capability.clone(), account_id);
            }
        }

        self.accounts.set(
            account_id,
            Account::new(name, true, false).add_capabilities(capabilities, &self.capabilities),
        );
    }

    pub fn add_account(
        &mut self,
        account_id: JMAPId,
        name: String,
        is_personal: bool,
        is_read_only: bool,
        capabilities: Option<&[URI]>,
    ) {
        self.accounts.set(
            account_id,
            Account::new(name, is_personal, is_read_only)
                .add_capabilities(capabilities, &self.capabilities),
        );
    }

    pub fn set_state(&mut self, state: u32) {
        self.state = state;
    }

    pub fn api_url(&self) -> &str {
        &self.api_url
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}

impl Account {
    pub fn new(name: String, is_personal: bool, is_read_only: bool) -> Account {
        Account {
            name,
            is_personal,
            is_read_only,
            account_capabilities: VecMap::new(),
        }
    }

    pub fn add_capabilities(
        mut self,
        capabilities: Option<&[URI]>,
        core_capabilities: &VecMap<URI, Capabilities>,
    ) -> Account {
        if let Some(capabilities) = capabilities {
            for capability in capabilities {
                self.account_capabilities.append(
                    capability.clone(),
                    core_capabilities.get(capability).unwrap().clone(),
                );
            }
        } else {
            self.account_capabilities = core_capabilities.clone();
        }
        self
    }
}

impl CoreCapabilities {
    pub fn new(config: &JMAPConfig) -> Self {
        CoreCapabilities {
            max_size_upload: config.max_size_upload,
            max_concurrent_upload: config.max_concurrent_uploads,
            max_size_request: config.max_size_request,
            max_concurrent_requests: config.max_concurrent_requests,
            max_calls_in_request: config.max_calls_in_request,
            max_objects_in_get: config.max_objects_in_get,
            max_objects_in_set: config.max_objects_in_set,
            collation_algorithms: vec![
                "i;ascii-numeric".to_string(),
                "i;ascii-casemap".to_string(),
                "i;unicode-casemap".to_string(),
            ],
        }
    }
}

impl WebSocketCapabilities {
    pub fn new(base_url: &str) -> Self {
        WebSocketCapabilities {
            url: format!("ws{}/jmap/ws", base_url.strip_prefix("http").unwrap()),
            supports_push: true,
        }
    }
}

impl MailCapabilities {
    pub fn new(config: &JMAPConfig) -> Self {
        MailCapabilities {
            max_mailboxes_per_email: None,
            max_mailbox_depth: config.mailbox_max_depth,
            max_size_mailbox_name: config.mailbox_name_max_len,
            max_size_attachments_per_email: config.mail_attachments_max_size,
            email_query_sort_options: [
                "receivedAt",
                "size",
                "from",
                "to",
                "subject",
                "sentAt",
                "hasKeyword",
                "allInThreadHaveKeyword",
                "someInThreadHaveKeyword",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            may_create_top_level_mailbox: true,
        }
    }
}

pub async fn handle_jmap_session<T>(
    core: web::Data<JMAPServer<T>>,
    session: authorization::Session,
) -> Result<HttpResponse, RequestError>
where
    T: for<'x> Store<'x> + 'static,
{
    let store = core.store.clone();
    match core
        .clone()
        .spawn_worker(move || {
            let mut response = core.base_session.clone();

            response.set_state(session.state());

            // Obtain member and shared accounts
            let acl = store.get_acl_token(session.account_id())?;

            for (pos, id) in acl
                .member_of
                .iter()
                .chain(acl.access_to.iter().map(|(id, _)| id))
                .enumerate()
            {
                let (email, mut name, ptype) = store
                    .get_account_details(*id)?
                    .unwrap_or_else(|| ("".to_string(), "".to_string(), Type::Individual));
                if pos == 0 {
                    if name.is_empty() {
                        name = email.clone();
                    }
                    response.set_primary_account(session.account_id().into(), email, name, None);
                } else {
                    let is_readonly = if !acl.is_member(*id) {
                        store
                            .mail_shared_folders(*id, &acl.member_of, ACL::AddItems)
                            .ok()
                            .as_ref()
                            .and_then(|v| v.as_ref().as_ref().map(|v| v.is_empty()))
                            .unwrap_or(true)
                    } else {
                        false
                    };

                    response.add_account(
                        (*id).into(),
                        if !name.is_empty() { name } else { email },
                        matches!(ptype, Type::Individual),
                        is_readonly,
                        Some(&[URI::Core, URI::Mail, URI::WebSocket]),
                    );
                }
            }

            Ok(response)
        })
        .await
    {
        Ok(response) => Ok(HttpResponse::build(StatusCode::OK)
            .insert_header(ContentType::json())
            .body(serde_json::to_string(&response).unwrap_or_default())),
        Err(_) => Err(RequestError::internal_server_error()),
    }
}
