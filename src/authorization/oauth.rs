use std::{
    hash::{Hash, Hasher},
    sync::{
        atomic::{self, AtomicU32},
        Arc,
    },
    time::{Instant, SystemTime},
};

use actix_web::{web, HttpResponse};
use jmap::base64;
use jmap_sharing::principal::account::JMAPAccountStore;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use store::{
    ahash::AHasher,
    core::error::StoreError,
    rand::{
        distributions::{Alphanumeric, Standard},
        thread_rng, Rng,
    },
    serialize::leb128::Leb128,
    tracing::{debug, error},
    AccountId, Store,
};

use crate::JMAPServer;

use super::SymmetricEncrypt;

const OAUTH_HTML_HEADER: &str = include_str!("../../resources/oauth_header.html");
const OAUTH_HTML_FOOTER: &str = include_str!("../../resources/oauth_footer.html");
const OAUTH_HTML_LOGIN: &str = include_str!("../../resources/oauth_login.html");
const OAUTH_HTML_LOGIN_SUCCESS: &str = include_str!("../../resources/oauth_login_success.html");
const OAUTH_HTML_LOGIN_FAILED: &str = include_str!("../../resources/oauth_login_failed.html");
const OAUTH_HTML_ERROR: &str = include_str!("../../resources/oauth_error.html");

const STATUS_AUTHORIZED: u32 = 0;
const STATUS_TOKEN_ISSUED: u32 = 1;
const STATUS_PENDING: u32 = 2;

const DEVICE_CODE_LEN: usize = 40;
const USER_CODE_LEN: usize = 8;
const RANDOM_CODE_LEN: usize = 32;

const USER_CODE_ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // No 0, O, I, 1

pub struct OAuth {
    pub key: String,
    pub expiry_user_code: u64,
    pub expiry_token: u64,
    pub expiry_refresh_token: u64,
    pub expiry_refresh_token_renew: u64,
    pub max_auth_attempts: u32,
    pub metadata: String,
}

pub struct OAuthStatus {
    pub status: AtomicU32,
    pub account_id: AtomicU32,
    pub expiry: Instant,
    pub client_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserAuthRequest {
    code: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserAuthForm {
    code: Option<String>,
    email: Option<String>,
    password: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeviceAuthRequest {
    client_id: u64,
    //scope: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeviceAuthResponse {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    pub verification_uri_complete: String,
    pub expires_in: u64,
    pub interval: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenRequest {
    pub grant_type: String,
    pub device_code: Option<String>,
    pub client_id: Option<u64>,
    pub refresh_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum TokenResponse {
    Granted {
        access_token: String,
        token_type: String,
        expires_in: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        refresh_token: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        scope: Option<String>,
    },
    Error {
        error: ErrorType,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ErrorType {
    #[serde(rename = "invalid_grant")]
    InvalidGrant,
    #[serde(rename = "invalid_client")]
    InvalidClient,
    #[serde(rename = "invalid_scope")]
    InvalidScope,
    #[serde(rename = "invalid_request")]
    InvalidRequest,
    #[serde(rename = "unauthorized_client")]
    UnauthorizedClient,
    #[serde(rename = "unsupported_grant_type")]
    UnsupportedGrantType,
    #[serde(rename = "authorization_pending")]
    AuthorizationPending,
    #[serde(rename = "slow_down")]
    SlowDown,
    #[serde(rename = "access_denied")]
    AccessDenied,
    #[serde(rename = "expired_token")]
    ExpiredToken,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OAuthMetadata {
    pub issuer: String,
    pub token_endpoint: String,
    pub grant_types_supported: Vec<String>,
    pub device_authorization_endpoint: String,
    pub response_types_supported: Vec<String>,
    pub scopes_supported: Vec<String>,
    //pub authorization_endpoint: String,
}

pub async fn handle_device_auth<T>(
    core: web::Data<JMAPServer<T>>,
    params: web::Form<DeviceAuthRequest>,
) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    // Generate device code
    let device_code = thread_rng()
        .sample_iter(Alphanumeric)
        .take(DEVICE_CODE_LEN)
        .map(char::from)
        .collect::<String>();

    // Generate user code
    let mut user_code = String::with_capacity(USER_CODE_LEN + 1);
    for (pos, ch) in thread_rng()
        .sample_iter::<usize, _>(Standard)
        .take(USER_CODE_LEN)
        .map(|v| char::from(USER_CODE_ALPHABET[v % USER_CODE_ALPHABET.len()]))
        .enumerate()
    {
        if pos == USER_CODE_LEN / 2 {
            user_code.push('-');
        }
        user_code.push(ch);
    }

    // Add OAuth status
    let oauth_status = Arc::new(OAuthStatus {
        status: STATUS_PENDING.into(),
        account_id: u32::MAX.into(),
        expiry: Instant::now(),
        client_id: params.into_inner().client_id,
    });
    core.oauth_status
        .insert(device_code.clone(), oauth_status.clone())
        .await;
    core.oauth_status
        .insert(user_code.clone(), oauth_status)
        .await;

    // Build response
    let response = DeviceAuthResponse {
        verification_uri: format!("{}/auth", core.base_session.base_url()),
        verification_uri_complete: format!(
            "{}/auth/code?={}",
            core.base_session.base_url(),
            user_code
        ),
        device_code,
        user_code,
        expires_in: core.oauth.expiry_user_code,
        interval: 5,
    };

    HttpResponse::build(StatusCode::OK)
        .content_type("application/json")
        .body(serde_json::to_string(&response).unwrap_or_default())
}

pub async fn handle_token_request<T>(
    core: web::Data<JMAPServer<T>>,
    params: web::Form<TokenRequest>,
) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    let mut response = TokenResponse::error(ErrorType::InvalidGrant);

    if params
        .grant_type
        .eq_ignore_ascii_case("urn:ietf:params:oauth:grant-type:device_code")
    {
        response = TokenResponse::error(ErrorType::ExpiredToken);

        if let (Some(oauth), Some(client_id)) = (
            params
                .device_code
                .as_ref()
                .and_then(|dc| core.oauth_status.get(dc)),
            params.client_id,
        ) {
            if oauth.client_id == client_id {
                if oauth.expiry.elapsed().as_secs() < core.oauth.expiry_user_code {
                    response = match oauth.status.load(atomic::Ordering::Relaxed) {
                        STATUS_AUTHORIZED => {
                            // Mark this token as issued
                            oauth
                                .status
                                .store(STATUS_TOKEN_ISSUED, atomic::Ordering::Relaxed);

                            // Issue token
                            core.issue_token(
                                oauth.account_id.load(atomic::Ordering::Relaxed),
                                oauth.client_id,
                                true,
                            )
                            .await
                            .unwrap_or_else(|err| {
                                error!("Failed to generate OAuth token: {}", err);
                                TokenResponse::error(ErrorType::InvalidRequest)
                            })
                        }
                        status
                            if (STATUS_PENDING..STATUS_PENDING + core.oauth.max_auth_attempts)
                                .contains(&status) =>
                        {
                            TokenResponse::error(ErrorType::AuthorizationPending)
                        }
                        STATUS_TOKEN_ISSUED => TokenResponse::error(ErrorType::ExpiredToken),
                        _ => TokenResponse::error(ErrorType::AccessDenied),
                    };
                }
            } else {
                response = TokenResponse::error(ErrorType::InvalidClient);
            }
        }
    } else if params.grant_type.eq_ignore_ascii_case("refresh_token") {
        if let Some(refresh_token) = &params.refresh_token {
            match core
                .validate_access_token("refresh_token", refresh_token)
                .await
            {
                Ok((account_id, client_id, time_left)) => {
                    // TODO implement revoking client ids
                    response = core
                        .issue_token(
                            account_id,
                            client_id,
                            time_left <= core.oauth.expiry_refresh_token_renew,
                        )
                        .await
                        .unwrap_or_else(|err| {
                            debug!("Faild to refresh OAuth token: {}", err);
                            TokenResponse::error(ErrorType::InvalidGrant)
                        });
                }
                Err(err) => {
                    debug!("Refresh token failed validation: {}", err);
                }
            }
        } else {
            response = TokenResponse::error(ErrorType::InvalidRequest);
        }
    }

    HttpResponse::build(if response.is_error() {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::OK
    })
    .content_type("application/json")
    .body(serde_json::to_string(&response).unwrap_or_default())
}

pub async fn handle_client_auth<T>(params: web::Query<UserAuthRequest>) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    let mut response = String::with_capacity(
        OAUTH_HTML_HEADER.len() + OAUTH_HTML_FOOTER.len() + OAUTH_HTML_LOGIN.len(),
    );

    response.push_str(OAUTH_HTML_HEADER);
    response.push_str(&OAUTH_HTML_LOGIN.replace("@@@", params.code.as_deref().unwrap_or("")));
    response.push_str(OAUTH_HTML_FOOTER);

    HttpResponse::build(StatusCode::OK)
        .content_type("text/html; charset=utf-8")
        .body(response)
}

pub async fn handle_client_auth_post<T>(
    core: web::Data<JMAPServer<T>>,
    params: web::Form<UserAuthForm>,
) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    enum Response {
        Success,
        Failed,
        InvalidCode,
        Error,
    }

    let params = params.into_inner();
    let code = if let Some(oauth) = params
        .code
        .as_ref()
        .and_then(|code| core.oauth_status.get(code))
    {
        if (STATUS_PENDING..STATUS_PENDING + core.oauth.max_auth_attempts)
            .contains(&oauth.status.load(atomic::Ordering::Relaxed))
            && oauth.expiry.elapsed().as_secs() < core.oauth.expiry_user_code
        {
            if let (Some(email), Some(password)) = (params.email, params.password) {
                let store = core.store.clone();
                match core
                    .spawn_worker(move || store.authenticate(&email, &password))
                    .await
                {
                    Ok(Some(account_id)) => {
                        oauth
                            .account_id
                            .store(account_id, atomic::Ordering::Relaxed);
                        oauth
                            .status
                            .store(STATUS_AUTHORIZED, atomic::Ordering::Relaxed);
                        Response::Success
                    }
                    Ok(None) => {
                        oauth.status.fetch_add(1, atomic::Ordering::Relaxed);
                        Response::Failed
                    }
                    Err(_) => Response::Error,
                }
            } else {
                Response::Failed
            }
        } else {
            Response::InvalidCode
        }
    } else {
        Response::InvalidCode
    };

    let mut response = String::with_capacity(
        OAUTH_HTML_HEADER.len() + OAUTH_HTML_FOOTER.len() + OAUTH_HTML_LOGIN.len(),
    );
    response.push_str(OAUTH_HTML_HEADER);

    match code {
        Response::Success => {
            response.push_str(OAUTH_HTML_LOGIN_SUCCESS);
        }
        Response::Failed => {
            response.push_str(
                &OAUTH_HTML_LOGIN_FAILED.replace("@@@", params.code.as_deref().unwrap_or("")),
            );
        }
        Response::InvalidCode => {
            response.push_str(
                &OAUTH_HTML_ERROR.replace("@@@", "Invalid or expired authentication code."),
            );
        }
        Response::Error => {
            response.push_str(&OAUTH_HTML_ERROR.replace(
                "@@@",
                "There was a problem processing your request, please try again later.",
            ));
        }
    }

    response.push_str(OAUTH_HTML_FOOTER);

    HttpResponse::build(StatusCode::OK)
        .content_type("text/html; charset=utf-8")
        .body(response)
}

pub async fn handle_oauth_metadata<T>(core: web::Data<JMAPServer<T>>) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    HttpResponse::build(StatusCode::OK)
        .content_type("application/json")
        .body(core.oauth.metadata.clone())
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    async fn issue_token(
        &self,
        account_id: AccountId,
        client_id: u64,
        with_refresh_token: bool,
    ) -> store::Result<TokenResponse>
    where
        T: for<'x> Store<'x> + 'static,
    {
        let store = self.store.clone();
        let password_hash = self
            .spawn_worker(move || {
                // Make sure account still exits
                if let Some(secret_hash) = store.get_account_secret_hash(account_id)? {
                    Ok(secret_hash)
                } else {
                    Err(StoreError::DeserializeError(
                        "Account no longer exists".into(),
                    ))
                }
            })
            .await?;

        Ok(TokenResponse::Granted {
            access_token: self
                .encode_access_token(
                    "access_token",
                    account_id,
                    &password_hash,
                    client_id,
                    self.oauth.expiry_token,
                )
                .await?,
            token_type: "bearer".to_string(),
            expires_in: self.oauth.expiry_token,
            refresh_token: if with_refresh_token {
                self.encode_access_token(
                    "refresh_token",
                    account_id,
                    &password_hash,
                    client_id,
                    self.oauth.expiry_refresh_token,
                )
                .await?
                .into()
            } else {
                None
            },
            scope: None,
        })
    }

    async fn encode_access_token(
        &self,
        grant_type: &str,
        account_id: u32,
        password_hash: &str,
        client_id: u64,
        expiry_in: u64,
    ) -> store::Result<String> {
        let key = self.oauth.key.clone();
        let context = format!("{} {} {}", grant_type, client_id, password_hash);

        self.spawn_worker(move || {
            // Set expiration time
            let expiry = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
                .saturating_sub(946684800) // Jan 1, 2000
                + expiry_in;

            // Create nonce
            let mut nonce = Vec::with_capacity(SymmetricEncrypt::NONCE_LEN);
            account_id.to_leb128_writer(&mut nonce).unwrap();
            expiry.to_leb128_writer(&mut nonce).unwrap();
            match nonce.len().cmp(&SymmetricEncrypt::NONCE_LEN) {
                std::cmp::Ordering::Less => nonce.resize(SymmetricEncrypt::NONCE_LEN, 0),
                std::cmp::Ordering::Greater => {
                    let mut hasher = AHasher::default();
                    nonce.hash(&mut hasher);
                    let hash = hasher.finish();
                    nonce.clear();
                    nonce.extend_from_slice(hash.to_be_bytes().as_slice());
                    nonce.resize(SymmetricEncrypt::NONCE_LEN, 0)
                }
                std::cmp::Ordering::Equal => {}
            }

            // Encrypt random bytes with nonce
            let mut token = SymmetricEncrypt::new(key.as_bytes(), &context)
                .encrypt(&thread_rng().gen::<[u8; RANDOM_CODE_LEN]>(), &nonce)
                .map_err(StoreError::DeserializeError)?;
            account_id.to_leb128_bytes(&mut token);
            client_id.to_leb128_bytes(&mut token);
            expiry.to_leb128_bytes(&mut token);

            Ok(base64::encode(&token))
        })
        .await
    }

    pub async fn validate_access_token(
        &self,
        grant_type: &str,
        token: &str,
    ) -> store::Result<(AccountId, u64, u64)> {
        // Base64 decode token
        let token = base64::decode(token)
            .map_err(|e| StoreError::DeserializeError(format!("Failed to decode: {}", e)))?;
        let (account_id, client_id, expiry) = token
            .get((RANDOM_CODE_LEN + SymmetricEncrypt::ENCRYPT_TAG_LEN)..)
            .and_then(|bytes| {
                let mut bytes = bytes.iter();
                (
                    AccountId::from_leb128_it(&mut bytes)?,
                    u64::from_leb128_it(&mut bytes)?,
                    u64::from_leb128_it(&mut bytes)?,
                )
                    .into()
            })
            .ok_or_else(|| StoreError::DeserializeError("Failed to decode token.".into()))?;

        // Validate expiration
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
            .saturating_sub(946684800); // Jan 1, 2000
        if expiry <= now {
            return Err(StoreError::DeserializeError("Token expired.".into()));
        }

        // Optain password hash
        let store = self.store.clone();
        let password_hash = self
            .spawn_worker(move || store.get_account_secret_hash(account_id))
            .await?
            .ok_or_else(|| StoreError::DeserializeError("Account no longer exists".into()))?;

        // Create nonce
        let mut nonce = Vec::with_capacity(SymmetricEncrypt::NONCE_LEN);
        account_id.to_leb128_writer(&mut nonce).unwrap();
        expiry.to_leb128_writer(&mut nonce).unwrap();
        match nonce.len().cmp(&SymmetricEncrypt::NONCE_LEN) {
            std::cmp::Ordering::Less => nonce.resize(SymmetricEncrypt::NONCE_LEN, 0),
            std::cmp::Ordering::Greater => {
                let mut hasher = AHasher::default();
                nonce.hash(&mut hasher);
                let hash = hasher.finish();
                nonce.clear();
                nonce.extend_from_slice(hash.to_be_bytes().as_slice());
                nonce.resize(SymmetricEncrypt::NONCE_LEN, 0)
            }
            std::cmp::Ordering::Equal => {}
        }

        // Decrypt
        let key = self.oauth.key.clone();
        let context = format!("{} {} {}", grant_type, client_id, password_hash);
        self.spawn_worker(move || {
            SymmetricEncrypt::new(key.as_bytes(), &context)
                .decrypt(
                    &token[..RANDOM_CODE_LEN + SymmetricEncrypt::ENCRYPT_TAG_LEN],
                    &nonce,
                )
                .map_err(|e| StoreError::DeserializeError(format!("Failed to decrypt: {}", e)))
        })
        .await?;

        // Success
        Ok((account_id, client_id, expiry - now))
    }
}

impl OAuthMetadata {
    pub fn new(base_url: &str) -> Self {
        OAuthMetadata {
            issuer: base_url.to_string(),
            //authorization_endpoint: format!("{}/oauth/code", base_url),
            token_endpoint: format!("{}/auth/token", base_url),
            grant_types_supported: vec!["urn:ietf:params:oauth:grant-type:device_code".to_string()],
            device_authorization_endpoint: format!("{}/auth/device", base_url),
            response_types_supported: vec!["token".to_string()],
            scopes_supported: vec!["offline_access".to_string()],
        }
    }
}

impl TokenResponse {
    pub fn error(error: ErrorType) -> Self {
        TokenResponse::Error { error }
    }

    pub fn is_error(&self) -> bool {
        matches!(self, TokenResponse::Error { .. })
    }
}
