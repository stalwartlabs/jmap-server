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

use std::{
    sync::{
        atomic::{self, AtomicU32},
        Arc,
    },
    time::{Instant, SystemTime},
};

use crate::JMAPServer;
use actix_web::{http::header, web, HttpResponse};
use jmap_mail::{
    mail_builder::encoders::base64::base64_encode, mail_parser::decoders::base64::decode_base64,
};
use jmap_sharing::principal::account::JMAPAccountStore;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::fmt::Write;
use store::{
    bincode, blake3,
    core::error::StoreError,
    rand::{
        distributions::{Alphanumeric, Standard},
        thread_rng, Rng,
    },
    serialize::leb128::{Leb128Iterator, Leb128Vec},
    tracing::{debug, error},
    AccountId, Store,
};

use super::SymmetricEncrypt;

const OAUTH_HTML_HEADER: &str = include_str!("../../resources/oauth/header.htx");
const OAUTH_HTML_FOOTER: &str = include_str!("../../resources/oauth/footer.htx");
const OAUTH_HTML_LOGIN_HEADER_CLIENT: &str =
    include_str!("../../resources/oauth/login_hdr_client.htx");
const OAUTH_HTML_LOGIN_HEADER_DEVICE: &str =
    include_str!("../../resources/oauth/login_hdr_device.htx");
const OAUTH_HTML_LOGIN_HEADER_FAILED: &str =
    include_str!("../../resources/oauth/login_hdr_failed.htx");
const OAUTH_HTML_LOGIN_FORM: &str = include_str!("../../resources/oauth/login.htx");
const OAUTH_HTML_LOGIN_CODE: &str = include_str!("../../resources/oauth/login_code.htx");
const OAUTH_HTML_LOGIN_CODE_HIDDEN: &str =
    include_str!("../../resources/oauth/login_code_hidden.htx");
const OAUTH_HTML_LOGIN_SUCCESS: &str = include_str!("../../resources/oauth/login_success.htx");
const OAUTH_HTML_ERROR: &str = include_str!("../../resources/oauth/error.htx");

const STATUS_AUTHORIZED: u32 = 0;
const STATUS_TOKEN_ISSUED: u32 = 1;
const STATUS_PENDING: u32 = 2;

const DEVICE_CODE_LEN: usize = 40;
const USER_CODE_LEN: usize = 8;
const RANDOM_CODE_LEN: usize = 32;
const CLIENT_ID_MAX_LEN: usize = 20;

const USER_CODE_ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // No 0, O, I, 1

pub struct OAuth {
    pub key: String,
    pub expiry_user_code: u64,
    pub expiry_auth_code: u64,
    pub expiry_token: u64,
    pub expiry_refresh_token: u64,
    pub expiry_refresh_token_renew: u64,
    pub max_auth_attempts: u32,
    pub metadata: String,
}

pub struct OAuthCode {
    pub status: AtomicU32,
    pub account_id: AtomicU32,
    pub expiry: Instant,
    pub client_id: String,
    pub redirect_uri: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeviceAuthGet {
    code: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeviceAuthPost {
    code: Option<String>,
    email: Option<String>,
    password: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeviceAuthRequest {
    client_id: String,
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
pub struct CodeAuthRequest {
    response_type: String,
    client_id: String,
    redirect_uri: String,
    scope: Option<String>,
    state: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CodeAuthForm {
    code: String,
    email: Option<String>,
    password: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenRequest {
    pub grant_type: String,
    pub code: Option<String>,
    pub device_code: Option<String>,
    pub client_id: Option<String>,
    pub refresh_token: Option<String>,
    pub redirect_uri: Option<String>,
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
    pub authorization_endpoint: String,
}

// Device authorization endpoint
pub async fn handle_device_auth<T>(
    core: web::Data<JMAPServer<T>>,
    params: web::Form<DeviceAuthRequest>,
) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    // Validate clientId
    if params.client_id.len() > CLIENT_ID_MAX_LEN {
        return HttpResponse::BadRequest().body("Client ID is too long");
    }

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
    let oauth_code = Arc::new(OAuthCode {
        status: STATUS_PENDING.into(),
        account_id: u32::MAX.into(),
        expiry: Instant::now(),
        client_id: params.into_inner().client_id,
        redirect_uri: None,
    });
    core.oauth_codes
        .insert(device_code.clone(), oauth_code.clone())
        .await;
    core.oauth_codes.insert(user_code.clone(), oauth_code).await;

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

// Token endpoint
pub async fn handle_token_request<T>(
    core: web::Data<JMAPServer<T>>,
    params: web::Form<TokenRequest>,
) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    let mut response = TokenResponse::error(ErrorType::InvalidGrant);

    if params.grant_type.eq_ignore_ascii_case("authorization_code") {
        response = if let (Some(code), Some(client_id), Some(redirect_uri)) =
            (&params.code, &params.client_id, &params.redirect_uri)
        {
            if let Some(oauth) = core.oauth_codes.get(code) {
                if client_id != &oauth.client_id
                    || redirect_uri != oauth.redirect_uri.as_deref().unwrap_or("")
                {
                    TokenResponse::error(ErrorType::InvalidClient)
                } else if oauth.status.load(atomic::Ordering::Relaxed) == STATUS_AUTHORIZED
                    && oauth.expiry.elapsed().as_secs() < core.oauth.expiry_auth_code
                {
                    // Mark this token as issued
                    oauth
                        .status
                        .store(STATUS_TOKEN_ISSUED, atomic::Ordering::Relaxed);

                    // Issue token
                    core.issue_token(
                        oauth.account_id.load(atomic::Ordering::Relaxed),
                        &oauth.client_id,
                        true,
                    )
                    .await
                    .unwrap_or_else(|err| {
                        error!("Failed to generate OAuth token: {}", err);
                        TokenResponse::error(ErrorType::InvalidRequest)
                    })
                } else {
                    TokenResponse::error(ErrorType::InvalidGrant)
                }
            } else {
                TokenResponse::error(ErrorType::AccessDenied)
            }
        } else {
            TokenResponse::error(ErrorType::InvalidClient)
        };
    } else if params
        .grant_type
        .eq_ignore_ascii_case("urn:ietf:params:oauth:grant-type:device_code")
    {
        response = TokenResponse::error(ErrorType::ExpiredToken);

        if let (Some(oauth), Some(client_id)) = (
            params
                .device_code
                .as_ref()
                .and_then(|dc| core.oauth_codes.get(dc)),
            &params.client_id,
        ) {
            if &oauth.client_id != client_id {
                response = TokenResponse::error(ErrorType::InvalidClient);
            } else if oauth.expiry.elapsed().as_secs() < core.oauth.expiry_user_code {
                response = match oauth.status.load(atomic::Ordering::Relaxed) {
                    STATUS_AUTHORIZED => {
                        // Mark this token as issued
                        oauth
                            .status
                            .store(STATUS_TOKEN_ISSUED, atomic::Ordering::Relaxed);

                        // Issue token
                        core.issue_token(
                            oauth.account_id.load(atomic::Ordering::Relaxed),
                            &oauth.client_id,
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
        }
    } else if params.grant_type.eq_ignore_ascii_case("refresh_token") {
        if let Some(refresh_token) = &params.refresh_token {
            match core
                .validate_access_token("refresh_token", refresh_token)
                .await
            {
                Ok((account_id, client_id, time_left)) => {
                    // TODO: implement revoking client ids
                    response = core
                        .issue_token(
                            account_id,
                            &client_id,
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

// Code authorization flow, handles an authorization request
pub async fn handle_user_code_auth<T>(params: web::Query<CodeAuthRequest>) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    // Validate clientId
    if params.client_id.len() > CLIENT_ID_MAX_LEN {
        return HttpResponse::BadRequest().body("Client ID is too long");
    } else if !params.redirect_uri.starts_with("https://") {
        return HttpResponse::BadRequest().body("Redirect URI must be HTTPS");
    }

    let params = params.into_inner();
    let mut cancel_link = format!("{}?error=access_denied", params.redirect_uri);
    if let Some(state) = &params.state {
        let _ = write!(cancel_link, "&state={}", state);
    }
    let code = String::from_utf8(
        base64_encode(&bincode::serialize(&(1u32, params)).unwrap_or_default()).unwrap_or_default(),
    )
    .unwrap();

    let mut response = String::with_capacity(
        OAUTH_HTML_HEADER.len()
            + OAUTH_HTML_LOGIN_HEADER_CLIENT.len()
            + OAUTH_HTML_LOGIN_CODE_HIDDEN.len()
            + OAUTH_HTML_LOGIN_FORM.len()
            + OAUTH_HTML_FOOTER.len()
            + code.len()
            + cancel_link.len()
            + 10,
    );

    response.push_str(&OAUTH_HTML_HEADER.replace("@@@", "/auth/code"));
    response.push_str(OAUTH_HTML_LOGIN_HEADER_CLIENT);
    response.push_str(&OAUTH_HTML_LOGIN_CODE_HIDDEN.replace("@@@", &code));
    response.push_str(&OAUTH_HTML_LOGIN_FORM.replace("@@@", &cancel_link));
    response.push_str(OAUTH_HTML_FOOTER);

    HttpResponse::build(StatusCode::OK)
        .content_type("text/html; charset=utf-8")
        .body(response)
}

// Handles POST request from the code authorization form
pub async fn handle_user_code_auth_post<T>(
    core: web::Data<JMAPServer<T>>,
    params: web::Form<CodeAuthForm>,
) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    let mut auth_code = None;
    let params = params.into_inner();
    let (auth_attempts, code_req) = match decode_base64(params.code.as_bytes())
        .and_then(|bytes| bincode::deserialize::<(u32, CodeAuthRequest)>(&bytes).ok())
    {
        Some(code) => code,
        None => {
            return HttpResponse::BadRequest().body("Failed to deserialize code.");
        }
    };

    // Authenticate user
    if let (Some(email), Some(password)) = (params.email, params.password) {
        let store = core.store.clone();

        if let Ok(Some(account_id)) = core
            .spawn_worker(move || store.authenticate(&email, &password))
            .await
        {
            // Generate client code
            let client_code = thread_rng()
                .sample_iter(Alphanumeric)
                .take(DEVICE_CODE_LEN)
                .map(char::from)
                .collect::<String>();

            // Add client code
            core.oauth_codes
                .insert(
                    client_code.clone(),
                    Arc::new(OAuthCode {
                        status: STATUS_AUTHORIZED.into(),
                        account_id: account_id.into(),
                        expiry: Instant::now(),
                        client_id: code_req.client_id.clone(),
                        redirect_uri: code_req.redirect_uri.clone().into(),
                    }),
                )
                .await;

            auth_code = client_code.into();
        }
    }

    // Build redirect link
    let mut redirect_link = if let Some(auth_code) = &auth_code {
        format!("{}?code={}", code_req.redirect_uri, auth_code)
    } else {
        format!("{}?error=access_denied", code_req.redirect_uri)
    };
    if let Some(state) = &code_req.state {
        let _ = write!(redirect_link, "&state={}", state);
    }

    if auth_code.is_none() && (auth_attempts < core.oauth.max_auth_attempts) {
        let code = String::from_utf8(
            base64_encode(&bincode::serialize(&(auth_attempts + 1, code_req)).unwrap_or_default())
                .unwrap_or_default(),
        )
        .unwrap();

        let mut response = String::with_capacity(
            OAUTH_HTML_HEADER.len()
                + OAUTH_HTML_LOGIN_HEADER_CLIENT.len()
                + OAUTH_HTML_LOGIN_CODE_HIDDEN.len()
                + OAUTH_HTML_LOGIN_FORM.len()
                + OAUTH_HTML_FOOTER.len()
                + code.len()
                + redirect_link.len()
                + 10,
        );
        response.push_str(&OAUTH_HTML_HEADER.replace("@@@", "/auth/code"));
        response.push_str(OAUTH_HTML_LOGIN_HEADER_FAILED);
        response.push_str(&OAUTH_HTML_LOGIN_CODE_HIDDEN.replace("@@@", &code));
        response.push_str(&OAUTH_HTML_LOGIN_FORM.replace("@@@", &redirect_link));
        response.push_str(OAUTH_HTML_FOOTER);

        HttpResponse::build(StatusCode::OK)
            .content_type("text/html; charset=utf-8")
            .body(response)
    } else {
        HttpResponse::build(StatusCode::TEMPORARY_REDIRECT)
            .insert_header((header::LOCATION, redirect_link))
            .finish()
    }
}

// Device authorization flow, renders the authorization page
pub async fn handle_user_device_auth<T>(params: web::Query<DeviceAuthGet>) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    let code = params.code.as_deref().unwrap_or("");
    let mut response = String::with_capacity(
        OAUTH_HTML_HEADER.len()
            + OAUTH_HTML_LOGIN_HEADER_DEVICE.len()
            + OAUTH_HTML_LOGIN_CODE.len()
            + OAUTH_HTML_LOGIN_FORM.len()
            + OAUTH_HTML_FOOTER.len()
            + code.len()
            + 16,
    );

    response.push_str(&OAUTH_HTML_HEADER.replace("@@@", "/auth"));
    response.push_str(OAUTH_HTML_LOGIN_HEADER_DEVICE);
    response.push_str(&OAUTH_HTML_LOGIN_CODE.replace("@@@", code));
    response.push_str(&OAUTH_HTML_LOGIN_FORM.replace("@@@", "about:blank"));
    response.push_str(OAUTH_HTML_FOOTER);

    HttpResponse::build(StatusCode::OK)
        .content_type("text/html; charset=utf-8")
        .body(response)
}

// Handles POST request from the device authorization form
pub async fn handle_user_device_auth_post<T>(
    core: web::Data<JMAPServer<T>>,
    params: web::Form<DeviceAuthPost>,
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
        .and_then(|code| core.oauth_codes.get(code))
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
        OAUTH_HTML_HEADER.len()
            + OAUTH_HTML_LOGIN_HEADER_DEVICE.len()
            + OAUTH_HTML_LOGIN_CODE.len()
            + OAUTH_HTML_LOGIN_FORM.len()
            + OAUTH_HTML_FOOTER.len()
            + USER_CODE_LEN
            + 17,
    );
    response.push_str(&OAUTH_HTML_HEADER.replace("@@@", "/auth"));

    match code {
        Response::Success => {
            response.push_str(OAUTH_HTML_LOGIN_SUCCESS);
        }
        Response::Failed => {
            response.push_str(OAUTH_HTML_LOGIN_HEADER_FAILED);
            response.push_str(
                &OAUTH_HTML_LOGIN_CODE.replace("@@@", params.code.as_deref().unwrap_or("")),
            );
            response.push_str(&OAUTH_HTML_LOGIN_FORM.replace("@@@", "about:blank"));
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

// /.well-known/oauth-authorization-server endpoint
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
        client_id: &str,
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
            access_token: self.encode_access_token(
                "access_token",
                account_id,
                &password_hash,
                client_id,
                self.oauth.expiry_token,
            )?,
            token_type: "bearer".to_string(),
            expires_in: self.oauth.expiry_token,
            refresh_token: if with_refresh_token {
                self.encode_access_token(
                    "refresh_token",
                    account_id,
                    &password_hash,
                    client_id,
                    self.oauth.expiry_refresh_token,
                )?
                .into()
            } else {
                None
            },
            scope: None,
        })
    }

    fn encode_access_token(
        &self,
        grant_type: &str,
        account_id: u32,
        password_hash: &str,
        client_id: &str,
        expiry_in: u64,
    ) -> store::Result<String> {
        // Build context
        if client_id.len() > CLIENT_ID_MAX_LEN {
            return Err(StoreError::DeserializeError("ClientId is too long".into()));
        }
        let key = self.oauth.key.clone();
        let context = format!(
            "{} {} {} {}",
            grant_type, client_id, account_id, password_hash
        );
        let context_nonce = format!("{} nonce {}", grant_type, password_hash);

        // Set expiration time
        let expiry = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
                .saturating_sub(946684800) // Jan 1, 2000
                + expiry_in;

        // Calculate nonce
        let mut hasher = blake3::Hasher::new();
        hasher.update(context_nonce.as_bytes());
        hasher.update(expiry.to_be_bytes().as_slice());
        let nonce = hasher
            .finalize()
            .as_bytes()
            .iter()
            .take(SymmetricEncrypt::NONCE_LEN)
            .copied()
            .collect::<Vec<_>>();

        // Encrypt random bytes
        let mut token = SymmetricEncrypt::new(key.as_bytes(), &context)
            .encrypt(&thread_rng().gen::<[u8; RANDOM_CODE_LEN]>(), &nonce)
            .map_err(StoreError::DeserializeError)?;
        token.push_leb128(account_id);
        token.push_leb128(expiry);
        token.extend_from_slice(client_id.as_bytes());

        Ok(String::from_utf8(base64_encode(&token).unwrap_or_default()).unwrap())
    }

    pub async fn validate_access_token(
        &self,
        grant_type: &str,
        token: &str,
    ) -> store::Result<(AccountId, String, u64)> {
        // Base64 decode token
        let token = decode_base64(token.as_bytes())
            .ok_or_else(|| StoreError::DeserializeError("Failed to decode.".to_string()))?;
        let (account_id, expiry, client_id) = token
            .get((RANDOM_CODE_LEN + SymmetricEncrypt::ENCRYPT_TAG_LEN)..)
            .and_then(|bytes| {
                let mut bytes = bytes.iter();
                (
                    bytes.next_leb128()?,
                    bytes.next_leb128::<u64>()?,
                    bytes.copied().map(char::from).collect::<String>(),
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

        // Build context
        let key = self.oauth.key.clone();
        let context = format!(
            "{} {} {} {}",
            grant_type, client_id, account_id, password_hash
        );
        let context_nonce = format!("{} nonce {}", grant_type, password_hash);

        // Calculate nonce
        let mut hasher = blake3::Hasher::new();
        hasher.update(context_nonce.as_bytes());
        hasher.update(expiry.to_be_bytes().as_slice());
        let nonce = hasher
            .finalize()
            .as_bytes()
            .iter()
            .take(SymmetricEncrypt::NONCE_LEN)
            .copied()
            .collect::<Vec<_>>();

        // Decrypt
        SymmetricEncrypt::new(key.as_bytes(), &context)
            .decrypt(
                &token[..RANDOM_CODE_LEN + SymmetricEncrypt::ENCRYPT_TAG_LEN],
                &nonce,
            )
            .map_err(|e| StoreError::DeserializeError(format!("Failed to decrypt: {}", e)))?;

        // Success
        Ok((account_id, client_id, expiry - now))
    }
}

impl OAuthMetadata {
    pub fn new(base_url: &str) -> Self {
        OAuthMetadata {
            issuer: base_url.to_string(),
            authorization_endpoint: format!("{}/auth/code", base_url),
            token_endpoint: format!("{}/auth/token", base_url),
            grant_types_supported: vec![
                "authorization_code".to_string(),
                "implicit".to_string(),
                "urn:ietf:params:oauth:grant-type:device_code".to_string(),
            ],
            device_authorization_endpoint: format!("{}/auth/device", base_url),
            response_types_supported: vec!["code".to_string(), "code token".to_string()],
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
