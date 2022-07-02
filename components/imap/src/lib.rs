use std::borrow::Cow;

pub mod parser;
pub mod protocol;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    // Client Commands - Any State
    Capability,
    Noop,
    Logout,
    // Client Commands - Not Authenticated State
    StartTls,
    Authenticate,
    Login,

    // Client Commands - Authenticated State
    Enable,
    Select,
    Examine,
    Create,
    Delete,
    Rename,
    Subscribe,
    Unsubscribe,
    List,
    Namespace,
    Status,
    Append,
    Idle,

    // Client Commands - Selected State
    Close,
    Unselect,
    Expunge(bool),
    Search(bool),
    Fetch(bool),
    Store(bool),
    Copy(bool),
    Move(bool),

    // IMAP4rev1
    Lsub,
    Check,

    // RFC5256
    Sort(bool),
    Thread(bool),
}

impl Command {
    #[inline(always)]
    pub fn is_fetch(&self) -> bool {
        matches!(self, Command::Fetch(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseCode {
    Alert,
    AlreadyExists,
    AppendUid,
    AuthenticationFailed,
    AuthorizationFailed,
    BadCharset,
    Cannot,
    Capability,
    ClientBug,
    Closed,
    ContactAdmin,
    CopyUid,
    Corruption,
    Expired,
    ExpungeIssued,
    HasChildren,
    InUse,
    Limit,
    Nonexistent,
    NoPerm,
    OverQuota,
    Parse,
    PermanentFlags,
    PrivacyRequired,
    ReadOnly,
    ReadWrite,
    ServerBug,
    TryCreate,
    UidNext,
    UidNotSticky,
    UidValidity,
    Unavailable,
    UnknownCte,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusResponse {
    pub tag: Option<String>,
    pub code: Option<ResponseCode>,
    pub message: Cow<'static, str>,
    pub rtype: ResponseType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseType {
    Ok,
    No,
    Bad,
    PreAuth,
    Bye,
}

impl StatusResponse {
    pub fn bad(
        tag: Option<String>,
        code: Option<ResponseCode>,
        message: impl Into<Cow<'static, str>>,
    ) -> Self {
        StatusResponse {
            tag,
            code,
            message: message.into(),
            rtype: ResponseType::Bad,
        }
    }

    pub fn parse(tag: Option<String>, message: impl Into<Cow<'static, str>>) -> Self {
        StatusResponse {
            tag,
            code: ResponseCode::Parse.into(),
            message: message.into(),
            rtype: ResponseType::Bad,
        }
    }

    pub fn no(
        tag: Option<String>,
        code: Option<ResponseCode>,
        message: impl Into<Cow<'static, str>>,
    ) -> Self {
        StatusResponse {
            tag,
            code,
            message: message.into(),
            rtype: ResponseType::No,
        }
    }

    pub fn ok(
        tag: Option<String>,
        code: Option<ResponseCode>,
        message: impl Into<Cow<'static, str>>,
    ) -> Self {
        StatusResponse {
            tag,
            code,
            message: message.into(),
            rtype: ResponseType::Ok,
        }
    }
}

pub type Result<T> = std::result::Result<T, StatusResponse>;
