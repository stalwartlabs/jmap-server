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
