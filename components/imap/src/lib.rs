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
    Expunge,
    Search,
    Fetch,
    Store,
    Copy,
    Move,
    Uid,
}
