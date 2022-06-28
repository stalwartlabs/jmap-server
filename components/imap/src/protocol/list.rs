use super::status::Status;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Arguments {
    Basic {
        reference_name: String,
        mailbox_name: String,
    },
    Extended {
        reference_name: String,
        mailbox_name: Vec<String>,
        selection_options: Vec<SelectionOption>,
        return_options: Vec<ReturnOption>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectionOption {
    Subscribed,
    Remote,
    RecursiveMatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReturnOption {
    Subscribed,
    Children,
    Status(Vec<Status>),
}
