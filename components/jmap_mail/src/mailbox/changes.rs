use jmap::jmap_store::changes::{ChangesObject, ChangesResult};
use store::Collection;

use super::MailboxProperty;

pub struct ChangesMailbox {}

impl ChangesObject for ChangesMailbox {
    fn collection() -> Collection {
        Collection::Mailbox
    }

    fn handle_result(result: &mut ChangesResult) {
        if result.has_children_changes {
            result.arguments.insert(
                "updatedProperties".to_string(),
                vec![
                    MailboxProperty::TotalEmails.into(),
                    MailboxProperty::UnreadEmails.into(),
                    MailboxProperty::TotalThreads.into(),
                    MailboxProperty::UnreadThreads.into(),
                ]
                .into(),
            );
        }
    }
}
