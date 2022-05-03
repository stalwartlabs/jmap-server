use jmap::jmap_store::changes::{ChangesObject, ChangesResult};
use store::core::collection::Collection;

pub struct ChangesMail {}

impl ChangesObject for ChangesMail {
    fn collection() -> Collection {
        Collection::Mail
    }

    fn handle_result(_result: &mut ChangesResult) {}
}
