use jmap::jmap_store::changes::{ChangesObject, ChangesResult};
use store::core::collection::Collection;

pub struct ChangesEmailSubmission {}

impl ChangesObject for ChangesEmailSubmission {
    fn collection() -> Collection {
        Collection::EmailSubmission
    }

    fn handle_result(_result: &mut ChangesResult) {}
}
