use jmap::jmap_store::changes::{ChangesObject, ChangesResult};
use store::core::collection::Collection;

pub struct ChangesIdentity {}

impl ChangesObject for ChangesIdentity {
    fn collection() -> Collection {
        Collection::Identity
    }

    fn handle_result(_result: &mut ChangesResult) {}
}
