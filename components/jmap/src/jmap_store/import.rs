use std::collections::HashMap;

use store::{core::collection::Collection, AccountId, JMAPId, JMAPStore, Store};

use crate::{
    error::method::MethodError,
    id::{state::JMAPState, JMAPIdSerialize},
    protocol::json::JSONValue,
};

use super::changes::JMAPChanges;

#[derive(Default)]
pub struct ImportResult {
    pub account_id: AccountId,
    pub collection: Collection,
    pub old_state: JMAPState,
    pub new_state: JMAPState,
    pub created: HashMap<String, JSONValue>,
    pub not_created: HashMap<String, JSONValue>,
}

pub trait ImportObject<'y, T>: Sized
where
    T: for<'x> Store<'x> + 'static,
{
    type Item;

    fn new(store: &'y JMAPStore<T>, request: &mut ImportRequest) -> crate::Result<Self>;
    fn parse_items(
        &self,
        request: &mut ImportRequest,
    ) -> crate::Result<HashMap<String, Self::Item>>;
    fn import_item(&self, item: Self::Item) -> crate::error::set::Result<JSONValue>;
    fn collection() -> Collection;
}

impl From<ImportResult> for JSONValue {
    fn from(import_result: ImportResult) -> Self {
        let mut result = HashMap::new();
        result.insert(
            "accountId".to_string(),
            (import_result.account_id as JMAPId).to_jmap_string().into(),
        );
        result.insert("newState".to_string(), import_result.new_state.into());
        result.insert("oldState".to_string(), import_result.old_state.into());
        result.insert("created".to_string(), import_result.created.into());
        result.insert("notCreated".to_string(), import_result.not_created.into());
        result.into()
    }
}
