use std::collections::HashMap;

use store::{AccountId, Collection, JMAPId, JMAPStore, Store};

use crate::{
    error::method::MethodError,
    id::{state::JMAPState, JMAPIdSerialize},
    protocol::json::JSONValue,
    request::import::ImportRequest,
};

use super::changes::JMAPChanges;

#[derive(Default)]
pub struct ImportResult {
    pub account_id: AccountId,
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

    fn init(store: &'y JMAPStore<T>, request: &mut ImportRequest) -> crate::Result<Self>;
    fn parse_items(
        &self,
        request: &mut ImportRequest,
    ) -> crate::Result<HashMap<String, Self::Item>>;
    fn import_item(&self, item: Self::Item) -> crate::Result<Result<JSONValue, JSONValue>>;
    fn collection() -> Collection;
}

pub trait JMAPImport<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn import<'y, 'z: 'y, U>(&'z self, request: ImportRequest) -> crate::Result<ImportResult>
    where
        U: ImportObject<'y, T>;
}

impl<T> JMAPImport<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn import<'y, 'z: 'y, U>(&'z self, mut request: ImportRequest) -> crate::Result<ImportResult>
    where
        U: ImportObject<'y, T>,
    {
        let object = U::init(self, &mut request)?;
        let collection = U::collection();
        let items = object.parse_items(&mut request)?;

        let old_state = self.get_state(request.account_id, collection)?;
        if let Some(if_in_state) = request.if_in_state {
            if old_state != if_in_state {
                return Err(MethodError::StateMismatch);
            }
        }

        let mut created = HashMap::with_capacity(items.len());
        let mut not_created = HashMap::with_capacity(items.len());

        for (id, item) in items {
            match object.import_item(item)? {
                Ok(value) => {
                    created.insert(id, value);
                }
                Err(value) => {
                    not_created.insert(id, value);
                }
            }
        }

        Ok(ImportResult {
            account_id: request.account_id,
            new_state: if !created.is_empty() {
                self.get_state(request.account_id, collection)?
            } else {
                old_state.clone()
            },
            old_state,
            created,
            not_created,
        })
    }
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
