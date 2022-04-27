use std::collections::{HashMap, HashSet};

use crate::error::set::SetError;
use crate::id::state::JMAPState;
use crate::id::JMAPIdSerialize;
use crate::protocol::invocation::Invocation;
use crate::Property;
use crate::{
    error::{method::MethodError, set::SetErrorType},
    protocol::{json::JSONValue, json_pointer::JSONPointer},
    request::set::SetRequest,
};
use store::batch::Document;
use store::parking_lot::MutexGuard;
use store::{batch::WriteBatch, roaring::RoaringBitmap, JMAPId, JMAPStore, Store};
use store::{AccountId, Collection, JMAPIdPrefix};

use super::changes::JMAPChanges;

pub struct SetObjectHelper<'y, T, U>
where
    T: for<'x> Store<'x> + 'static,
    U: SetObjectData<T>,
{
    pub store: &'y JMAPStore<T>,
    pub lock: Option<MutexGuard<'y, ()>>,
    pub changes: WriteBatch,
    pub document_ids: RoaringBitmap,
    pub account_id: AccountId,
    pub will_destroy: HashSet<JMAPId>,

    pub created: HashMap<String, JSONValue>,
    pub not_created: HashMap<String, JSONValue>,
    pub updated: HashMap<String, JSONValue>,
    pub not_updated: HashMap<String, JSONValue>,
    pub destroyed: Vec<JSONValue>,
    pub not_destroyed: HashMap<String, JSONValue>,
    pub data: U,
}

pub struct SetResult {
    pub account_id: AccountId,
    pub new_state: JMAPState,
    pub old_state: JMAPState,
    pub created: HashMap<String, JSONValue>,
    pub not_created: HashMap<String, JSONValue>,
    pub updated: HashMap<String, JSONValue>,
    pub not_updated: HashMap<String, JSONValue>,
    pub destroyed: Vec<JSONValue>,
    pub not_destroyed: HashMap<String, JSONValue>,
    pub next_invocation: Option<Invocation>,
}

pub struct DefaultCreateItem {
    pub id: JMAPId,
}

#[derive(Default)]
pub struct DefaultUpdateItem {}

pub trait CreateItemResult: Into<JSONValue> {
    fn get_id(&self) -> JMAPId;
}

pub trait UpdateItemResult: Into<JSONValue> {}

pub trait SetObjectData<T>: Sized
where
    T: for<'x> Store<'x> + 'static,
{
    fn new(store: &JMAPStore<T>, request: &mut SetRequest) -> crate::Result<Self>;
    fn unwrap_invocation(self) -> Option<Invocation>;
}

pub trait SetObject<'y, T>: Sized
where
    T: for<'x> Store<'x> + 'static,
{
    type Property: Property;
    type CreateItemResult: CreateItemResult;
    type UpdateItemResult: UpdateItemResult;
    type Helper: SetObjectData<T>;

    fn new(
        helper: &mut SetObjectHelper<T, Self::Helper>,
        fields: &mut HashMap<String, JSONValue>,
        jmap_id: Option<JMAPId>,
    ) -> crate::error::set::Result<Self>;
    fn set_field(
        &mut self,
        helper: &mut SetObjectHelper<T, Self::Helper>,
        field: Self::Property,
        value: JSONValue,
    ) -> crate::error::set::Result<()>;
    fn patch_field(
        &mut self,
        helper: &mut SetObjectHelper<T, Self::Helper>,
        field: Self::Property,
        property: String,
        value: JSONValue,
    ) -> crate::error::set::Result<()>;
    fn create(
        self,
        helper: &mut SetObjectHelper<T, Self::Helper>,
        create_id: &str,
        document: &mut Document,
    ) -> crate::error::set::Result<Self::CreateItemResult>;
    fn update(
        self,
        helper: &mut SetObjectHelper<T, Self::Helper>,
        document: &mut Document,
    ) -> crate::error::set::Result<Option<Self::UpdateItemResult>>;
    fn delete(
        helper: &mut SetObjectHelper<T, Self::Helper>,
        document: &mut Document,
    ) -> crate::error::set::Result<()>;
}

pub trait JMAPSet<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn set<'y, 'z: 'y, V>(&'z self, request: SetRequest) -> crate::Result<SetResult>
    where
        V: SetObject<'y, T>;
}

impl<T> JMAPSet<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn set<'y, 'z: 'y, V>(&'z self, mut request: SetRequest) -> crate::Result<SetResult>
    where
        V: SetObject<'y, T>,
    {
        let collection = V::Property::collection();
        let data = V::Helper::new(self, &mut request)?;
        let mut change_id = None;

        let old_state = self.get_state(request.account_id, collection)?;
        if let Some(if_in_state) = request.if_in_state {
            if old_state != if_in_state {
                return Err(MethodError::StateMismatch);
            }
        }

        let mut will_destroy = HashSet::new();
        let destroyed = Vec::with_capacity(request.destroy.len());
        let mut not_destroyed = HashMap::with_capacity(request.destroy.len());

        for destroy_id in request.destroy {
            if let Some(jmap_id_str) = destroy_id.to_string() {
                if let Some(jmap_id) = JMAPId::from_jmap_string(jmap_id_str) {
                    will_destroy.insert(jmap_id);
                } else {
                    not_destroyed.insert(
                        jmap_id_str.to_string(),
                        SetError::new(SetErrorType::InvalidProperties, "Failed to parse Id.")
                            .into(),
                    );
                }
            }
        }

        let mut helper = SetObjectHelper {
            store: self,
            lock: None,
            changes: WriteBatch::new(request.account_id, self.config.is_in_cluster),
            document_ids: self
                .get_document_ids(request.account_id, collection)?
                .unwrap_or_else(RoaringBitmap::new),
            account_id: request.account_id,
            will_destroy,
            created: HashMap::with_capacity(request.create.len()),
            not_created: HashMap::with_capacity(request.create.len()),
            updated: HashMap::with_capacity(request.update.len()),
            not_updated: HashMap::with_capacity(request.update.len()),
            destroyed,
            not_destroyed,
            data,
        };

        'create: for (create_id, fields) in request.create {
            if let Some(mut fields) = fields.unwrap_object() {
                let mut object = match V::new(&mut helper, &mut fields, None) {
                    Ok(object) => object,
                    Err(err) => {
                        helper.not_created.insert(create_id, err.into());
                        continue 'create;
                    }
                };
                for (field, value) in fields {
                    if let Some(field) = V::Property::parse(&field) {
                        if let Err(err) = object.set_field(&mut helper, field, value) {
                            helper.not_created.insert(create_id, err.into());
                            continue 'create;
                        }
                    } else {
                        helper.not_created.insert(
                            create_id,
                            SetError::invalid_property(field, "Unsupported property.").into(),
                        );
                        continue 'create;
                    }
                }

                let mut document = Document::new(
                    collection,
                    helper
                        .store
                        .assign_document_id(helper.account_id, collection)?,
                );

                match object.create(&mut helper, &create_id, &mut document) {
                    Ok(result) => {
                        helper.document_ids.insert(document.document_id);
                        helper.changes.insert_document(document);
                        helper.changes.log_insert(collection, result.get_id());
                        if helper.lock.is_some() {
                            change_id = self.write(helper.changes)?;
                            helper.changes =
                                WriteBatch::new(request.account_id, self.config.is_in_cluster);
                            helper.lock = None;
                        }
                        helper.created.insert(create_id, result.into());
                    }
                    Err(err) => {
                        helper.not_created.insert(create_id, err.into());
                    }
                };
            } else {
                helper.not_created.insert(
                    create_id,
                    SetError::new(
                        SetErrorType::InvalidProperties,
                        "Failed to parse request, expected object.",
                    )
                    .into(),
                );
            };
        }

        'update: for (jmap_id_str, fields) in request.update {
            let (jmap_id, mut fields) = if let (Some(jmap_id), Some(fields)) = (
                JMAPId::from_jmap_string(&jmap_id_str),
                fields.unwrap_object(),
            ) {
                (jmap_id, fields)
            } else {
                helper.not_updated.insert(
                    jmap_id_str,
                    SetError::new(SetErrorType::InvalidProperties, "Failed to parse request.")
                        .into(),
                );
                continue;
            };

            let document_id = jmap_id.get_document_id();
            if !helper.document_ids.contains(document_id) {
                helper.not_updated.insert(
                    jmap_id_str,
                    SetError::new(SetErrorType::NotFound, "ID not found.").into(),
                );
                continue;
            } else if helper.will_destroy.contains(&jmap_id) {
                helper.not_updated.insert(
                    jmap_id_str,
                    SetError::new(SetErrorType::WillDestroy, "ID will be destroyed.").into(),
                );
                continue;
            }

            let mut object = match V::new(&mut helper, &mut fields, jmap_id.into()) {
                Ok(object) => object,
                Err(err) => {
                    helper.not_updated.insert(jmap_id_str, err.into());
                    continue 'update;
                }
            };

            for (field, value) in fields {
                match JSONPointer::parse(&field).unwrap_or(JSONPointer::Root) {
                    JSONPointer::String(field) => {
                        if let Some(field) = V::Property::parse(&field) {
                            if let Err(err) = object.set_field(&mut helper, field, value) {
                                helper.not_updated.insert(jmap_id_str, err.into());
                                continue 'update;
                            }
                        } else {
                            helper.not_updated.insert(
                                jmap_id_str,
                                SetError::invalid_property(field, "Unsupported property.").into(),
                            );
                            continue 'update;
                        }
                    }

                    JSONPointer::Path(mut path) if path.len() == 2 => {
                        if let (JSONPointer::String(property), JSONPointer::String(field)) =
                            (path.pop().unwrap(), path.pop().unwrap())
                        {
                            if let Some(field) = V::Property::parse(&field) {
                                if let Err(err) =
                                    object.patch_field(&mut helper, field, property, value)
                                {
                                    helper.not_updated.insert(jmap_id_str, err.into());
                                    continue 'update;
                                }
                            } else {
                                helper.not_updated.insert(
                                    format!("{}/{}", field, property),
                                    SetError::invalid_property(field, "Unsupported property.")
                                        .into(),
                                );
                                continue 'update;
                            }
                        } else {
                            helper.not_updated.insert(
                                jmap_id_str,
                                SetError::invalid_property(field, "Unsupported property.").into(),
                            );
                            continue 'update;
                        }
                    }
                    _ => {
                        helper.not_updated.insert(
                            jmap_id_str,
                            SetError::invalid_property(field.to_string(), "Unsupported property.")
                                .into(),
                        );
                        continue 'update;
                    }
                }
            }

            let mut document = Document::new(collection, document_id);
            match object.update(&mut helper, &mut document) {
                Ok(Some(result)) => {
                    helper.changes.update_document(document);
                    helper.changes.log_update(collection, jmap_id);
                    helper.updated.insert(jmap_id_str, result.into());
                }
                Ok(None) => {
                    helper.updated.insert(jmap_id_str, JSONValue::Null);
                }
                Err(err) => {
                    helper.not_updated.insert(jmap_id_str, err.into());
                }
            };
        }

        for jmap_id in std::mem::take(&mut helper.will_destroy) {
            let document_id = jmap_id.get_document_id();
            let mut document = Document::new(collection, document_id);
            if helper.document_ids.contains(document_id) {
                if let Err(err) = V::delete(&mut helper, &mut document) {
                    helper
                        .not_destroyed
                        .insert(jmap_id.to_jmap_string(), err.into());
                } else {
                    helper.changes.delete_document(document);
                    helper.changes.log_delete(collection, jmap_id);
                    helper.destroyed.push(jmap_id.to_jmap_string().into());
                }
            } else {
                helper.not_destroyed.insert(
                    jmap_id.to_jmap_string(),
                    SetError::new(SetErrorType::NotFound, "ID not found.").into(),
                );
            }
        }

        if !helper.changes.is_empty() {
            change_id = self.write(helper.changes)?;
        }

        Ok(SetResult {
            account_id: request.account_id,
            new_state: if let Some(change_id) = change_id {
                change_id.into()
            } else {
                old_state.clone()
            },
            old_state,
            created: helper.created,
            not_created: helper.not_created,
            updated: helper.updated,
            not_updated: helper.not_updated,
            destroyed: helper.destroyed,
            not_destroyed: helper.not_destroyed,
            next_invocation: helper.data.unwrap_invocation(),
        })
    }
}

impl<'y, T, U> SetObjectHelper<'y, T, U>
where
    T: for<'x> Store<'x> + 'static,
    U: SetObjectData<T>,
{
    pub fn lock(&mut self, collection: Collection) {
        self.lock = self.store.lock_account(self.account_id, collection).into();
    }
}

impl DefaultCreateItem {
    pub fn new(id: JMAPId) -> Self {
        Self { id }
    }
}

impl From<DefaultCreateItem> for JSONValue {
    fn from(ci_result: DefaultCreateItem) -> Self {
        let mut result: HashMap<String, JSONValue> = HashMap::new();
        result.insert("id".to_string(), ci_result.id.to_jmap_string().into());
        result.into()
    }
}

impl CreateItemResult for DefaultCreateItem {
    fn get_id(&self) -> JMAPId {
        self.id
    }
}

impl From<DefaultUpdateItem> for JSONValue {
    fn from(_: DefaultUpdateItem) -> Self {
        JSONValue::Null
    }
}

impl UpdateItemResult for DefaultUpdateItem {}

impl From<SetResult> for JSONValue {
    fn from(set_result: SetResult) -> Self {
        let mut result = HashMap::with_capacity(9);
        result.insert(
            "accountId".to_string(),
            (set_result.account_id as JMAPId).to_jmap_string().into(),
        );
        result.insert("created".to_string(), set_result.created.into());
        result.insert("notCreated".to_string(), set_result.not_created.into());

        result.insert("updated".to_string(), set_result.updated.into());
        result.insert("notUpdated".to_string(), set_result.not_updated.into());

        result.insert("destroyed".to_string(), set_result.destroyed.into());
        result.insert("notDestroyed".to_string(), set_result.not_destroyed.into());

        result.insert("newState".to_string(), set_result.new_state.into());
        result.insert("oldState".to_string(), set_result.old_state.into());
        result.into()
    }
}

impl<'y, T, U> SetObjectHelper<'y, T, U>
where
    T: for<'x> Store<'x> + 'static,
    U: SetObjectData<T>,
{
    pub fn resolve_reference(&self, id: &str) -> crate::Result<JMAPId>
    where
        Self: Sized,
    {
        if !id.starts_with('#') {
            JMAPId::from_jmap_string(id)
                .ok_or_else(|| MethodError::InvalidArguments(format!("Invalid JMAP Id: {}", id)))
        } else {
            let id_ref = id.get(1..).ok_or_else(|| {
                MethodError::InvalidArguments(format!("Invalid reference to JMAP Id: {}", id))
            })?;

            if let Some(created_id) = self.created.get(id_ref) {
                let created_id = created_id
                    .to_object()
                    .unwrap()
                    .get("id")
                    .unwrap()
                    .to_string()
                    .unwrap();
                JMAPId::from_jmap_string(created_id).ok_or_else(|| {
                    MethodError::InvalidArguments(format!(
                        "Invalid referenced JMAP Id: {} ({})",
                        id_ref, created_id
                    ))
                })
            } else {
                Err(MethodError::InvalidArguments(format!(
                    "Reference '{}' not found in createdIds.",
                    id_ref
                )))
            }
        }
    }
}
