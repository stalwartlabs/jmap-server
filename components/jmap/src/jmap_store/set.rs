use std::collections::HashMap;

use crate::id::JMAPIdSerialize;
use crate::{
    error::{method::MethodError, set::SetErrorType},
    protocol::{json::JSONValue, json_pointer::JSONPointer},
    request::set::SetRequest,
};
use store::JMAPIdPrefix;
use store::{batch::WriteBatch, roaring::RoaringBitmap, Collection, JMAPId, JMAPStore, Store};

use super::changes::JMAPChanges;

pub trait PropertyParser: Sized {
    fn parse_property(property: &str) -> Option<Self>;
}

pub trait SetObjectHelper<T>: Sized
where
    T: for<'x> Store<'x> + 'static,
{
    fn init(store: &JMAPStore<T>, request: &SetRequest) -> crate::Result<Self>;
}

pub trait SetObject<T>: Sized
where
    T: for<'x> Store<'x> + 'static,
{
    type Property: PropertyParser;
    type Helper: SetObjectHelper<T>;

    fn create(
        store: &JMAPStore<T>,
        fields: &mut HashMap<String, JSONValue>,
        helper: &Self::Helper,
    ) -> Self;
    fn update(
        store: &JMAPStore<T>,
        fields: &mut HashMap<String, JSONValue>,
        helper: &Self::Helper,
        jmap_id: JMAPId,
    ) -> Self;
    fn set_field(
        &mut self,
        store: &JMAPStore<T>,
        helper: &Self::Helper,
        field: Self::Property,
        value: JSONValue,
    ) -> Result<(), JSONValue>;
    fn patch_field(
        &mut self,
        store: &JMAPStore<T>,
        helper: &Self::Helper,
        field: Self::Property,
        property: String,
        value: JSONValue,
    ) -> Result<(), JSONValue>;
    fn write(
        self,
        store: &JMAPStore<T>,
        helper: &Self::Helper,
        batch: &mut WriteBatch,
    ) -> crate::Result<Result<JSONValue, JSONValue>>;
    fn delete(
        store: &JMAPStore<T>,
        helper: &Self::Helper,
        batch: &mut WriteBatch,
        jmap_id: JMAPId,
    ) -> crate::Result<Result<(), JSONValue>>;
    fn is_empty(&self) -> bool;
    fn collection() -> Collection;
}

pub trait JMAPSet<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn set<U>(&self, request: SetRequest) -> crate::Result<JSONValue>
    where
        U: SetObject<T>;
}

impl<T> JMAPSet<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn set<U>(&self, request: SetRequest) -> crate::Result<JSONValue>
    where
        U: SetObject<T>,
    {
        let collection = U::collection();
        let helper = U::Helper::init(self, &request)?;

        let old_state = self.get_state(request.account_id, collection)?;
        if let Some(if_in_state) = request.if_in_state {
            if old_state != if_in_state {
                return Err(MethodError::StateMismatch);
            }
        }

        let mut changes = WriteBatch::new(request.account_id, self.config.is_in_cluster);
        let document_ids = self
            .get_document_ids(request.account_id, collection)?
            .unwrap_or_else(RoaringBitmap::new);

        let mut created = HashMap::with_capacity(request.create.len());
        let mut not_created = HashMap::with_capacity(request.create.len());

        'create: for (create_id, fields) in request.create {
            if let Some(mut fields) = fields.unwrap_object() {
                let mut object = U::create(self, &mut fields, &helper);
                for (field, value) in fields {
                    if let Some(field) = U::Property::parse_property(&field) {
                        if let Err(err) = object.set_field(self, &helper, field, value) {
                            not_created.insert(create_id, err);
                            continue 'create;
                        }
                    } else {
                        not_created.insert(
                            create_id,
                            JSONValue::new_invalid_property(field, "Unsupported property."),
                        );
                        continue 'create;
                    }
                }

                match object.write(self, &helper, &mut changes)? {
                    Ok(result) => created.insert(create_id, result),
                    Err(err) => not_created.insert(create_id, err),
                };
            } else {
                not_created.insert(
                    create_id,
                    JSONValue::new_error(
                        SetErrorType::InvalidProperties,
                        "Failed to parse request, expected object.",
                    ),
                );
            };
        }

        let mut updated = HashMap::with_capacity(request.update.len());
        let mut not_updated = HashMap::with_capacity(request.update.len());

        'update: for (jmap_id_str, fields) in request.update {
            let (jmap_id, mut fields) = if let (Some(jmap_id), Some(fields)) = (
                JMAPId::from_jmap_string(&jmap_id_str),
                fields.unwrap_object(),
            ) {
                (jmap_id, fields)
            } else {
                not_updated.insert(
                    jmap_id_str,
                    JSONValue::new_error(
                        SetErrorType::InvalidProperties,
                        "Failed to parse request.",
                    ),
                );
                continue;
            };

            let document_id = jmap_id.get_document_id();
            if !document_ids.contains(document_id) {
                not_updated.insert(
                    jmap_id_str,
                    JSONValue::new_error(SetErrorType::NotFound, "ID not found."),
                );
                continue;
            } else if request
                .destroy
                .iter()
                .any(|x| x.to_string().map(|v| v == jmap_id_str).unwrap_or(false))
            {
                not_updated.insert(
                    jmap_id_str,
                    JSONValue::new_error(SetErrorType::WillDestroy, "ID will be destroyed."),
                );
                continue;
            }

            let mut object = U::update(self, &mut fields, &helper, jmap_id);

            for (field, value) in fields {
                match JSONPointer::parse(&field).unwrap_or(JSONPointer::Root) {
                    JSONPointer::String(field) => {
                        if let Some(field) = U::Property::parse_property(&field) {
                            if let Err(err) = object.set_field(self, &helper, field, value) {
                                not_updated.insert(jmap_id_str, err);
                                continue 'update;
                            }
                        } else {
                            not_updated.insert(
                                jmap_id_str,
                                JSONValue::new_invalid_property(field, "Unsupported property."),
                            );
                            continue 'update;
                        }
                    }

                    JSONPointer::Path(mut path) if path.len() == 2 => {
                        if let (JSONPointer::String(property), JSONPointer::String(field)) =
                            (path.pop().unwrap(), path.pop().unwrap())
                        {
                            if let Some(field) = U::Property::parse_property(&field) {
                                if let Err(err) =
                                    object.patch_field(self, &helper, field, property, value)
                                {
                                    not_updated.insert(jmap_id_str, err);
                                    continue 'update;
                                }
                            } else {
                                not_updated.insert(
                                    format!("{}/{}", field, property),
                                    JSONValue::new_invalid_property(field, "Unsupported property."),
                                );
                                continue 'update;
                            }
                        } else {
                            not_updated.insert(
                                jmap_id_str,
                                JSONValue::new_invalid_property(field, "Unsupported property."),
                            );
                            continue 'update;
                        }
                    }
                    _ => {
                        not_updated.insert(
                            jmap_id_str,
                            JSONValue::new_invalid_property(
                                field.to_string(),
                                "Unsupported property.",
                            ),
                        );
                        continue 'update;
                    }
                }
            }

            if !object.is_empty() {
                match object.write(self, &helper, &mut changes)? {
                    Ok(result) => {
                        changes.log_update(collection, jmap_id);
                        updated.insert(jmap_id_str, result)
                    }
                    Err(err) => not_updated.insert(jmap_id_str, err),
                };
            } else {
                not_updated.insert(
                    jmap_id_str,
                    JSONValue::new_error(
                        SetErrorType::InvalidPatch,
                        "No changes found in request.",
                    ),
                );
            }
        }

        let mut destroyed = Vec::with_capacity(request.destroy.len());
        let mut not_destroyed = HashMap::with_capacity(request.destroy.len());

        for destroy_id in request.destroy {
            if let Some(jmap_id_str) = destroy_id.to_string() {
                if let Some(jmap_id) = JMAPId::from_jmap_string(jmap_id_str) {
                    let document_id = jmap_id.get_document_id();
                    if document_ids.contains(document_id) {
                        if let Err(err) = U::delete(self, &helper, &mut changes, jmap_id)? {
                            not_destroyed.insert(jmap_id_str.to_string(), err);
                        } else {
                            changes.delete_document(collection, jmap_id.get_document_id());
                            changes.log_delete(collection, jmap_id);
                            destroyed.push(destroy_id);
                        }
                    } else {
                        not_destroyed.insert(
                            jmap_id_str.to_string(),
                            JSONValue::new_error(SetErrorType::NotFound, "ID not found."),
                        );
                    }
                } else {
                    not_destroyed.insert(
                        jmap_id_str.to_string(),
                        JSONValue::new_error(
                            SetErrorType::InvalidProperties,
                            "Failed to parse Id.",
                        ),
                    );
                }
            }
        }

        let mut response = HashMap::new();
        response.insert(
            "accountId".to_string(),
            (request.account_id as JMAPId).to_jmap_string().into(),
        );
        response.insert("created".to_string(), created.into());
        response.insert("notCreated".to_string(), not_created.into());

        response.insert("updated".to_string(), updated.into());
        response.insert("notUpdated".to_string(), not_updated.into());

        response.insert("destroyed".to_string(), destroyed.into());
        response.insert("notDestroyed".to_string(), not_destroyed.into());

        response.insert(
            "newState".to_string(),
            if !changes.is_empty() {
                self.write(changes)?;
                self.get_state(request.account_id, collection)?
            } else {
                old_state.clone()
            }
            .into(),
        );
        response.insert("oldState".to_string(), old_state.into());

        Ok(response.into())
    }
}
