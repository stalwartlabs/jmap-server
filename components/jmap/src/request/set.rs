use std::collections::HashMap;

use store::roaring::RoaringBitmap;
use store::{AccountId, DocumentId, JMAPId, JMAPIdPrefix};

use crate::error::method::MethodError;
use crate::error::set::SetErrorType;
use crate::id::state::JMAPState;
use crate::id::JMAPIdSerialize;
use crate::protocol::json::JSONValue;
use crate::protocol::json_pointer::JSONPointer;
use crate::protocol::response::Response;

#[derive(Debug, Clone)]
pub struct SetRequest {
    pub account_id: AccountId,
    pub if_in_state: Option<JMAPState>,
    pub create: Vec<(String, JSONValue)>,
    pub update: HashMap<String, JSONValue>,
    pub destroy: Vec<JSONValue>,
    pub arguments: HashMap<String, JSONValue>,
}

pub trait PropertyParser: Sized {
    fn parse_property(_: &str) -> Option<Self>;
}

pub trait SetObject: Sized {
    type Property: PropertyParser;

    fn new(document_id: DocumentId) -> Self;
    fn set_field(
        &mut self,
        field: Self::Property,
        value: JSONValue,
    ) -> crate::Result<Result<(), JSONValue>>;
    fn patch_field(
        &mut self,
        field: Self::Property,
        property: String,
        value: JSONValue,
    ) -> crate::Result<Result<(), JSONValue>>;
}

pub type CreateResults = (HashMap<String, JSONValue>, HashMap<String, JSONValue>);
pub type DestroyResults = (Vec<JSONValue>, HashMap<String, JSONValue>);

impl SetRequest {
    fn map_id_references(
        child_id: &str,
        property: &mut JSONValue,
        response: &Response,
        mut graph: Option<&mut HashMap<String, Vec<String>>>,
    ) {
        match property {
            JSONValue::String(id_ref) if id_ref.starts_with('#') => {
                if let Some(parent_id) = id_ref.get(1..) {
                    if let Some(id) = response.created_ids.get(parent_id) {
                        *id_ref = id.to_string();
                    } else if let Some(graph) = graph.as_mut() {
                        graph
                            .entry(child_id.to_string())
                            .or_insert_with(Vec::new)
                            .push(parent_id.to_string());
                    }
                }
            }
            JSONValue::Array(array) => {
                for array_item in array {
                    if let JSONValue::String(id_ref) = array_item {
                        if id_ref.starts_with('#') {
                            if let Some(parent_id) = id_ref.get(1..) {
                                if let Some(id) = response.created_ids.get(parent_id) {
                                    *id_ref = id.to_string();
                                } else if let Some(graph) = graph.as_mut() {
                                    graph
                                        .entry(child_id.to_string())
                                        .or_insert_with(Vec::new)
                                        .push(parent_id.to_string());
                                }
                            }
                        }
                    }
                }
            }
            JSONValue::Object(object) => {
                let mut rename_keys = HashMap::with_capacity(object.len());
                for key in object.keys() {
                    if key.starts_with('#') {
                        if let Some(parent_id) = key.get(1..) {
                            if let Some(id) = response.created_ids.get(parent_id) {
                                rename_keys.insert(key.to_string(), id.to_string());
                            } else if let Some(graph) = graph.as_mut() {
                                graph
                                    .entry(child_id.to_string())
                                    .or_insert_with(Vec::new)
                                    .push(parent_id.to_string());
                            }
                        }
                    }
                }
                for (rename_from_key, rename_to_key) in rename_keys {
                    let value = object.remove(&rename_from_key).unwrap();
                    object.insert(rename_to_key, value);
                }
            }
            _ => (),
        }
    }

    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = SetRequest {
            account_id: 1, //TODO
            if_in_state: None,
            create: Vec::with_capacity(0),
            update: HashMap::with_capacity(0),
            destroy: Vec::with_capacity(0),
            arguments: HashMap::new(),
        };

        invocation.parse_arguments(response, |name, value| {
            match name.as_str() {
                "accountId" => request.account_id = value.parse_document_id()?,
                "ifInState" => request.if_in_state = value.parse_jmap_state(true)?,
                "create" => {
                    // Order create objects by reference
                    if let Some(mut objects) = value.unwrap_object() {
                        let mut create = Vec::with_capacity(objects.len());
                        let mut graph = HashMap::with_capacity(objects.len());

                        for (child_id, object) in objects.iter_mut() {
                            if let Some(properties) = object.to_object_mut() {
                                for (property_id, property) in properties {
                                    if property_id.ends_with("Id") || property_id.ends_with("Ids") {
                                        SetRequest::map_id_references(
                                            child_id,
                                            property,
                                            response,
                                            Some(&mut graph),
                                        );
                                    }
                                }
                            }
                        }

                        // Topological sort
                        if !graph.is_empty() {
                            let mut it_stack = Vec::new();
                            let keys = graph.keys().cloned().collect::<Vec<_>>();
                            let mut it = keys.iter();

                            'main: loop {
                                while let Some(from_id) = it.next() {
                                    if let Some(to_ids) = graph.get(from_id) {
                                        it_stack.push((it, from_id));
                                        if it_stack.len() > 1000 {
                                            return Err(MethodError::InvalidArguments(
                                                "Cyclical references are not allowed.".to_string(),
                                            ));
                                        }
                                        it = to_ids.iter();
                                        continue;
                                    } else if let Some(object) = objects.remove(from_id) {
                                        create.push((from_id.to_string(), object));
                                        if objects.is_empty() {
                                            break 'main;
                                        }
                                    }
                                }

                                if let Some((prev_it, from_id)) = it_stack.pop() {
                                    it = prev_it;
                                    if let Some(object) = objects.remove(from_id) {
                                        create.push((from_id.to_string(), object));
                                        if objects.is_empty() {
                                            break 'main;
                                        }
                                    }
                                } else {
                                    break;
                                }
                            }
                        }

                        for (user_id, object) in objects {
                            create.push((user_id, object));
                        }
                        request.create = create;
                    }
                }
                "update" => {
                    if let Some(mut objects) = value.unwrap_object() {
                        for object in objects.values_mut() {
                            if let Some(properties) = object.to_object_mut() {
                                for (property_id, property) in properties {
                                    if property_id.ends_with("Id") || property_id.ends_with("Ids") {
                                        SetRequest::map_id_references(
                                            property_id,
                                            property,
                                            response,
                                            None,
                                        );
                                    }
                                }
                            }
                        }
                        request.update = objects;
                    }
                }
                "destroy" => {
                    if let Some(mut array_items) = value.unwrap_array() {
                        for item in &mut array_items {
                            SetRequest::map_id_references("", item, response, None);
                        }
                        request.destroy = array_items;
                    }
                }
                _ => {
                    request.arguments.insert(name, value);
                }
            }
            Ok(())
        })?;

        Ok(request)
    }

    pub fn parse_create<T, C>(&mut self, mut create_fnc: C) -> crate::Result<CreateResults>
    where
        C: FnMut(T) -> crate::Result<Result<JSONValue, JSONValue>>,
        T: SetObject,
    {
        let mut created = HashMap::with_capacity(self.create.len());
        let mut not_created = HashMap::with_capacity(self.create.len());

        'main: for (create_id, fields) in std::mem::take(&mut self.create) {
            if let Some(fields) = fields.unwrap_object() {
                let mut object = T::new(0);
                for (field, value) in fields {
                    if let Some(field) = T::Property::parse_property(&field) {
                        if let Err(err) = object.set_field(field, value)? {
                            not_created.insert(create_id, err);
                            continue 'main;
                        }
                    } else {
                        not_created.insert(
                            create_id,
                            JSONValue::new_invalid_property(field, "Unsupported property."),
                        );
                        continue 'main;
                    }
                }

                match create_fnc(object)? {
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

        Ok((created, not_created))
    }

    pub fn parse_update<U, T>(
        &mut self,
        document_ids: &RoaringBitmap,
        mut update_fnc: U,
    ) -> crate::Result<CreateResults>
    where
        U: FnMut(T) -> crate::Result<Result<JSONValue, JSONValue>>,
        T: SetObject,
    {
        let mut updated = HashMap::with_capacity(self.create.len());
        let mut not_updated = HashMap::with_capacity(self.create.len());

        'main: for (jmap_id_str, fields) in std::mem::take(&mut self.update) {
            let (jmap_id, fields) = if let (Some(jmap_id), Some(fields)) = (
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
            } else if self
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

            let mut object = T::new(document_id);

            for (field, value) in fields {
                match JSONPointer::parse(&field).unwrap_or(JSONPointer::Root) {
                    JSONPointer::String(field) => {
                        if let Some(field) = T::Property::parse_property(&field) {
                            if let Err(err) = object.set_field(field, value)? {
                                not_updated.insert(jmap_id_str, err);
                                continue 'main;
                            }
                        } else {
                            not_updated.insert(
                                jmap_id_str,
                                JSONValue::new_invalid_property(field, "Unsupported property."),
                            );
                            continue 'main;
                        }
                    }

                    JSONPointer::Path(mut path) if path.len() == 2 => {
                        if let (JSONPointer::String(property), JSONPointer::String(field)) =
                            (path.pop().unwrap(), path.pop().unwrap())
                        {
                            if let Some(field) = T::Property::parse_property(&field) {
                                if let Err(err) = object.patch_field(field, property, value)? {
                                    not_updated.insert(jmap_id_str, err);
                                    continue 'main;
                                }
                            } else {
                                not_updated.insert(
                                    format!("{}/{}", field, property),
                                    JSONValue::new_invalid_property(field, "Unsupported property."),
                                );
                                continue 'main;
                            }
                        } else {
                            not_updated.insert(
                                jmap_id_str,
                                JSONValue::new_invalid_property(field, "Unsupported property."),
                            );
                            continue 'main;
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
                        continue 'main;
                    }
                }
            }

            match update_fnc(object)? {
                Ok(result) => updated.insert(jmap_id_str, result),
                Err(err) => not_updated.insert(jmap_id_str, err),
            };
        }

        Ok((updated, not_updated))
    }

    pub fn parse_destroy<D>(
        &mut self,
        document_ids: &RoaringBitmap,
        mut destroy_fnc: D,
    ) -> crate::Result<DestroyResults>
    where
        D: FnMut(JMAPId) -> crate::Result<Result<(), JSONValue>>,
    {
        let mut destroyed = Vec::with_capacity(self.destroy.len());
        let mut not_destroyed = HashMap::with_capacity(self.destroy.len());

        for destroy_id in std::mem::take(&mut self.destroy) {
            if let Some(jmap_id_str) = destroy_id.to_string() {
                if let Some(jmap_id) = JMAPId::from_jmap_string(jmap_id_str) {
                    let document_id = jmap_id.get_document_id();
                    if document_ids.contains(document_id) {
                        if let Err(err) = destroy_fnc(jmap_id)? {
                            not_destroyed.insert(jmap_id_str.to_string(), err);
                        } else {
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

        Ok((destroyed, not_destroyed))
    }
}
