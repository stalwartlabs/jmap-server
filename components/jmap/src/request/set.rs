use crate::error::method::MethodError;
use crate::id::state::JMAPState;
use crate::protocol::json::JSONValue;
use crate::protocol::response::Response;
use std::collections::HashMap;
use store::AccountId;

#[derive(Debug, Clone)]
pub struct SetRequest {
    pub account_id: AccountId,
    pub if_in_state: Option<JMAPState>,
    pub create: Vec<(String, JSONValue)>,
    pub update: HashMap<String, JSONValue>,
    pub destroy: Vec<JSONValue>,
    pub arguments: HashMap<String, JSONValue>,
    pub tombstone_deletions: bool,
}

impl SetRequest {
    pub fn parse(invocation: JSONValue, response: &Response) -> crate::Result<Self> {
        let mut request = SetRequest {
            account_id: 1, //TODO
            if_in_state: None,
            create: Vec::with_capacity(0),
            update: HashMap::with_capacity(0),
            destroy: Vec::with_capacity(0),
            arguments: HashMap::new(),
            tombstone_deletions: false,
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
                                        property.map_id_references(
                                            child_id,
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
                                        property.map_id_references(property_id, response, None);
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
                            item.map_id_references("", response, None);
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
}

impl JSONValue {
    fn map_id_references(
        &mut self,
        child_id: &str,
        response: &Response,
        mut graph: Option<&mut HashMap<String, Vec<String>>>,
    ) {
        match self {
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
}
