use std::collections::HashMap;

use serde::Serialize;

use crate::{
    error::method::MethodError,
    id::jmap::JMAPId,
    jmap_store::set::SetObject,
    request::{set::SetRequest, Method},
};

#[derive(Debug, serde::Serialize)]
pub struct Response<T: serde::Serialize> {
    #[serde(rename = "methodResponses")]
    pub method_responses: Vec<(Method, T, String)>,

    #[serde(rename = "sessionState")]
    #[serde(serialize_with = "serialize_hex")]
    pub session_state: u64,

    #[serde(rename(deserialize = "createdIds"))]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub created_ids: HashMap<String, JMAPId>,
}

impl<U: serde::Serialize> Response<U> {
    pub fn new(session_state: u64, created_ids: HashMap<String, JMAPId>, capacity: usize) -> Self {
        Response {
            session_state,
            created_ids,
            method_responses: Vec::with_capacity(capacity),
        }
    }

    pub fn push_response(&mut self, method: Method, call_id: String, response: U) {
        self.method_responses.push((method, response, call_id));
    }

    pub fn push_created_id(&mut self, create_id: String, id: JMAPId) {
        self.created_ids.insert(create_id, id);
    }

    pub fn push_error(&mut self, call_id: String, error: U) {
        self.method_responses.push((Method::Error, error, call_id));
    }

    pub fn sort_map_references<O>(&mut self, request: &mut SetRequest<O>) -> crate::Result<()>
    where
        O: SetObject,
    {
        if let Some(mut objects) = request.create.take() {
            let mut create = Vec::with_capacity(objects.len());
            let mut graph = HashMap::with_capacity(objects.len());

            for (child_id, object) in objects.iter_mut() {
                object.map_references(|parent_id| {
                    if let Some(id) = self.created_ids.get(parent_id) {
                        Some(*id)
                    } else {
                        graph
                            .entry(child_id.to_string())
                            .or_insert_with(Vec::new)
                            .push(parent_id.to_string());
                        None
                    }
                });
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
                        } else if let Some(object_pos) =
                            objects.iter().position(|(id, _)| id == from_id)
                        {
                            create.push((from_id.to_string(), objects.swap_remove(object_pos).1));
                            if objects.is_empty() {
                                break 'main;
                            }
                        }
                    }

                    if let Some((prev_it, from_id)) = it_stack.pop() {
                        it = prev_it;
                        if let Some(object_pos) = objects.iter().position(|(id, _)| id == from_id) {
                            create.push((from_id.to_string(), objects.swap_remove(object_pos).1));
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

            request.create = create.into();
        }

        Ok(())
    }
}

pub fn serialize_hex<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    format!("{:x}", value).serialize(serializer)
}
