pub mod changes;
pub mod get;
pub mod import;
pub mod parse;
pub mod query;
pub mod query_changes;
pub mod set;

use store::{chrono::DateTime, DocumentId, JMAPId};

use crate::{
    id::blob::BlobId,
    protocol::{json::JSONValue, response::Response},
    MethodError,
};

pub trait JSONArgumentParser: Sized {
    fn parse_argument(argument: JSONValue) -> crate::Result<Self>;
}

impl<T> JSONArgumentParser for Vec<T>
where
    T: JSONArgumentParser,
{
    fn parse_argument(argument: JSONValue) -> crate::Result<Self> {
        if let JSONValue::Array(array) = argument {
            let mut result = Vec::with_capacity(array.len());
            for value in array {
                result.push(T::parse_argument(value)?);
            }
            Ok(result)
        } else {
            Err(MethodError::InvalidArguments("Expected Array.".to_string()))
        }
    }
}

impl JSONArgumentParser for JMAPId {
    fn parse_argument(argument: JSONValue) -> crate::Result<Self> {
        argument
            .to_jmap_id()
            .ok_or_else(|| MethodError::InvalidArguments("Failed to parse JMAP Id.".to_string()))
    }
}

impl JSONArgumentParser for DocumentId {
    fn parse_argument(argument: JSONValue) -> crate::Result<Self> {
        argument
            .to_jmap_id()
            .map(|id| id as DocumentId)
            .ok_or_else(|| MethodError::InvalidArguments("Failed to parse JMAP Id.".to_string()))
    }
}

impl JSONArgumentParser for BlobId {
    fn parse_argument(argument: JSONValue) -> crate::Result<Self> {
        argument
            .parse_blob_id(false)?
            .ok_or_else(|| MethodError::InvalidArguments("Failed to parse Blob Id.".to_string()))
    }
}

impl JSONValue {
    fn eval_result_reference(&self, response: &Response) -> crate::Result<JSONValue> {
        if let JSONValue::Object(obj) = self {
            let result_of = obj
                .get("resultOf")
                .ok_or_else(|| MethodError::InvalidArguments("resultOf key missing.".to_string()))?
                .to_string()
                .ok_or_else(|| {
                    MethodError::InvalidArguments("resultOf key is not a string.".to_string())
                })?;
            let name = obj
                .get("name")
                .ok_or_else(|| MethodError::InvalidArguments("name key missing.".to_string()))?
                .to_string()
                .ok_or_else(|| {
                    MethodError::InvalidArguments("name key is not a string.".to_string())
                })?;
            let path = obj
                .get("path")
                .ok_or_else(|| MethodError::InvalidArguments("path key missing.".to_string()))?
                .to_string()
                .ok_or_else(|| {
                    MethodError::InvalidArguments("path key is not a string.".to_string())
                })?;

            for (method_name, result, call_id) in &response.method_responses {
                if name == method_name && call_id == result_of {
                    return result.eval(path);
                }
            }

            Err(MethodError::InvalidArguments(format!(
                "No methodResponse found with name '{}' and call id '{}'.",
                name, result_of
            )))
        } else {
            Err(MethodError::InvalidArguments(
                "ResultReference is not an object".to_string(),
            ))
        }
    }

    pub fn parse_document_id(self) -> crate::Result<DocumentId> {
        self.to_jmap_id()
            .map(|id| id as DocumentId)
            .ok_or_else(|| MethodError::InvalidArguments("Failed to parse JMAP Id.".to_string()))
    }

    pub fn parse_unsigned_int(self, optional: bool) -> crate::Result<Option<u64>> {
        match self {
            JSONValue::Number(number) => Ok(Some(number.to_unsigned_int())),
            JSONValue::Null if optional => Ok(None),
            _ => Err(MethodError::InvalidArguments(
                "Expected unsigned integer.".to_string(),
            )),
        }
    }

    pub fn parse_int(self, optional: bool) -> crate::Result<Option<i64>> {
        match self {
            JSONValue::Number(number) => Ok(Some(number.to_int())),
            JSONValue::Null if optional => Ok(None),
            _ => Err(MethodError::InvalidArguments(
                "Expected integer.".to_string(),
            )),
        }
    }

    pub fn parse_string(self) -> crate::Result<String> {
        self.unwrap_string()
            .ok_or_else(|| MethodError::InvalidArguments("Expected string.".to_string()))
    }

    pub fn parse_bool(self) -> crate::Result<bool> {
        self.to_bool()
            .ok_or_else(|| MethodError::InvalidArguments("Expected boolean.".to_string()))
    }

    pub fn parse_utc_date(self, optional: bool) -> crate::Result<Option<i64>> {
        match self {
            JSONValue::String(date_time) => Ok(Some(
                DateTime::parse_from_rfc3339(&date_time)
                    .map_err(|_| {
                        MethodError::InvalidArguments(format!(
                            "Failed to parse UTC Date '{}'",
                            date_time
                        ))
                    })?
                    .timestamp(),
            )),
            JSONValue::Null if optional => Ok(None),
            _ => Err(MethodError::InvalidArguments(
                "Expected UTC date.".to_string(),
            )),
        }
    }

    pub fn parse_array_items<T>(self, optional: bool) -> crate::Result<Option<Vec<T>>>
    where
        T: JSONArgumentParser,
    {
        match self {
            JSONValue::Array(items) => {
                if !items.is_empty() {
                    let mut result = Vec::with_capacity(items.len());
                    for item in items {
                        result.push(T::parse_argument(item)?);
                    }
                    Ok(Some(result))
                } else if optional {
                    Ok(None)
                } else {
                    Err(MethodError::InvalidArguments(
                        "Expected array with at least one item.".to_string(),
                    ))
                }
            }
            JSONValue::Null if optional => Ok(None),
            _ => Err(MethodError::InvalidArguments("Expected array.".to_string())),
        }
    }

    fn parse_arguments<T>(self, response: &Response, mut parse_fnc: T) -> crate::Result<()>
    where
        T: FnMut(String, JSONValue) -> crate::Result<()>,
    {
        for (arg_name, arg_value) in self
            .unwrap_object()
            .ok_or_else(|| MethodError::InvalidArguments("Expected object.".to_string()))?
            .into_iter()
        {
            if arg_name.starts_with('#') {
                parse_fnc(
                    arg_name
                        .get(1..)
                        .ok_or_else(|| {
                            MethodError::InvalidArguments(
                                "Failed to parse argument name.".to_string(),
                            )
                        })?
                        .to_string(),
                    arg_value.eval_result_reference(response)?,
                )?;
            } else {
                parse_fnc(arg_name, arg_value)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use store::{config::EnvSettings, JMAPConfig};

    use crate::{
        protocol::{
            invocation::{Invocation, Method},
            json::JSONValue,
            request::Request,
        },
        MethodError,
    };

    use super::Response;

    #[test]
    fn map_sort_created_ids() {
        let request = serde_json::from_slice::<Request>(
            br##"{
                    "using": [
                        "urn:ietf:params:jmap:core",
                        "urn:ietf:params:jmap:mail"
                    ],
                    "methodCalls": [
                        [
                            "Mailbox/set",
                            {
                                "accountId": "i01",
                                "create": {
                                    "a": {
                                        "name": "Folder a",
                                        "parentId": "#b"
                                    },
                                    "b": {
                                        "name": "Folder b",
                                        "parentId": "#c"
                                    },
                                    "c": {
                                        "name": "Folder c",
                                        "parentId": "#d"
                                    },
                                    "d": {
                                        "name": "Folder d",
                                        "parentId": "#e"
                                    },
                                    "e": {
                                        "name": "Folder e",
                                        "parentId": "#f"
                                    },
                                    "f": {
                                        "name": "Folder f",
                                        "parentId": "#g"
                                    },
                                    "g": {
                                        "name": "Folder g",
                                        "parentId": null
                                    }
                                }
                            },
                            "fulltree"
                        ],
                        [
                            "Mailbox/set",
                            {
                                "accountId": "i01",
                                "create": {
                                    "a1": {
                                        "name": "Folder a1",
                                        "parentId": null
                                    },
                                    "b2": {
                                        "name": "Folder b2",
                                        "parentId": "#a1"
                                    },
                                    "c3": {
                                        "name": "Folder c3",
                                        "parentId": "#a1"
                                    },
                                    "d4": {
                                        "name": "Folder d4",
                                        "parentId": "#b2"
                                    },
                                    "e5": {
                                        "name": "Folder e5",
                                        "parentId": "#b2"
                                    },
                                    "f6": {
                                        "name": "Folder f6",
                                        "parentId": "#d4"
                                    },
                                    "g7": {
                                        "name": "Folder g7",
                                        "parentId": "#e5"
                                    }
                                }
                            },
                            "fulltree2"
                        ],
                        [
                            "Mailbox/set",
                            {
                                "accountId": "i01",
                                "create": {
                                    "z": {
                                        "name": "Folder Z",
                                        "parentId": "#x"
                                    },
                                    "y": {
                                        "name": null
                                    },
                                    "x": {
                                        "name": "Folder X"
                                    }
                                }
                            },
                            "xyz"
                        ],
                        [
                            "Mailbox/set",
                            {
                                "accountId": "i01",
                                "create": {
                                    "a": {
                                        "name": "Folder a",
                                        "parentId": "#b"
                                    },
                                    "b": {
                                        "name": "Folder b",
                                        "parentId": "#c"
                                    },
                                    "c": {
                                        "name": "Folder c",
                                        "parentId": "#d"
                                    },
                                    "d": {
                                        "name": "Folder d",
                                        "parentId": "#a"
                                    }
                                }
                            },
                            "circular"
                        ]
                    ]
                }"##,
        )
        .unwrap();

        let response = Response::new(
            1234,
            request.created_ids.unwrap_or_default(),
            request.method_calls.len(),
        );
        let config = JMAPConfig::from(&EnvSettings {
            args: HashMap::new(),
        });

        for (test_num, (name, arguments, _)) in request.method_calls.into_iter().enumerate() {
            match Invocation::parse(&name, arguments, &response, &config) {
                Ok(invocation) => {
                    assert!((0..3).contains(&test_num), "Unexpected invocation");

                    if let Method::Set(set) = invocation.call {
                        if test_num == 0 {
                            assert_eq!(
                                set.create.into_iter().map(|b| b.0).collect::<Vec<_>>(),
                                ["g", "f", "e", "d", "c", "b", "a"]
                                    .iter()
                                    .map(|i| i.to_string())
                                    .collect::<Vec<_>>()
                            );
                        } else if test_num == 1 {
                            let mut pending_ids = vec!["a1", "b2", "d4", "e5", "f6", "c3", "g7"];

                            for (id, _) in &set.create {
                                match id.as_str() {
                                    "a1" => (),
                                    "b2" | "c3" => assert!(!pending_ids.contains(&"a1")),
                                    "d4" | "e5" => assert!(!pending_ids.contains(&"b2")),
                                    "f6" => assert!(!pending_ids.contains(&"d4")),
                                    "g7" => assert!(!pending_ids.contains(&"e5")),
                                    _ => panic!("Unexpected ID"),
                                }
                                pending_ids.retain(|i| i != id);
                            }

                            if !pending_ids.is_empty() {
                                panic!(
                                    "Unexpected order: {:?}",
                                    all_ids = set
                                        .create
                                        .iter()
                                        .map(|b| b.0.to_string())
                                        .collect::<Vec<_>>()
                                );
                            }
                        } else if test_num == 2 {
                            assert_eq!(
                                set.create.into_iter().map(|b| b.0).collect::<Vec<_>>(),
                                ["x", "z", "y"]
                                    .iter()
                                    .map(|i| i.to_string())
                                    .collect::<Vec<_>>()
                            );
                        }
                    } else {
                        panic!("Expected SetRequest");
                    };
                }
                Err(err) => {
                    assert_eq!(test_num, 3);
                    assert!(matches!(err, MethodError::InvalidArguments(_)));
                }
            }
        }

        let request = serde_json::from_slice::<Request>(
            br##"{
                "using": [
                    "urn:ietf:params:jmap:core",
                    "urn:ietf:params:jmap:mail"
                ],
                "methodCalls": [
                    [
                        "Mailbox/set",
                        {
                            "accountId": "i01",
                            "create": {
                                "a": {
                                    "name": "a",
                                    "parentId": "#x"
                                },
                                "b": {
                                    "name": "b",
                                    "parentId": "#y"
                                },
                                "c": {
                                    "name": "c",
                                    "parentId": "#z"
                                }
                            }
                        },
                        "ref1"
                    ],
                    [
                        "Mailbox/set",
                        {
                            "accountId": "i01",
                            "create": {
                                "a1": {
                                    "name": "a1",
                                    "parentId": "#a"
                                },
                                "b2": {
                                    "name": "b2",
                                    "parentId": "#b"
                                },
                                "c3": {
                                    "name": "c3",
                                    "parentId": "#c"
                                }
                            }
                        },
                        "red2"
                    ]
                ],
                "createdIds": {
                    "x": "i01",
                    "y": "i02",
                    "z": "i03"
                }
            }"##,
        )
        .unwrap();

        let mut response = Response::new(
            1234,
            request.created_ids.unwrap_or_default(),
            request.method_calls.len(),
        );

        let mut invocations = request.method_calls.into_iter();
        let (name, arguments, _) = invocations.next().unwrap();
        let invocation = Invocation::parse(&name, arguments, &response, &config).unwrap();
        if let Method::Set(set) = invocation.call {
            let create: JSONValue = set.create.into_iter().collect::<HashMap<_, _>>().into();
            assert_eq!(create.eval_unwrap_string("/a/parentId"), "i01");
            assert_eq!(create.eval_unwrap_string("/b/parentId"), "i02");
            assert_eq!(create.eval_unwrap_string("/c/parentId"), "i03");
        } else {
            panic!("Expected SetRequest");
        };

        response.push_response(
            "test".to_string(),
            "test".to_string(),
            serde_json::from_slice::<JSONValue>(
                br##"{
                "created": {
                    "a": {
                        "id": "i05"
                    },
                    "b": {
                        "id": "i06"
                    },
                    "c": {
                        "id": "i07"
                    }
                }
            }"##,
            )
            .unwrap(),
            true,
        );

        let (name, arguments, _) = invocations.next().unwrap();
        let invocation = Invocation::parse(&name, arguments, &response, &config).unwrap();
        if let Method::Set(set) = invocation.call {
            let create: JSONValue = set.create.into_iter().collect::<HashMap<_, _>>().into();
            assert_eq!(create.eval_unwrap_string("/a1/parentId"), "i05");
            assert_eq!(create.eval_unwrap_string("/b2/parentId"), "i06");
            assert_eq!(create.eval_unwrap_string("/c3/parentId"), "i07");
        } else {
            panic!("Expected SetRequest");
        };
    }
}
