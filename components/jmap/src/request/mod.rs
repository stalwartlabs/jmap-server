pub mod changes;
pub mod copy;
pub mod get;
pub mod query;
pub mod query_changes;
pub mod set;

use store::{chrono::DateTime, DocumentId};

use crate::{
    id::{blob::JMAPBlob, jmap::JMAPId},
    protocol::{json::JSONValue, response::Response},
    MethodError,
};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ResultReference {
    #[serde(rename = "resultOf")]
    result_of: String,
    name: Method,
    path: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged)]
pub enum MaybeResultReference<T> {
    Value(T),
    Reference(ResultReference),
}

#[derive(Debug, Clone)]
pub enum MaybeIdReference {
    Value(JMAPId),
    Reference(String),
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum Method {
    #[serde(rename = "Core/echo")]
    Echo,
    #[serde(rename = "Blob/copy")]
    CopyBlob,
    #[serde(rename = "PushSubscription/get")]
    GetPushSubscription,
    #[serde(rename = "PushSubscription/set")]
    SetPushSubscription,
    #[serde(rename = "Mailbox/get")]
    GetMailbox,
    #[serde(rename = "Mailbox/changes")]
    ChangesMailbox,
    #[serde(rename = "Mailbox/query")]
    QueryMailbox,
    #[serde(rename = "Mailbox/queryChanges")]
    QueryChangesMailbox,
    #[serde(rename = "Mailbox/set")]
    SetMailbox,
    #[serde(rename = "Thread/get")]
    GetThread,
    #[serde(rename = "Thread/changes")]
    ChangesThread,
    #[serde(rename = "Email/get")]
    GetEmail,
    #[serde(rename = "Email/changes")]
    ChangesEmail,
    #[serde(rename = "Email/query")]
    QueryEmail,
    #[serde(rename = "Email/queryChanges")]
    QueryChangesEmail,
    #[serde(rename = "Email/set")]
    SetEmail,
    #[serde(rename = "Email/copy")]
    CopyEmail,
    #[serde(rename = "Email/import")]
    ImportEmail,
    #[serde(rename = "Email/parse")]
    ParseEmail,
    #[serde(rename = "SearchSnippet/get")]
    GetSearchSnippet,
    #[serde(rename = "Identity/get")]
    GetIdentity,
    #[serde(rename = "Identity/changes")]
    ChangesIdentity,
    #[serde(rename = "Identity/set")]
    SetIdentity,
    #[serde(rename = "EmailSubmission/get")]
    GetEmailSubmission,
    #[serde(rename = "EmailSubmission/changes")]
    ChangesEmailSubmission,
    #[serde(rename = "EmailSubmission/query")]
    QueryEmailSubmission,
    #[serde(rename = "EmailSubmission/queryChanges")]
    QueryChangesEmailSubmission,
    #[serde(rename = "EmailSubmission/set")]
    SetEmailSubmission,
    #[serde(rename = "VacationResponse/get")]
    GetVacationResponse,
    #[serde(rename = "VacationResponse/set")]
    SetVacationResponse,
    #[serde(rename = "error")]
    Error,
}

struct MaybeIdReferenceVisitor;

impl<'de> serde::de::Visitor<'de> for MaybeIdReferenceVisitor {
    type Value = MaybeIdReference;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a valid JMAP state")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(if !v.starts_with('#') {
            MaybeIdReference::Value(JMAPId::parse(v).ok_or_else(|| {
                serde::de::Error::custom(format!("Failed to parse JMAP id '{}'", v))
            })?)
        } else {
            MaybeIdReference::Reference(
                v.get(1..)
                    .ok_or_else(|| {
                        serde::de::Error::custom(format!("Failed to parse JMAP id '{}'", v))
                    })?
                    .to_string(),
            )
        })
    }
}

impl<'de> serde::Deserialize<'de> for MaybeIdReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(MaybeIdReferenceVisitor)
    }
}

/*
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use store::config::{env_settings::EnvSettings, jmap::JMAPConfig};

    use crate::{
        protocol::{json::JSONValue, request::Request},
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
*/
