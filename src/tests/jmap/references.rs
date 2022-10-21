/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use jmap::{error::method::MethodError, jmap_store::get::GetObject, types::jmap::JMAPId};
use jmap_mail::mailbox::schema::Property;
use store::ahash::AHashMap;

use crate::api::{method, request::Request, response::Response};

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
                            "accountId": "b",
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
                            "accountId": "b",
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
                            "accountId": "b",
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
                            "accountId": "b",
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

    for (test_num, mut call) in request.method_calls.into_iter().enumerate() {
        match call.method.prepare_request(&response) {
            Ok(_) => assert!(
                (0..3).contains(&test_num),
                "Unexpected invocation {}",
                test_num
            ),
            Err(err) => {
                assert_eq!(test_num, 3);
                assert!(matches!(err, MethodError::InvalidArguments(_)));
                continue;
            }
        }

        if let method::Request::SetMailbox(request) = call.method {
            if test_num == 0 {
                assert_eq!(
                    request
                        .create
                        .unwrap()
                        .into_iter()
                        .map(|b| b.0)
                        .collect::<Vec<_>>(),
                    ["g", "f", "e", "d", "c", "b", "a"]
                        .iter()
                        .map(|i| i.to_string())
                        .collect::<Vec<_>>()
                );
            } else if test_num == 1 {
                let mut pending_ids = vec!["a1", "b2", "d4", "e5", "f6", "c3", "g7"];

                for (id, _) in request.create.as_ref().unwrap() {
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
                        request
                            .create
                            .as_ref()
                            .unwrap()
                            .iter()
                            .map(|b| b.0.to_string())
                            .collect::<Vec<_>>()
                    );
                }
            } else if test_num == 2 {
                assert_eq!(
                    request
                        .create
                        .unwrap()
                        .into_iter()
                        .map(|b| b.0)
                        .collect::<Vec<_>>(),
                    ["x", "z", "y"]
                        .iter()
                        .map(|i| i.to_string())
                        .collect::<Vec<_>>()
                );
            }
        } else {
            panic!("Expected Set Mailbox Request");
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
                        "accountId": "b",
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
                        "accountId": "b",
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
                "x": "b",
                "y": "c",
                "z": "d"
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
    let mut call = invocations.next().unwrap();
    call.method.prepare_request(&response).unwrap();

    if let method::Request::SetMailbox(request) = call.method {
        let create = request
            .create
            .unwrap()
            .into_iter()
            .map(|(p, v)| (p, v.get_as_id(&Property::ParentId).unwrap().pop().unwrap()))
            .collect::<AHashMap<_, _>>();
        assert_eq!(create.get("a").unwrap(), &JMAPId::new(1));
        assert_eq!(create.get("b").unwrap(), &JMAPId::new(2));
        assert_eq!(create.get("c").unwrap(), &JMAPId::new(3));
    } else {
        panic!("Expected Mailbox Set Request");
    }

    response.created_ids.insert("a".to_string(), JMAPId::new(5));
    response.created_ids.insert("b".to_string(), JMAPId::new(6));
    response.created_ids.insert("c".to_string(), JMAPId::new(7));

    let mut call = invocations.next().unwrap();
    call.method.prepare_request(&response).unwrap();

    if let method::Request::SetMailbox(request) = call.method {
        let create = request
            .create
            .unwrap()
            .into_iter()
            .map(|(p, v)| (p, v.get_as_id(&Property::ParentId).unwrap().pop().unwrap()))
            .collect::<AHashMap<_, _>>();
        assert_eq!(create.get("a1").unwrap(), &JMAPId::new(5));
        assert_eq!(create.get("b2").unwrap(), &JMAPId::new(6));
        assert_eq!(create.get("c3").unwrap(), &JMAPId::new(7));
    } else {
        panic!("Expected Mailbox Set Request");
    }
}
