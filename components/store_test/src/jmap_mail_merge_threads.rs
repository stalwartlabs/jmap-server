use std::collections::HashSet;

use jmap_mail::{import::JMAPMailImport, MessageField};
use store::{
    query::{JMAPIdMapFnc, JMAPStoreQuery},
    Collection, Comparator, Filter, JMAPIdPrefix, JMAPStore, Store, Tag, ThreadId,
};

pub enum ThreadTest {
    Message,
    MessageWithReplies(Vec<ThreadTest>),
    Root(Vec<ThreadTest>),
}

fn build_message(message: usize, in_reply_to: Option<usize>, thread_num: usize) -> String {
    if let Some(in_reply_to) = in_reply_to {
        format!(
            "Message-ID: <{}>\nReferences: <{}>\nSubject: re: T{}\n\nreply\n",
            message, in_reply_to, thread_num
        )
    } else {
        format!(
            "Message-ID: <{}>\nSubject: T{}\n\nmsg\n",
            message, thread_num
        )
    }
}

fn build_messages(
    three: &ThreadTest,
    messages: &mut Vec<String>,
    total_messages: &mut usize,
    in_reply_to: Option<usize>,
    thread_num: usize,
) -> Vec<usize> {
    let mut messages_per_thread = Vec::new();
    match three {
        ThreadTest::Message => {
            *total_messages += 1;
            messages.push(build_message(*total_messages, in_reply_to, thread_num));
        }
        ThreadTest::MessageWithReplies(replies) => {
            *total_messages += 1;
            messages.push(build_message(*total_messages, in_reply_to, thread_num));
            let in_reply_to = Some(*total_messages);
            for reply in replies {
                build_messages(reply, messages, total_messages, in_reply_to, thread_num);
            }
        }
        ThreadTest::Root(items) => {
            for (thread_num, item) in items.iter().enumerate() {
                let count_start = *total_messages;
                build_messages(item, messages, total_messages, None, thread_num);
                messages_per_thread.push(*total_messages - count_start);
            }
        }
    }
    messages_per_thread
}

pub fn jmap_mail_merge_threads<T>(mail_store: &JMAPStore<T>)
where
    T: for<'x> Store<'x> + 'static,
{
    for (base_test_num, test) in [test_1(), test_2(), test_3()].iter().enumerate() {
        let base_test_num = ((base_test_num * 6) as u32) + 1;
        let mut messages = Vec::new();
        let mut total_messages = 0;
        let mut messages_per_thread =
            build_messages(test, &mut messages, &mut total_messages, None, 0);
        messages_per_thread.sort_unstable();

        for message in &messages {
            mail_store
                .mail_import_blob(
                    base_test_num,
                    message.as_bytes().to_vec(),
                    vec![],
                    vec![],
                    None,
                )
                .unwrap();
        }

        for message in messages.iter().rev() {
            mail_store
                .mail_import_blob(
                    base_test_num + 1,
                    message.as_bytes().to_vec(),
                    vec![],
                    vec![],
                    None,
                )
                .unwrap();
        }

        for chunk in messages.chunks(5) {
            for message in chunk {
                mail_store
                    .mail_import_blob(
                        base_test_num + 2,
                        message.as_bytes().to_vec(),
                        vec![],
                        vec![],
                        None,
                    )
                    .unwrap();
            }
            for message in chunk.iter().rev() {
                mail_store
                    .mail_import_blob(
                        base_test_num + 3,
                        message.as_bytes().to_vec(),
                        vec![],
                        vec![],
                        None,
                    )
                    .unwrap();
            }
        }

        for chunk in messages.chunks(5).rev() {
            for message in chunk {
                mail_store
                    .mail_import_blob(
                        base_test_num + 4,
                        message.as_bytes().to_vec(),
                        vec![],
                        vec![],
                        None,
                    )
                    .unwrap();
            }
            for message in chunk.iter().rev() {
                mail_store
                    .mail_import_blob(
                        base_test_num + 5,
                        message.as_bytes().to_vec(),
                        vec![],
                        vec![],
                        None,
                    )
                    .unwrap();
            }
        }

        for test_num in 0..=5 {
            let message_doc_ids = mail_store
                .query::<JMAPIdMapFnc>(JMAPStoreQuery::new(
                    base_test_num + test_num,
                    Collection::Mail,
                    Filter::None,
                    Comparator::None,
                ))
                .unwrap()
                .collect::<Vec<u64>>();

            assert_eq!(
                message_doc_ids.len(),
                total_messages,
                "test# {}/{}",
                base_test_num,
                test_num
            );

            let mut thread_ids: HashSet<ThreadId> = HashSet::new();

            for message_doc_id in message_doc_ids {
                thread_ids.insert(
                    mail_store
                        .get_document_value(
                            base_test_num + test_num,
                            Collection::Mail,
                            message_doc_id.get_document_id(),
                            MessageField::ThreadId.into(),
                        )
                        .unwrap()
                        .unwrap(),
                );
            }

            assert_eq!(
                thread_ids.len(),
                messages_per_thread.len(),
                "{:?}",
                thread_ids
            );

            let mut messages_per_thread_db = Vec::new();

            for thread_id in thread_ids {
                messages_per_thread_db.push(
                    mail_store
                        .get_tag(
                            base_test_num + test_num,
                            Collection::Mail,
                            MessageField::ThreadId.into(),
                            Tag::Id(thread_id),
                        )
                        .unwrap()
                        .unwrap()
                        .len() as usize,
                );
            }
            messages_per_thread_db.sort_unstable();

            assert_eq!(messages_per_thread_db, messages_per_thread);
        }
    }
}

fn test_1() -> ThreadTest {
    ThreadTest::Root(vec![
        ThreadTest::Message,
        ThreadTest::MessageWithReplies(vec![
            ThreadTest::Message,
            ThreadTest::MessageWithReplies(vec![ThreadTest::Message]),
            ThreadTest::MessageWithReplies(vec![
                ThreadTest::Message,
                ThreadTest::MessageWithReplies(vec![
                    ThreadTest::Message,
                    ThreadTest::Message,
                    ThreadTest::MessageWithReplies(vec![
                        ThreadTest::Message,
                        ThreadTest::MessageWithReplies(vec![
                            ThreadTest::Message,
                            ThreadTest::Message,
                            ThreadTest::Message,
                        ]),
                    ]),
                    ThreadTest::MessageWithReplies(vec![
                        ThreadTest::Message,
                        ThreadTest::MessageWithReplies(vec![
                            ThreadTest::Message,
                            ThreadTest::Message,
                            ThreadTest::Message,
                            ThreadTest::Message,
                            ThreadTest::MessageWithReplies(vec![
                                ThreadTest::Message,
                                ThreadTest::MessageWithReplies(vec![
                                    ThreadTest::Message,
                                    ThreadTest::Message,
                                    ThreadTest::MessageWithReplies(vec![ThreadTest::Message]),
                                ]),
                                ThreadTest::MessageWithReplies(vec![
                                    ThreadTest::Message,
                                    ThreadTest::Message,
                                ]),
                            ]),
                        ]),
                    ]),
                ]),
            ]),
        ]),
    ])
}

fn test_2() -> ThreadTest {
    ThreadTest::Root(vec![
        ThreadTest::MessageWithReplies(vec![
            ThreadTest::Message,
            ThreadTest::Message,
            ThreadTest::Message,
            ThreadTest::MessageWithReplies(vec![
                ThreadTest::MessageWithReplies(vec![
                    ThreadTest::Message,
                    ThreadTest::MessageWithReplies(vec![
                        ThreadTest::MessageWithReplies(vec![
                            ThreadTest::MessageWithReplies(vec![
                                ThreadTest::MessageWithReplies(vec![
                                    ThreadTest::MessageWithReplies(vec![
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                            ]),
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                            ]),
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                            ]),
                                        ]),
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                            ]),
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                        ]),
                                        ThreadTest::Message,
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::Message,
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                            ]),
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                        ]),
                                    ]),
                                    ThreadTest::Message,
                                ]),
                                ThreadTest::Message,
                            ]),
                            ThreadTest::Message,
                        ]),
                        ThreadTest::Message,
                    ]),
                    ThreadTest::MessageWithReplies(vec![
                        ThreadTest::MessageWithReplies(vec![
                            ThreadTest::MessageWithReplies(vec![ThreadTest::MessageWithReplies(
                                vec![
                                    ThreadTest::MessageWithReplies(vec![
                                        ThreadTest::Message,
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                            ]),
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                            ]),
                                            ThreadTest::Message,
                                        ]),
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::Message,
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                            ]),
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                            ]),
                                        ]),
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                            ]),
                                            ThreadTest::Message,
                                        ]),
                                    ]),
                                    ThreadTest::Message,
                                    ThreadTest::Message,
                                    ThreadTest::MessageWithReplies(vec![
                                        ThreadTest::Message,
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::Message,
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                            ]),
                                        ]),
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::Message,
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                            ]),
                                        ]),
                                    ]),
                                ],
                            )]),
                            ThreadTest::Message,
                        ]),
                        ThreadTest::Message,
                        ThreadTest::MessageWithReplies(vec![ThreadTest::MessageWithReplies(vec![
                            ThreadTest::Message,
                            ThreadTest::MessageWithReplies(vec![
                                ThreadTest::MessageWithReplies(vec![
                                    ThreadTest::MessageWithReplies(vec![
                                        ThreadTest::MessageWithReplies(vec![ThreadTest::Message]),
                                    ]),
                                ]),
                                ThreadTest::MessageWithReplies(vec![ThreadTest::Message]),
                                ThreadTest::MessageWithReplies(vec![
                                    ThreadTest::MessageWithReplies(vec![
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                        ]),
                                        ThreadTest::Message,
                                        ThreadTest::Message,
                                        ThreadTest::Message,
                                    ]),
                                    ThreadTest::MessageWithReplies(vec![ThreadTest::Message]),
                                    ThreadTest::Message,
                                    ThreadTest::MessageWithReplies(vec![
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                        ]),
                                    ]),
                                ]),
                                ThreadTest::MessageWithReplies(vec![
                                    ThreadTest::MessageWithReplies(vec![ThreadTest::Message]),
                                ]),
                            ]),
                            ThreadTest::Message,
                            ThreadTest::Message,
                        ])]),
                    ]),
                ]),
                ThreadTest::Message,
                ThreadTest::MessageWithReplies(vec![
                    ThreadTest::MessageWithReplies(vec![
                        ThreadTest::MessageWithReplies(vec![
                            ThreadTest::Message,
                            ThreadTest::Message,
                        ]),
                        ThreadTest::MessageWithReplies(vec![
                            ThreadTest::Message,
                            ThreadTest::MessageWithReplies(vec![
                                ThreadTest::MessageWithReplies(vec![
                                    ThreadTest::MessageWithReplies(vec![ThreadTest::Message]),
                                ]),
                                ThreadTest::MessageWithReplies(vec![
                                    ThreadTest::Message,
                                    ThreadTest::MessageWithReplies(vec![
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                            ]),
                                            ThreadTest::Message,
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                            ]),
                                            ThreadTest::Message,
                                        ]),
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                            ]),
                                        ]),
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::Message,
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                            ]),
                                        ]),
                                    ]),
                                    ThreadTest::Message,
                                ]),
                            ]),
                            ThreadTest::MessageWithReplies(vec![
                                ThreadTest::MessageWithReplies(vec![
                                    ThreadTest::Message,
                                    ThreadTest::MessageWithReplies(vec![
                                        ThreadTest::Message,
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                            ]),
                                        ]),
                                        ThreadTest::Message,
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                        ]),
                                    ]),
                                ]),
                                ThreadTest::MessageWithReplies(vec![
                                    ThreadTest::MessageWithReplies(vec![
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                        ]),
                                    ]),
                                ]),
                                ThreadTest::Message,
                                ThreadTest::Message,
                            ]),
                        ]),
                        ThreadTest::Message,
                        ThreadTest::MessageWithReplies(vec![
                            ThreadTest::MessageWithReplies(vec![ThreadTest::Message]),
                            ThreadTest::MessageWithReplies(vec![
                                ThreadTest::MessageWithReplies(vec![
                                    ThreadTest::Message,
                                    ThreadTest::Message,
                                    ThreadTest::Message,
                                ]),
                                ThreadTest::Message,
                                ThreadTest::Message,
                                ThreadTest::MessageWithReplies(vec![
                                    ThreadTest::MessageWithReplies(vec![
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                            ]),
                                            ThreadTest::Message,
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                            ]),
                                            ThreadTest::Message,
                                        ]),
                                        ThreadTest::Message,
                                        ThreadTest::Message,
                                    ]),
                                    ThreadTest::Message,
                                    ThreadTest::Message,
                                ]),
                            ]),
                            ThreadTest::Message,
                            ThreadTest::Message,
                        ]),
                    ]),
                    ThreadTest::Message,
                    ThreadTest::Message,
                ]),
            ]),
        ]),
        ThreadTest::Message,
        ThreadTest::MessageWithReplies(vec![ThreadTest::Message, ThreadTest::Message]),
    ])
}

fn test_3() -> ThreadTest {
    ThreadTest::Root(vec![
        ThreadTest::MessageWithReplies(vec![ThreadTest::Message, ThreadTest::Message]),
        ThreadTest::Message,
        ThreadTest::MessageWithReplies(vec![
            ThreadTest::MessageWithReplies(vec![
                ThreadTest::MessageWithReplies(vec![
                    ThreadTest::Message,
                    ThreadTest::Message,
                    ThreadTest::Message,
                ]),
                ThreadTest::Message,
                ThreadTest::MessageWithReplies(vec![ThreadTest::Message]),
                ThreadTest::Message,
            ]),
            ThreadTest::Message,
            ThreadTest::Message,
        ]),
        ThreadTest::Message,
        ThreadTest::MessageWithReplies(vec![
            ThreadTest::MessageWithReplies(vec![ThreadTest::Message]),
            ThreadTest::MessageWithReplies(vec![
                ThreadTest::Message,
                ThreadTest::MessageWithReplies(vec![ThreadTest::MessageWithReplies(vec![
                    ThreadTest::Message,
                    ThreadTest::MessageWithReplies(vec![
                        ThreadTest::MessageWithReplies(vec![ThreadTest::MessageWithReplies(vec![
                            ThreadTest::MessageWithReplies(vec![ThreadTest::MessageWithReplies(
                                vec![ThreadTest::MessageWithReplies(vec![
                                    ThreadTest::Message,
                                    ThreadTest::Message,
                                ])],
                            )]),
                            ThreadTest::Message,
                            ThreadTest::Message,
                        ])]),
                        ThreadTest::MessageWithReplies(vec![
                            ThreadTest::Message,
                            ThreadTest::MessageWithReplies(vec![
                                ThreadTest::Message,
                                ThreadTest::MessageWithReplies(vec![ThreadTest::Message]),
                                ThreadTest::MessageWithReplies(vec![
                                    ThreadTest::MessageWithReplies(vec![
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                        ]),
                                        ThreadTest::MessageWithReplies(vec![ThreadTest::Message]),
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                        ]),
                                        ThreadTest::Message,
                                    ]),
                                    ThreadTest::MessageWithReplies(vec![
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                        ]),
                                        ThreadTest::Message,
                                    ]),
                                ]),
                            ]),
                        ]),
                    ]),
                    ThreadTest::Message,
                    ThreadTest::Message,
                ])]),
                ThreadTest::Message,
            ]),
            ThreadTest::MessageWithReplies(vec![
                ThreadTest::Message,
                ThreadTest::MessageWithReplies(vec![ThreadTest::MessageWithReplies(vec![
                    ThreadTest::Message,
                ])]),
                ThreadTest::Message,
            ]),
        ]),
        ThreadTest::MessageWithReplies(vec![
            ThreadTest::MessageWithReplies(vec![ThreadTest::MessageWithReplies(vec![
                ThreadTest::Message,
                ThreadTest::MessageWithReplies(vec![ThreadTest::Message, ThreadTest::Message]),
                ThreadTest::Message,
                ThreadTest::Message,
            ])]),
            ThreadTest::MessageWithReplies(vec![
                ThreadTest::MessageWithReplies(vec![
                    ThreadTest::MessageWithReplies(vec![
                        ThreadTest::Message,
                        ThreadTest::Message,
                        ThreadTest::Message,
                        ThreadTest::Message,
                    ]),
                    ThreadTest::MessageWithReplies(vec![ThreadTest::MessageWithReplies(vec![
                        ThreadTest::MessageWithReplies(vec![
                            ThreadTest::Message,
                            ThreadTest::MessageWithReplies(vec![ThreadTest::MessageWithReplies(
                                vec![
                                    ThreadTest::Message,
                                    ThreadTest::Message,
                                    ThreadTest::MessageWithReplies(vec![
                                        ThreadTest::Message,
                                        ThreadTest::Message,
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                        ]),
                                    ]),
                                    ThreadTest::Message,
                                ],
                            )]),
                            ThreadTest::Message,
                            ThreadTest::MessageWithReplies(vec![ThreadTest::MessageWithReplies(
                                vec![
                                    ThreadTest::Message,
                                    ThreadTest::MessageWithReplies(vec![ThreadTest::Message]),
                                ],
                            )]),
                        ]),
                        ThreadTest::MessageWithReplies(vec![
                            ThreadTest::Message,
                            ThreadTest::Message,
                        ]),
                    ])]),
                    ThreadTest::MessageWithReplies(vec![ThreadTest::Message]),
                    ThreadTest::Message,
                ]),
                ThreadTest::MessageWithReplies(vec![ThreadTest::Message]),
                ThreadTest::Message,
            ]),
        ]),
        ThreadTest::MessageWithReplies(vec![
            ThreadTest::Message,
            ThreadTest::MessageWithReplies(vec![
                ThreadTest::MessageWithReplies(vec![
                    ThreadTest::MessageWithReplies(vec![
                        ThreadTest::MessageWithReplies(vec![
                            ThreadTest::MessageWithReplies(vec![
                                ThreadTest::MessageWithReplies(vec![
                                    ThreadTest::MessageWithReplies(vec![
                                        ThreadTest::MessageWithReplies(vec![
                                            ThreadTest::Message,
                                            ThreadTest::Message,
                                            ThreadTest::MessageWithReplies(vec![
                                                ThreadTest::Message,
                                                ThreadTest::Message,
                                            ]),
                                        ]),
                                        ThreadTest::Message,
                                    ]),
                                    ThreadTest::Message,
                                    ThreadTest::Message,
                                ]),
                                ThreadTest::Message,
                                ThreadTest::Message,
                                ThreadTest::Message,
                            ]),
                            ThreadTest::Message,
                        ]),
                        ThreadTest::Message,
                    ]),
                    ThreadTest::Message,
                ]),
                ThreadTest::MessageWithReplies(vec![
                    ThreadTest::Message,
                    ThreadTest::MessageWithReplies(vec![
                        ThreadTest::Message,
                        ThreadTest::Message,
                        ThreadTest::MessageWithReplies(vec![
                            ThreadTest::Message,
                            ThreadTest::MessageWithReplies(vec![ThreadTest::MessageWithReplies(
                                vec![ThreadTest::Message, ThreadTest::Message],
                            )]),
                            ThreadTest::Message,
                        ]),
                        ThreadTest::Message,
                    ]),
                ]),
            ]),
        ]),
    ])
}

pub fn build_thread_test_messages() -> Vec<String> {
    let mut messages = Vec::new();
    let mut total_messages = 0;
    build_messages(&test_3(), &mut messages, &mut total_messages, None, 0);
    messages
}
