use jmap_mail::JMAPMailLocalStore;
use jmap_store::{json::JSONValue, JMAPGet};

pub fn test_jmap_mail_thread<T>(mail_store: T)
where
    T: for<'x> JMAPMailLocalStore<'x>,
{
    let mut expected_result = vec![JSONValue::Null; 5];
    let mut thread_id = 0;

    for num in [5, 3, 1, 2, 4] {
        let mut result = mail_store
            .mail_import_blob(
                0,
                format!("Subject: test\nReferences: <1234>\n\n{}", num).as_bytes(),
                vec![],
                vec![],
                Some(10000i64 + num as i64),
            )
            .unwrap()
            .unwrap_object()
            .unwrap();
        thread_id = result.remove("threadId").unwrap().to_jmap_id().unwrap();
        expected_result[num - 1] = result.remove("id").unwrap();
    }

    assert_eq!(
        mail_store
            .thread_get(JMAPGet {
                account_id: 0,
                ids: Some(vec![thread_id]),
                properties: None,
            })
            .unwrap()
            .list
            .unwrap_array()
            .unwrap()
            .pop()
            .unwrap()
            .unwrap_object()
            .unwrap()
            .remove("emailIds")
            .unwrap()
            .unwrap_array()
            .unwrap(),
        expected_result
    );
}
