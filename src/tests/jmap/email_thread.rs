use actix_web::web;
use jmap::id::JMAPIdSerialize;
use jmap_client::{client::Client, mailbox::Role};
use store::Store;

use crate::{tests::store::utils::StoreCompareWith, JMAPServer};

pub async fn test<T>(server: web::Data<JMAPServer<T>>, client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
    let mailbox_id = client
        .set_default_account_id(1u64.to_jmap_string())
        .mailbox_create("JMAP Get", None::<String>, Role::None)
        .await
        .unwrap()
        .unwrap_id();

    let mut expected_result = vec!["".to_string(); 5];
    let mut thread_id = "".to_string();

    for num in [5, 3, 1, 2, 4] {
        let email = client
            .email_import(
                format!("Subject: test\nReferences: <1234>\n\n{}", num).into_bytes(),
                [&mailbox_id],
                None::<Vec<String>>,
                Some(10000i64 + num as i64),
            )
            .await
            .unwrap();
        thread_id = email.thread_id().to_string();
        expected_result[num - 1] = email.unwrap_id();
    }

    assert_eq!(
        client
            .thread_get(&thread_id)
            .await
            .unwrap()
            .unwrap()
            .email_ids(),
        expected_result
    );

    client.mailbox_destroy(&mailbox_id, true).await.unwrap();

    server.store.assert_is_empty();
}
