use std::{collections::HashMap, fs, path::PathBuf};

use actix_web::web;
use jmap::types::jmap::JMAPId;
use jmap_client::{client::Client, email::query::Filter, mailbox::Role};
use store::Store;

use crate::JMAPServer;

pub async fn test<T>(server: web::Data<JMAPServer<T>>, client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Running SearchSnippet tests...");

    let mailbox_id = client
        .set_default_account_id(JMAPId::new(1).to_string())
        .mailbox_create("JMAP SearchSnippet", None::<String>, Role::None)
        .await
        .unwrap()
        .unwrap_id();

    let mut email_ids = HashMap::new();

    let mut test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_dir.push("src");
    test_dir.push("tests");
    test_dir.push("resources");
    test_dir.push("jmap_mail_snippet");

    // Import test messages
    for test_name in ["html", "subpart", "text_plain", "text_plain_i18n"] {
        let mut file_name = test_dir.clone();
        file_name.push(format!("{}.eml", test_name));
        let email_id = client
            .email_import(
                fs::read(&file_name).unwrap(),
                [&mailbox_id],
                None::<Vec<&str>>,
                None,
            )
            .await
            .unwrap()
            .unwrap_id();
        email_ids.insert(test_name, email_id);
    }

    // Run tests
    let mut request = client.build();
    let result_ref = request
        .query_email()
        .filter(Filter::text("côte"))
        .result_reference();
    request
        .get_search_snippet()
        .filter(Filter::text("côte"))
        .email_ids_ref(result_ref);
    let response = request.send().await.unwrap();
    let coco = response
        .unwrap_method_responses()
        .pop()
        .unwrap()
        .unwrap_get_search_snippet()
        .unwrap();
    println!("{:?}", coco);
}
