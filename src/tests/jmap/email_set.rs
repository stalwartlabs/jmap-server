use actix_web::web;
use jmap_client::{client::Client, mailbox::Role};
use store::Store;

use crate::JMAPServer;

pub async fn test<T>(server: web::Data<JMAPServer<T>>, client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
    let mailbox = client
        .mailbox_create("Cocomiel", None::<String>, Role::Inbox)
        .await
        .unwrap();
    println!(
        "Created mailbox {:?}",
        client.mailbox_get(mailbox.id(), None).await.unwrap()
    );
}

/*

        if file_name.extension().map_or(true, |e| e != "json") {
            continue;
        }

        println!("{}", file_name.display());

        let mut emails: Vec<Email> =
            serde_json::from_slice(&fs::read(&file_name).unwrap()).unwrap();
        assert!(emails.len() == 1);
        let sorted_email =
            serde_json::to_string_pretty(&emails.pop().unwrap().into_test()).unwrap();

        println!("{}", sorted_email);
        fs::write(&file_name, &sorted_email).unwrap();
        continue;

*/
