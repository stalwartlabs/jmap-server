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
        "CREATED! {}",
        serde_json::to_string_pretty(&client.mailbox_get(mailbox.id(), None).await.unwrap())
            .unwrap()
    );

    let mail = client
        .email_import(
            "From: Peperino <peperino@pomoro.com>\nSubject: hello world!\n\nthis is the body"
                .to_string()
                .into_bytes(),
            [mailbox.id()],
            ["$seen"].into(),
            12345.into(),
        )
        .await
        .unwrap();

    println!("GOT! {}", serde_json::to_string_pretty(&mail).unwrap());

    println!(
        "CREATED! {}",
        serde_json::to_string_pretty(&client.email_get(mail.id(), None).await.unwrap()).unwrap()
    );
}
