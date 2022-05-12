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
