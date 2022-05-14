use actix_web::web;
use jmap_client::client::Client;
use store::Store;

use crate::JMAPServer;

pub async fn test<T>(server: web::Data<JMAPServer<T>>, client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
}
