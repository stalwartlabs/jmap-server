use actix_web::{web, HttpResponse};
use store::Store;

use super::server::JMAPServer;

#[derive(Debug, serde::Deserialize)]
pub enum CloseAfter {
    #[serde(rename(deserialize = "state"))]
    State,
    #[serde(rename(deserialize = "no"))]
    No,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
pub struct Params {
    types: String,
    closeafter: CloseAfter,
    ping: u32,
}

pub async fn handle_jmap_event_source<T>(
    params: web::Query<Params>,
    _core: web::Data<JMAPServer<T>>,
) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    println!("{:?}", params);
    HttpResponse::Ok().body("")
}
