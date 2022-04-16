use actix_web::{web, HttpResponse};
use store::Store;

use super::server::JMAPServer;

pub async fn handle_jmap_event_source<T>(
    _path: web::Path<(String, String, String)>,
    _core: web::Data<JMAPServer<T>>,
) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    HttpResponse::Ok().body("")
}
