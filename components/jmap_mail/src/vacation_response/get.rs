use std::time::SystemTime;

use jmap::jmap_store::get::{GetHelper, GetObject, IdMapper, SharedDocsFnc};
use jmap::orm::{serialize::JMAPOrm, TinyORM};
use jmap::request::get::{GetRequest, GetResponse};
use jmap::types::jmap::JMAPId;
use store::ahash::AHashSet;

use mail_builder::headers::address::Address;
use mail_builder::MessageBuilder;
use store::core::collection::Collection;
use store::core::document::Document;
use store::core::error::StoreError;
use store::core::vec_map::VecMap;
use store::write::batch::WriteBatch;
use store::Store;
use store::{AccountId, JMAPStore};

use super::schema::{Property, VacationResponse, Value};

impl GetObject for VacationResponse {
    type GetArguments = ();

    fn default_properties() -> Vec<Self::Property> {
        vec![
            Property::Id,
            Property::IsEnabled,
            Property::FromDate,
            Property::ToDate,
            Property::Subject,
            Property::TextBody,
            Property::HtmlBody,
        ]
    }

    fn get_as_id(&self, property: &Self::Property) -> Option<Vec<JMAPId>> {
        match self.properties.get(property)? {
            Value::Id { value } => Some(vec![*value]),
            _ => None,
        }
    }
}

pub struct VacationMessage {
    pub from: String,
    pub to: String,
    pub message: Vec<u8>,
}

pub trait JMAPGetVacationResponse<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn vacation_response_get(
        &self,
        request: GetRequest<VacationResponse>,
    ) -> jmap::Result<GetResponse<VacationResponse>>;

    fn build_vacation_response(
        &self,
        account_id: AccountId,
        from_name: Option<&str>,
        from_addr: &str,
        to: &str,
    ) -> store::Result<Option<VacationMessage>>;
}

impl<T> JMAPGetVacationResponse<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn vacation_response_get(
        &self,
        request: GetRequest<VacationResponse>,
    ) -> jmap::Result<GetResponse<VacationResponse>> {
        let helper = GetHelper::new(self, request, None::<IdMapper>, None::<SharedDocsFnc>)?;
        let account_id = helper.account_id;

        helper.get(|_id, properties| {
            let mut fields = self
                .get_orm::<VacationResponse>(account_id, JMAPId::singleton().get_document_id())?
                .ok_or_else(|| {
                    StoreError::NotFound("VacationResponse data not found".to_string())
                })?;
            let mut vacation_response = VecMap::with_capacity(properties.len());

            for property in properties {
                vacation_response.append(
                    *property,
                    if let Property::Id = property {
                        Value::Id {
                            value: JMAPId::singleton(),
                        }
                    } else if let Some(value) = fields.remove(property) {
                        value
                    } else {
                        Value::Null
                    },
                );
            }
            Ok(Some(VacationResponse {
                properties: vacation_response,
            }))
        })
    }

    fn build_vacation_response(
        &self,
        account_id: AccountId,
        from_name: Option<&str>,
        from_addr: &str,
        to: &str,
    ) -> store::Result<Option<VacationMessage>> {
        let id = JMAPId::singleton();
        let document_id = id.get_document_id();
        if let Some(mut vr) = self.get_orm::<VacationResponse>(account_id, document_id)? {
            if matches!(
                vr.get(&Property::IsEnabled),
                Some(Value::Bool { value: true })
            ) {
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0) as i64;
                if !matches!(vr.get(&Property::FromDate), Some(Value::DateTime { value: from_date })
                        if from_date.timestamp() > now)
                    && !matches!(vr.get(&Property::ToDate), Some(Value::DateTime { value: to_date })
                        if to_date.timestamp() < now)
                {
                    let address = to.to_string();

                    // Make sure we havent emailed this address before
                    let addresses = if let Some(Value::SentResponses {
                        value: mut addresses,
                    }) = vr.remove(&Property::SentResponses_)
                    {
                        if !addresses.insert(address.clone()) {
                            return Ok(None);
                        }
                        addresses
                    } else {
                        AHashSet::from_iter([address.clone()])
                    };

                    // Update the vacation response object with the new addresses
                    let mut new_vr = TinyORM::track_changes(&vr);
                    new_vr.set(
                        Property::SentResponses_,
                        Value::SentResponses { value: addresses },
                    );

                    // Build vacation response
                    let mut message = MessageBuilder::new()
                        .from(
                            from_name
                                .map(|from_name| Address::from((from_name, from_addr)))
                                .unwrap_or_else(|| Address::from(from_addr)),
                        )
                        .to(address.as_str())
                        .subject(
                            vr.get(&Property::Subject)
                                .and_then(|s| {
                                    if let Value::Text { value } = s {
                                        Some(value.as_str())
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or("Recipient is away"),
                        );

                    let mut has_body = false;
                    match vr.get(&Property::TextBody) {
                        Some(Value::Text { value }) if !value.is_empty() => {
                            message = message.text_body(value);
                            has_body = true;
                        }
                        _ => (),
                    }

                    match vr.get(&Property::HtmlBody) {
                        Some(Value::Text { value }) if !value.is_empty() => {
                            message = message.html_body(value);
                            has_body = true;
                        }
                        _ => (),
                    }

                    if !has_body {
                        message =
                            message.text_body("The requested recipient is away at the moment.");
                    }
                    let message = message.write_to_vec().unwrap_or_default();

                    // Save changes
                    let mut batch = WriteBatch::new(account_id);
                    let mut document = Document::new(Collection::VacationResponse, document_id);
                    vr.merge(&mut document, new_vr)?;
                    batch.update_document(document);
                    batch.log_update(Collection::VacationResponse, id);
                    self.write(batch)?;

                    return Ok(Some(VacationMessage {
                        from: from_addr.to_string(),
                        to: address,
                        message,
                    }));
                }
            }
        }
        Ok(None)
    }
}
