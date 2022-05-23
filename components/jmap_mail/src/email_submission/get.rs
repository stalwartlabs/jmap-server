use std::collections::HashMap;

use jmap::jmap_store::get::{default_mapper, GetHelper, GetObject};
use jmap::jmap_store::orm::{self, JMAPOrm};
use jmap::request::get::{GetRequest, GetResponse};

use store::core::error::StoreError;
use store::JMAPStore;
use store::Store;

use super::schema::{EmailSubmission, Property, Value};

impl GetObject for EmailSubmission {
    type GetArguments = ();

    fn default_properties() -> Vec<Self::Property> {
        vec![
            Property::Id,
            Property::EmailId,
            Property::IdentityId,
            Property::ThreadId,
            Property::Envelope,
            Property::SendAt,
            Property::UndoStatus,
            Property::DeliveryStatus,
            Property::DsnBlobIds,
            Property::MdnBlobIds,
        ]
    }
}

pub trait JMAPGetEmailSubmission<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn email_submission_get(
        &self,
        request: GetRequest<EmailSubmission>,
    ) -> jmap::Result<GetResponse<EmailSubmission>>;
}

impl<T> JMAPGetEmailSubmission<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn email_submission_get(
        &self,
        request: GetRequest<EmailSubmission>,
    ) -> jmap::Result<GetResponse<EmailSubmission>> {
        let helper = GetHelper::new(self, request, default_mapper.into())?;
        let account_id = helper.account_id;

        helper.get(|id, properties| {
            let document_id = id.get_document_id();
            let mut fields = self
                .get_orm::<EmailSubmission>(account_id, document_id)?
                .ok_or_else(|| {
                    StoreError::InternalError("EmailSubmission data not found".to_string())
                })?;
            let mut email_submission = HashMap::with_capacity(properties.len());

            for property in properties {
                email_submission.insert(
                    *property,
                    if let Property::Id = property {
                        Value::Id { value: id }
                    } else if let Some(orm::Value::Object(value)) = fields.remove(property) {
                        value
                    } else {
                        Value::Null
                    },
                );
            }
            Ok(Some(EmailSubmission {
                properties: email_submission,
            }))
        })
    }
}
