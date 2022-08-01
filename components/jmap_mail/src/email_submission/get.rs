use jmap::jmap_store::get::{default_mapper, GetHelper, GetObject, SharedDocsFnc};
use jmap::orm::serialize::JMAPOrm;
use jmap::request::get::{GetRequest, GetResponse};
use jmap::types::jmap::JMAPId;

use store::core::error::StoreError;
use store::core::vec_map::VecMap;
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

    fn get_as_id(&self, property: &Self::Property) -> Option<Vec<JMAPId>> {
        match self.properties.get(property)? {
            Value::Id { value } => Some(vec![*value]),
            _ => None,
        }
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
        let mut helper =
            GetHelper::new(self, request, default_mapper.into(), None::<SharedDocsFnc>)?;
        let account_id = helper.account_id;

        // Add Id Property
        if !helper.properties.contains(&Property::Id) {
            helper.properties.push(Property::Id);
        }

        helper.get(|id, properties| {
            let document_id = id.get_document_id();
            let mut fields = self
                .get_orm::<EmailSubmission>(account_id, document_id)?
                .ok_or_else(|| {
                    StoreError::InternalError("EmailSubmission data not found".to_string())
                })?;
            let mut email_submission = VecMap::with_capacity(properties.len());

            for property in properties {
                email_submission.append(
                    *property,
                    if let Property::Id = property {
                        Value::Id { value: id }
                    } else if let Some(value) = fields.remove(property) {
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
