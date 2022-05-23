use std::collections::HashMap;

use crate::identity;
use crate::identity::schema::Identity;
use crate::mail::schema::Email;
use crate::mail::{MessageData, MessageField};

use jmap::error::set::{SetError, SetErrorType};

use jmap::id::jmap::JMAPId;
use jmap::jmap_store::orm::{self, JMAPOrm, TinyORM};
use jmap::jmap_store::Object;

use jmap::jmap_store::set::SetHelper;
use jmap::request::set::SetResponse;
use jmap::request::MaybeIdReference;
use jmap::{jmap_store::set::SetObject, request::set::SetRequest};
use mail_parser::RfcHeader;

use store::blob::BlobId;
use store::chrono::Utc;
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::parking_lot::MutexGuard;
use store::serialize::StoreSerialize;
use store::write::options::IndexOptions;
use store::{JMAPStore, Store};

use super::schema::{Address, EmailSubmission, Envelope, Property, Value};

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct SetArguments {
    #[serde(rename = "onSuccessUpdateEmail")]
    pub on_success_update_email: Option<HashMap<MaybeIdReference, Email>>,
    #[serde(rename = "onSuccessDestroyEmail")]
    pub on_success_destroy_email: Option<Vec<MaybeIdReference>>,
}

impl SetObject for EmailSubmission {
    type SetArguments = SetArguments;

    type NextInvocation = ();

    fn map_references(&mut self, fnc: impl FnMut(&str) -> Option<JMAPId>) {
        todo!()
    }
}

pub trait JMAPSetEmailSubmission<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn email_submission_set(
        &self,
        request: SetRequest<EmailSubmission>,
    ) -> jmap::Result<SetResponse<EmailSubmission>>;
}

impl<T> JMAPSetEmailSubmission<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn email_submission_set(
        &self,
        request: SetRequest<EmailSubmission>,
    ) -> jmap::Result<SetResponse<EmailSubmission>> {
        let mut helper = SetHelper::new(self, request)?;
        let mut on_success: Option<SetRequest<Email>> = if helper
            .request
            .arguments
            .on_success_destroy_email
            .as_ref()
            .map_or(false, |p| !p.is_empty())
            || helper
                .request
                .arguments
                .on_success_update_email
                .as_ref()
                .map_or(false, |p| !p.is_empty())
        {
            Some(SetRequest {
                account_id: helper.request.account_id,
                if_in_state: None,
                create: None,
                update: None,
                destroy: None,
                destroy_ref: None,
                arguments: (),
            })
        } else {
            None
        };

        helper.create(|create_id, item, helper, document| {
            let mut fields = TinyORM::<EmailSubmission>::new();
            let mut email_id = JMAPId::from(u32::MAX);
            let mut identity_id = u32::MAX;
            let mut envelope = None;

            for (property, value) in item.properties {
                let value = match (property, value) {
                    (Property::EmailId, Value::Id { value }) => {
                        fields.set(
                            Property::ThreadId,
                            orm::Value::Object(Value::Id {
                                value: value.get_prefix_id().into(),
                            }),
                        );
                        email_id = value;
                        orm::Value::Object(Value::Id { value })
                    }
                    (Property::IdentityId, Value::Id { value }) => {
                        identity_id = value.get_document_id();
                        orm::Value::Object(Value::Id { value })
                    }
                    (Property::Envelope, Value::Envelope { value }) => {
                        envelope = Some(value);
                        continue;
                    }
                    (Property::Envelope, Value::Null) => {
                        continue;
                    }
                    (Property::UndoStatus, value @ Value::UndoStatus { .. }) => {
                        orm::Value::Object(value)
                    }
                    (property, _) => {
                        return Err(SetError::invalid_property(
                            property,
                            "Field could not be set.",
                        ));
                    }
                };
                fields.set(property, value);
            }

            // Fetch mailFrom
            let mail_from = helper
                .store
                .get_orm::<Identity>(helper.account_id, identity_id)?
                .ok_or_else(|| {
                    SetError::invalid_property(Property::IdentityId, "Identity not found.")
                })?
                .remove_string(&identity::schema::Property::Email)
                .ok_or_else(|| {
                    SetError::invalid_property(
                        Property::IdentityId,
                        "The speficied identity does not have a valid e-mail address.",
                    )
                })?;

            // Make sure the envelope address matches the identity email address
            let mut envelope = if let Some(envelope) = envelope {
                if envelope.mail_from.email != mail_from {
                    return Err(SetError::invalid_property(
                        Property::IdentityId,
                        format!(
                            "The envelope mailFrom ({}) does not match the identity email ({})",
                            envelope.mail_from.email, mail_from
                        ),
                    ));
                }
                envelope
            } else {
                Envelope::new(mail_from)
            };

            // Make sure we have all required fields.
            if email_id.get_document_id() == u32::MAX || identity_id == u32::MAX {
                return Err(SetError::invalid_property(
                    Property::EmailId,
                    "emailId and identityId properties are required.",
                ));
            }

            // Set the sentAt property
            // TODO parse FUTURERELEASE
            fields.set(
                Property::SendAt,
                orm::Value::Object(Value::DateTime { value: Utc::now() }),
            );

            // Fetch message data
            let mut message_data = MessageData::from_metadata(
                &helper
                    .store
                    .blob_get(
                        &helper
                            .store
                            .get_document_value::<BlobId>(
                                helper.account_id,
                                Collection::Mail,
                                email_id.get_document_id(),
                                MessageField::Metadata.into(),
                            )?
                            .ok_or_else(|| {
                                SetError::invalid_property(Property::EmailId, "Email not found.")
                            })?,
                    )?
                    .ok_or(StoreError::DataCorruption)?,
            )
            .ok_or(StoreError::DataCorruption)?;

            // Obtain recipients from e-mail if missing
            if envelope.rcpt_to.is_empty() {
                for header in [RfcHeader::To, RfcHeader::Cc] {
                    if let Some(values) = message_data.headers.remove(&header) {
                        for value in values {
                            if let Some(recipients) = value.into_addresses() {
                                for recipient in recipients {
                                    envelope.rcpt_to.push(Address {
                                        email: recipient.email,
                                        parameters: None,
                                    });
                                }
                            }
                        }
                    }
                }

                if envelope.rcpt_to.is_empty() {
                    return Err(SetError::invalid_property(
                        Property::Envelope,
                        "No recipients found in the e-mail.",
                    ));
                }
            }

            // Add and link blob
            document.binary(
                Property::EmailId,
                message_data.raw_message.serialize().unwrap(),
                IndexOptions::new(),
            );
            document.blob(message_data.raw_message, IndexOptions::new());

            // Insert envelope
            fields.set(
                Property::Envelope,
                orm::Value::Object(Value::Envelope { value: envelope }),
            );

            // Validate fields
            fields.insert_validate(document)?;

            // Update onSuccess actions
            if let Some(on_success) = on_success.as_mut() {
                let id_ref = MaybeIdReference::Reference(create_id.to_string());
                if let Some(update) = helper
                    .request
                    .arguments
                    .on_success_update_email
                    .as_mut()
                    .and_then(|p| p.remove(&id_ref))
                {
                    on_success
                        .update
                        .get_or_insert_with(HashMap::new)
                        .insert(email_id, update);
                }
                if helper
                    .request
                    .arguments
                    .on_success_destroy_email
                    .as_ref()
                    .map_or(false, |p| p.contains(&id_ref))
                {
                    on_success
                        .destroy
                        .get_or_insert_with(Vec::new)
                        .push(email_id);
                }
            }

            Ok((
                EmailSubmission::new(document.document_id.into()),
                None::<MutexGuard<'_, ()>>,
            ))
        })?;

        helper.update(|id, mut item, helper, document| {
            // Only undoStatus can be changed
            if let Some(Value::UndoStatus { value }) = item.properties.remove(&Property::UndoStatus)
            {
                let current_fields = self
                    .get_orm::<EmailSubmission>(helper.account_id, id.get_document_id())?
                    .ok_or_else(|| SetError::new_err(SetErrorType::NotFound))?;
                let mut fields = TinyORM::track_changes(&current_fields);

                fields.set(
                    Property::UndoStatus,
                    orm::Value::Object(Value::UndoStatus { value }),
                );

                // Merge changes
                current_fields.merge_validate(document, fields)?;
            }

            Ok(None)
        })?;

        helper.destroy(|_id, _batch, _document| {
            Err(SetError::forbidden(concat!(
                "Deleting Email Submissions is not allowed, ",
                "update its status to 'canceled' insted."
            )))
        })?;

        helper.into_response()
    }
}
