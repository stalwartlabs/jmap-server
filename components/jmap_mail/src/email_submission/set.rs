use std::collections::HashMap;
use std::time::SystemTime;

use crate::identity::IdentityProperty;
use crate::mail::get::get_rfc_header;
use crate::mail::{MailHeaderForm, MailProperty, MessageData, MessageField};

use super::EmailSubmissionProperty;
use jmap::error::set::{SetError, SetErrorType};
use jmap::id::JMAPIdSerialize;
use jmap::jmap_store::orm::{JMAPOrm, TinyORM};
use jmap::jmap_store::set::{DefaultCreateItem, DefaultUpdateItem};
use jmap::protocol::invocation::{Invocation, Method, Object};
use jmap::{
    jmap_store::set::{SetObject, SetObjectData, SetObjectHelper},
    protocol::json::JSONValue,
    request::set::SetRequest,
};
use mail_parser::RfcHeader;

use store::blob::BlobId;
use store::core::collection::Collection;
use store::core::document::Document;
use store::core::error::StoreError;
use store::core::JMAPIdPrefix;
use store::serialize::StoreSerialize;
use store::write::options::IndexOptions;
use store::{AccountId, JMAPId, JMAPStore, Store};

#[derive(Default)]
pub struct SetEmailSubmission {
    pub current_email_submission: Option<TinyORM<EmailSubmissionProperty>>,
    pub email_submission: TinyORM<EmailSubmissionProperty>,
    pub email_id: JMAPId,
    pub identity_id: JMAPId,
}

struct OnSuccess {
    update_email: HashMap<String, JSONValue>,
    destroy_email: Vec<String>,
    set_email: SetRequest,
}

#[derive(Default)]
pub struct Envelope {
    pub mail_from: Option<Address>,
    pub rcpt_to: Vec<Address>,
}

pub struct Address {
    pub email: String,
    pub parameters: Option<HashMap<String, String>>,
}

pub struct SetEmailSubmissionHelper {
    account_id: AccountId,
    on_success: Option<OnSuccess>,
}

impl<T> SetObjectData<T> for SetEmailSubmissionHelper
where
    T: for<'x> Store<'x> + 'static,
{
    fn new(_store: &JMAPStore<T>, request: &mut SetRequest) -> jmap::Result<Self> {
        let on_success_update_email = request
            .arguments
            .remove("onSuccessUpdateEmail")
            .and_then(|p| p.unwrap_object());
        let on_success_destroy_email =
            request
                .arguments
                .remove("onSuccessDestroyEmail")
                .and_then(|p| {
                    p.unwrap_array().and_then(|array| {
                        array
                            .into_iter()
                            .filter_map(|v| v.unwrap_string())
                            .collect::<Vec<String>>()
                            .into()
                    })
                });

        Ok(SetEmailSubmissionHelper {
            account_id: request.account_id,
            on_success: if on_success_update_email.is_some() || on_success_destroy_email.is_some() {
                let update_email = on_success_update_email.unwrap_or_default();
                let destroy_email = on_success_destroy_email.unwrap_or_default();
                Some(OnSuccess {
                    set_email: SetRequest {
                        account_id: request.account_id,
                        if_in_state: None,
                        create: Vec::with_capacity(0),
                        update: HashMap::with_capacity(update_email.len()),
                        destroy: Vec::with_capacity(destroy_email.len()),
                        arguments: HashMap::with_capacity(0),
                        tombstone_deletions: false,
                    },
                    update_email,
                    destroy_email,
                })
            } else {
                None
            },
        })
    }

    fn unwrap_invocation(self) -> Option<Invocation> {
        match self.on_success {
            Some(on_success)
                if !on_success.set_email.update.is_empty()
                    || !on_success.set_email.destroy.is_empty() =>
            {
                Invocation {
                    obj: Object::Email,
                    call: Method::Set(on_success.set_email),
                    account_id: self.account_id,
                }
                .into()
            }
            _ => None,
        }
    }
}

impl<'y, T> SetObject<'y, T> for SetEmailSubmission
where
    T: for<'x> Store<'x> + 'static,
{
    type Property = EmailSubmissionProperty;
    type Helper = SetEmailSubmissionHelper;
    type CreateItemResult = DefaultCreateItem;
    type UpdateItemResult = DefaultUpdateItem;

    fn new(
        helper: &mut SetObjectHelper<T, Self::Helper>,
        _fields: &mut HashMap<String, JSONValue>,
        jmap_id: Option<JMAPId>,
    ) -> jmap::error::set::Result<Self> {
        Ok(SetEmailSubmission {
            current_email_submission: if let Some(jmap_id) = jmap_id {
                helper
                    .store
                    .get_orm::<EmailSubmissionProperty>(
                        helper.account_id,
                        jmap_id.get_document_id(),
                    )?
                    .ok_or_else(|| {
                        SetError::new(
                            SetErrorType::NotFound,
                            "EmailSubmission not found.".to_string(),
                        )
                    })?
                    .into()
            } else {
                None
            },
            email_id: JMAPId::MAX,
            identity_id: JMAPId::MAX,
            ..Default::default()
        })
    }

    fn set_field(
        &mut self,
        _helper: &mut SetObjectHelper<T, Self::Helper>,
        field: Self::Property,
        value: JSONValue,
    ) -> jmap::error::set::Result<()> {
        let is_create = self.current_email_submission.is_none();
        match (field, &value) {
            (EmailSubmissionProperty::EmailId, JSONValue::String(id)) if is_create => {
                if let Some(id) = JMAPId::from_jmap_string(id) {
                    self.email_submission.set(field, value);
                    self.email_id = id;
                } else {
                    return Err(SetError::invalid_property(
                        field.to_string(),
                        "emailId does not exist.",
                    ));
                }
            }
            (EmailSubmissionProperty::IdentityId, JSONValue::String(id)) if is_create => {
                if let Some(id) = JMAPId::from_jmap_string(id) {
                    self.email_submission.set(field, value);
                    self.identity_id = id;
                } else {
                    return Err(SetError::invalid_property(
                        field.to_string(),
                        "identityId does not exist.",
                    ));
                }
            }
            (EmailSubmissionProperty::Envelope, JSONValue::Object(_) | JSONValue::Null)
                if is_create =>
            {
                self.email_submission.set(field, value);
            }
            (EmailSubmissionProperty::UndoStatus, JSONValue::String(status))
                if ["pending", "final", "canceled"].contains(&status.as_str()) =>
            {
                self.email_submission.set(field, value);
            }

            (field, _) => {
                return Err(SetError::invalid_property(
                    field.to_string(),
                    "Field could not be set.",
                ));
            }
        }
        Ok(())
    }

    fn patch_field(
        &mut self,
        _helper: &mut SetObjectHelper<T, Self::Helper>,
        field: Self::Property,
        _property: String,
        _value: JSONValue,
    ) -> jmap::error::set::Result<()> {
        Err(SetError::invalid_property(
            field.to_string(),
            "Patch operations not supported on this field.",
        ))
    }

    fn create(
        mut self,
        helper: &mut SetObjectHelper<T, Self::Helper>,
        create_id: &str,
        document: &mut Document,
    ) -> jmap::error::set::Result<Self::CreateItemResult> {
        if self.email_id == JMAPId::MAX || self.identity_id == JMAPId::MAX {
            return Err(SetError::invalid_property(
                EmailSubmissionProperty::EmailId.to_string(),
                "emailId and identityId properties are required.",
            ));
        }

        // Set threadId
        self.email_submission.set(
            EmailSubmissionProperty::ThreadId,
            (self.email_id.get_prefix_id() as JMAPId).into(),
        );

        // Parse envelope property
        let envelope = self
            .email_submission
            .remove(&EmailSubmissionProperty::Envelope)
            .unwrap_or_default();
        let mut envelope = if !envelope.is_null() {
            Envelope::parse(envelope).ok_or_else(|| {
                SetError::invalid_property(
                    EmailSubmissionProperty::Envelope.to_string(),
                    "Failed to parse Envelope.",
                )
            })?
        } else {
            Envelope::default()
        };

        // Make sure the envelope matches the identity address
        let mail_from = helper
            .store
            .get_orm::<IdentityProperty>(
                helper.data.account_id,
                self.identity_id.get_document_id(),
            )?
            .ok_or_else(|| {
                SetError::invalid_property(
                    EmailSubmissionProperty::IdentityId.to_string(),
                    "Identity not found.",
                )
            })?
            .remove(&IdentityProperty::Email)
            .and_then(|email| email.unwrap_string())
            .ok_or_else(|| {
                SetError::invalid_property(
                    EmailSubmissionProperty::IdentityId.to_string(),
                    "The speficied identity does not have a valid e-mail address.",
                )
            })?;
        if let Some(envelope_mail_from) = envelope.mail_from.as_ref() {
            if envelope_mail_from.email != mail_from {
                return Err(SetError::invalid_property(
                    EmailSubmissionProperty::IdentityId.to_string(),
                    format!(
                        "The envelope mailFrom ({}) does not match the identity email ({})",
                        envelope_mail_from.email, mail_from
                    ),
                ));
            }
        } else {
            envelope.mail_from = Some(Address {
                email: mail_from,
                parameters: None,
            });
        }

        // Set the sentAt property
        // TODO parse FUTURERELEASE
        self.email_submission.set(
            EmailSubmissionProperty::SendAt,
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
                .into(),
        );

        // Fetch message data
        let document_id = self.email_id.get_document_id();
        let mut message_data = MessageData::from_metadata(
            &helper
                .store
                .blob_get(
                    &helper
                        .store
                        .get_document_value::<BlobId>(
                            helper.account_id,
                            Collection::Mail,
                            document_id,
                            MessageField::Metadata.into(),
                        )?
                        .ok_or_else(|| {
                            SetError::invalid_property(
                                EmailSubmissionProperty::EmailId.to_string(),
                                "Email not found.",
                            )
                        })?,
                )?
                .ok_or(StoreError::DataCorruption)?,
        )
        .ok_or(StoreError::DataCorruption)?;

        // Obtain recipients from e-mail if missing
        if envelope.rcpt_to.is_empty() {
            for (property, header) in [
                (MailProperty::To, RfcHeader::To),
                (MailProperty::Cc, RfcHeader::Cc),
            ] {
                if let Some(recipients) = get_rfc_header(
                    &mut message_data.properties,
                    header,
                    MailHeaderForm::Addresses,
                    false,
                )
                .map_err(|_| {
                    SetError::invalid_property(property.to_string(), "Failed to parse recipients.")
                })?
                .unwrap_array()
                {
                    recipients.into_iter().for_each(|recipient| {
                        if let Some(mut recipient) = recipient.unwrap_object() {
                            if let Some(email) = recipient
                                .remove("email")
                                .and_then(|email| email.unwrap_string())
                            {
                                envelope.rcpt_to.push(Address {
                                    email,
                                    parameters: None,
                                })
                            }
                        }
                    });
                }
            }
        }

        // Add and link blob
        document.binary(
            EmailSubmissionProperty::EmailId,
            message_data.raw_message.serialize().unwrap(),
            IndexOptions::new(),
        );
        document.blob(message_data.raw_message, IndexOptions::new());

        // Insert envelope
        self.email_submission
            .set(EmailSubmissionProperty::Envelope, envelope.into());

        self.email_submission.insert_validate(document)?;

        if let Some(on_success) = helper.data.on_success.as_mut() {
            let create_id = format!("#{}", create_id);
            if let Some(update_email) = on_success.update_email.remove(&create_id) {
                on_success
                    .set_email
                    .update
                    .insert(self.email_id.to_jmap_string(), update_email);
            } else if on_success.destroy_email.contains(&create_id) {
                on_success
                    .set_email
                    .destroy
                    .push(self.email_id.to_jmap_string().into());
            }
        }

        Ok(DefaultCreateItem::new(document.document_id as JMAPId))
    }

    fn update(
        self,
        _helper: &mut SetObjectHelper<T, Self::Helper>,
        document: &mut Document,
    ) -> jmap::error::set::Result<Option<Self::UpdateItemResult>> {
        if self
            .current_email_submission
            .unwrap()
            .merge_validate(document, self.email_submission)?
        {
            Ok(Some(DefaultUpdateItem::default()))
        } else {
            Ok(None)
        }
    }

    fn validate_delete(
        _helper: &mut SetObjectHelper<T, Self::Helper>,
        _jmap_id: JMAPId,
    ) -> jmap::error::set::Result<()> {
        Err(SetError::forbidden(concat!(
            "Deleting Email Submissions is not allowed, ",
            "update its status to 'canceled' insted."
        )))
    }

    fn delete(
        _store: &JMAPStore<T>,
        _account_id: AccountId,
        _document: &mut Document,
    ) -> store::Result<()> {
        Ok(())
    }
}

impl Envelope {
    pub fn parse(envelope: JSONValue) -> Option<Self> {
        let mut envelope = envelope.unwrap_object()?;
        Some(Envelope {
            mail_from: envelope.remove("mailFrom").and_then(Address::parse),
            rcpt_to: envelope
                .remove("rcptTo")?
                .unwrap_array()?
                .into_iter()
                .filter_map(Address::parse)
                .collect::<Vec<Address>>(),
        })
    }
}

impl Address {
    pub fn parse(address: JSONValue) -> Option<Self> {
        let mut address = address.unwrap_object()?;
        let email = address.remove("email")?.unwrap_string()?;
        if email.len() > 255 {
            return None;
        }

        Some(Address {
            email,
            parameters: if let Some(parameters) = address
                .remove("parameters")
                .and_then(|params| params.unwrap_object())
            {
                parameters
                    .into_iter()
                    .filter_map(|(k, v)| {
                        v.unwrap_string().and_then(|v| {
                            if k.len() < 256 && v.len() < 256 {
                                (k, v).into()
                            } else {
                                None
                            }
                        })
                    })
                    .collect::<HashMap<String, String>>()
                    .into()
            } else {
                None
            },
        })
    }
}

impl From<Envelope> for JSONValue {
    fn from(envelope: Envelope) -> Self {
        let mut result = HashMap::with_capacity(2);
        result.insert("mailFrom".into(), envelope.mail_from.into());
        result.insert(
            "rcptTo".into(),
            envelope
                .rcpt_to
                .into_iter()
                .map(|addr| addr.into())
                .collect::<Vec<JSONValue>>()
                .into(),
        );
        result.into()
    }
}

impl From<Address> for JSONValue {
    fn from(addr: Address) -> Self {
        let mut result = HashMap::with_capacity(2);
        result.insert("email".to_string(), addr.email.into());
        result.insert(
            "parameters".to_string(),
            if let Some(parameters) = addr.parameters {
                parameters
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect::<HashMap<String, JSONValue>>()
                    .into()
            } else {
                JSONValue::Null
            },
        );
        result.into()
    }
}
