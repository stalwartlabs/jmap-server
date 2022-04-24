use std::collections::HashMap;

use super::IdentityProperty;
use jmap::error::set::{SetError, SetErrorType};
use jmap::jmap_store::orm::{JMAPOrm, TinyORM};
use jmap::jmap_store::set::{DefaultCreateItem, DefaultUpdateItem};
use jmap::protocol::invocation::Invocation;
use jmap::{
    jmap_store::set::{SetObject, SetObjectData, SetObjectHelper},
    protocol::json::JSONValue,
    request::set::SetRequest,
};
use store::batch::Document;
use store::{JMAPId, JMAPIdPrefix, JMAPStore, Store};

#[derive(Default)]
pub struct SetIdentity {
    pub current_identity: Option<TinyORM<IdentityProperty>>,
    pub identity: TinyORM<IdentityProperty>,
}

pub struct SetIdentityHelper {}

impl<T> SetObjectData<T> for SetIdentityHelper
where
    T: for<'x> Store<'x> + 'static,
{
    fn new(_store: &JMAPStore<T>, _request: &mut SetRequest) -> jmap::Result<Self> {
        Ok(SetIdentityHelper {})
    }

    fn unwrap_invocation(self) -> Option<Invocation> {
        None
    }
}

impl<'y, T> SetObject<'y, T> for SetIdentity
where
    T: for<'x> Store<'x> + 'static,
{
    type Property = IdentityProperty;
    type Helper = SetIdentityHelper;
    type CreateItemResult = DefaultCreateItem;
    type UpdateItemResult = DefaultUpdateItem;

    fn new(
        helper: &mut SetObjectHelper<T, Self::Helper>,
        _fields: &mut HashMap<String, JSONValue>,
        jmap_id: Option<JMAPId>,
    ) -> jmap::error::set::Result<Self> {
        Ok(SetIdentity {
            current_identity: if let Some(jmap_id) = jmap_id {
                helper
                    .store
                    .get_orm::<IdentityProperty>(helper.account_id, jmap_id.get_document_id())?
                    .ok_or_else(|| {
                        SetError::new(SetErrorType::NotFound, "Identity not found.".to_string())
                    })?
                    .into()
            } else {
                None
            },
            ..Default::default()
        })
    }

    fn set_field(
        &mut self,
        _helper: &mut SetObjectHelper<T, Self::Helper>,
        field: Self::Property,
        value: JSONValue,
    ) -> jmap::error::set::Result<()> {
        match (field, &value) {
            (IdentityProperty::Name, JSONValue::String(name)) if name.len() < 255 => {
                self.identity.set(field, value);
            }
            (IdentityProperty::Email, JSONValue::String(email))
                if email.contains('@') && email.len() < 255 && self.current_identity.is_none() =>
            {
                self.identity.set(field, value);
            }
            (IdentityProperty::ReplyTo | IdentityProperty::Bcc, JSONValue::Array(addresses)) => {
                for address in addresses {
                    if !validate_email_address(address) {
                        return Err(SetError::invalid_property(
                            field.to_string(),
                            "Invalid EmailAddress.".to_string(),
                        ));
                    }
                }
                self.identity.set(field, value);
            }
            (
                IdentityProperty::TextSignature | IdentityProperty::HtmlSignature,
                JSONValue::String(signature),
            ) if signature.len() < 1024 => {
                self.identity.set(field, value);
            }
            (_, JSONValue::Null) => {
                self.identity.set(field, value);
            }
            (field, _) => {
                return Err(SetError::invalid_property(
                    field.to_string(),
                    "Field cannot be set.",
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
        self,
        _helper: &mut SetObjectHelper<T, Self::Helper>,
        _create_id: &str,
        document: &mut Document,
    ) -> jmap::error::set::Result<Self::CreateItemResult> {
        TinyORM::default().merge_validate(document, self.identity)?;
        Ok(DefaultCreateItem::new(document.document_id as JMAPId))
    }

    fn update(
        self,
        _helper: &mut SetObjectHelper<T, Self::Helper>,
        document: &mut Document,
    ) -> jmap::error::set::Result<Option<Self::UpdateItemResult>> {
        if self
            .current_identity
            .unwrap()
            .merge_validate(document, self.identity)?
        {
            Ok(Some(DefaultUpdateItem::default()))
        } else {
            Ok(None)
        }
    }

    fn delete(
        _helper: &mut SetObjectHelper<T, Self::Helper>,
        _jmap_id: store::JMAPId,
    ) -> jmap::error::set::Result<()> {
        Ok(())
    }
}

fn validate_email_address(argument: &JSONValue) -> bool {
    let mut has_email = false;
    if let Some(address) = argument.to_object() {
        for (key, value) in address {
            if key == "email" {
                has_email = value
                    .to_string()
                    .map(|v| v.contains('@') && v.len() <= 255)
                    .unwrap_or(false);
            } else if key == "name" {
                match value {
                    JSONValue::Null => (),
                    JSONValue::String(name) if name.len() <= 255 => (),
                    _ => return false,
                }
            } else {
                return false;
            }
        }
    }
    has_email
}
