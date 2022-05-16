use std::collections::HashMap;
use std::time::SystemTime;

use crate::error::set::{SetError, SetErrorType};
use crate::jmap_store::orm::{JMAPOrm, TinyORM};
use crate::jmap_store::set::{
    DefaultCreateItem, DefaultUpdateItem, SetObject, SetObjectData, SetObjectHelper,
};
use crate::protocol::invocation::{Invocation, Object};
use crate::protocol::json::JSONValue;
use crate::request::parse_utc_date;
use crate::request::set::SetRequest;

use store::core::document::Document;
use store::core::JMAPIdPrefix;
use store::rand::distributions::Alphanumeric;
use store::rand::{thread_rng, Rng};
use store::{AccountId, Store};
use store::{JMAPId, JMAPStore};

use super::PushSubscriptionProperty;

const EXPIRES_MAX: u64 = 7 * 24 * 3600; // 7 days
const MAX_SUBSCRIPTIONS: u64 = 100;
const VERIFICATION_CODE_LEN: usize = 32;

#[derive(Default)]
pub struct SetPushSubscription {
    pub current_subscription: Option<TinyORM<PushSubscriptionProperty>>,
    pub subscription: TinyORM<PushSubscriptionProperty>,
}

pub struct SetPushSubscriptionHelper {}

impl<T> SetObjectData<T> for SetPushSubscriptionHelper
where
    T: for<'x> Store<'x> + 'static,
{
    fn new(_store: &JMAPStore<T>, _request: &mut SetRequest) -> crate::Result<Self> {
        Ok(SetPushSubscriptionHelper {})
    }

    fn unwrap_invocation(self) -> Option<Invocation> {
        None
    }
}

impl<'y, T> SetObject<'y, T> for SetPushSubscription
where
    T: for<'x> Store<'x> + 'static,
{
    type Property = PushSubscriptionProperty;
    type Helper = SetPushSubscriptionHelper;
    type CreateItemResult = DefaultCreateItem;
    type UpdateItemResult = DefaultUpdateItem;

    fn new(
        helper: &mut SetObjectHelper<T, SetPushSubscriptionHelper>,
        _fields: &mut HashMap<String, JSONValue>,
        jmap_id: Option<JMAPId>,
    ) -> crate::error::set::Result<Self> {
        Ok(if let Some(jmap_id) = jmap_id {
            let current_subscription = helper
                .store
                .get_orm::<PushSubscriptionProperty>(helper.account_id, jmap_id.get_document_id())?
                .ok_or_else(|| {
                    SetError::new(
                        SetErrorType::NotFound,
                        "PushSubscription not found.".to_string(),
                    )
                })?;
            SetPushSubscription {
                subscription: TinyORM::track_changes(&current_subscription),
                current_subscription: current_subscription.into(),
            }
        } else {
            SetPushSubscription::default()
        })
    }

    fn set_field(
        &mut self,
        _helper: &mut SetObjectHelper<T, SetPushSubscriptionHelper>,
        field: Self::Property,
        value: JSONValue,
    ) -> crate::error::set::Result<()> {
        let value = match (field, &value) {
            (PushSubscriptionProperty::DeviceClientId, JSONValue::String(id))
                if self.current_subscription.is_none() && id.len() < 255 =>
            {
                Ok(value)
            }
            (PushSubscriptionProperty::Url, JSONValue::String(url))
                if self.current_subscription.is_none()
                    && url.starts_with("https://")
                    && url.len() < 512 =>
            {
                Ok(value)
            }
            (PushSubscriptionProperty::Keys, JSONValue::Object(keys))
                if self.current_subscription.is_none() =>
            {
                if keys
                    .get("p256dh")
                    .and_then(|v| v.to_string())
                    .and_then(|v| base64::decode_config(v, base64::URL_SAFE).ok())
                    .map_or(false, |v| (5..=255).contains(&v.len()))
                    && keys
                        .get("auth")
                        .and_then(|v| v.to_string())
                        .and_then(|v| base64::decode_config(v, base64::URL_SAFE).ok())
                        .map_or(false, |v| (5..=255).contains(&v.len()))
                    && keys.len() == 2
                {
                    Ok(value)
                } else {
                    Err(SetError::invalid_property(
                        field.to_string(),
                        "Invalid keys object.".to_string(),
                    ))
                }
            }
            (PushSubscriptionProperty::VerificationCode, JSONValue::String(name)) => {
                if self
                    .current_subscription
                    .as_ref()
                    .and_then(|c| c.get_string(&PushSubscriptionProperty::VerificationCode_))
                    .map_or(false, |v| v == name)
                {
                    Ok(value)
                } else {
                    Err(SetError::invalid_property(
                        field.to_string(),
                        "Verification code does not match.".to_string(),
                    ))
                }
            }
            (PushSubscriptionProperty::Expires, JSONValue::String(expires)) => {
                let expires = parse_utc_date(expires).ok_or_else(|| {
                    SetError::invalid_property(
                        field.to_string(),
                        format!("Invalid date: {}", expires),
                    )
                })?;
                let current_time = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);

                Ok(
                    if expires > current_time && (expires - current_time) > EXPIRES_MAX {
                        current_time + EXPIRES_MAX
                    } else {
                        expires
                    }
                    .into(),
                )
            }
            (PushSubscriptionProperty::Expires, JSONValue::Null) => Ok((SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
                + EXPIRES_MAX)
                .into()),
            (PushSubscriptionProperty::Types, JSONValue::Array(types)) => {
                for obj_type in types {
                    if obj_type.to_string().and_then(Object::parse).is_none() {
                        return Err(SetError::invalid_property(
                            field.to_string(),
                            "One or more TypeState objects are invalid.".to_string(),
                        ));
                    }
                }

                Ok(value)
            }
            (PushSubscriptionProperty::Types, JSONValue::Null) => Ok(value),
            (
                PushSubscriptionProperty::VerificationCode | PushSubscriptionProperty::Keys,
                JSONValue::Null,
            ) if self.current_subscription.is_none() => Ok(value),
            (_, _) => Err(SetError::invalid_property(
                field.to_string(),
                "Property cannot be set or an invalid value was provided.".to_string(),
            )),
        }?;

        self.subscription.set(field, value);

        Ok(())
    }

    fn patch_field(
        &mut self,
        _helper: &mut SetObjectHelper<T, SetPushSubscriptionHelper>,
        field: Self::Property,
        _property: String,
        _value: JSONValue,
    ) -> crate::error::set::Result<()> {
        Err(SetError::invalid_property(
            field.to_string(),
            "Patch operations not supported on this field.",
        ))
    }

    fn create(
        mut self,
        helper: &mut SetObjectHelper<T, Self::Helper>,
        _create_id: &str,
        document: &mut Document,
    ) -> crate::error::set::Result<Self::CreateItemResult> {
        // Limit the number of subscriptions
        if helper.document_ids.len() > MAX_SUBSCRIPTIONS {
            return Err(SetError::new(
                SetErrorType::Forbidden,
                "There are too many subscriptions, please delete some before adding a new one."
                    .to_string(),
            ));
        }

        // Add expire time if missing
        if !self
            .subscription
            .has_property(&PushSubscriptionProperty::Expires)
        {
            self.subscription.set(
                PushSubscriptionProperty::Expires,
                (SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0)
                    + EXPIRES_MAX)
                    .into(),
            );
        }

        // Generate random verification code
        self.subscription.set(
            PushSubscriptionProperty::VerificationCode_,
            thread_rng()
                .sample_iter(Alphanumeric)
                .take(VERIFICATION_CODE_LEN)
                .map(char::from)
                .collect::<String>()
                .into(),
        );

        // Insert and validate
        self.subscription.insert_validate(document)?;

        Ok(DefaultCreateItem::new(document.document_id as JMAPId))
    }

    fn update(
        self,
        _helper: &mut SetObjectHelper<T, Self::Helper>,
        document: &mut Document,
    ) -> crate::error::set::Result<Option<Self::UpdateItemResult>> {
        if self
            .current_subscription
            .unwrap()
            .merge_validate(document, self.subscription)?
        {
            Ok(Some(DefaultUpdateItem::default()))
        } else {
            Ok(None)
        }
    }

    fn delete(
        store: &JMAPStore<T>,
        account_id: AccountId,
        document: &mut Document,
    ) -> store::Result<()> {
        // Delete index
        if let Some(orm) =
            store.get_orm::<PushSubscriptionProperty>(account_id, document.document_id)?
        {
            orm.delete(document);
        }
        Ok(())
    }

    fn validate_delete(
        _helper: &mut SetObjectHelper<T, Self::Helper>,
        _jmap_id: JMAPId,
    ) -> crate::error::set::Result<()> {
        Ok(())
    }
}
