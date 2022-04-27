use std::collections::HashMap;

use crate::mail::MessageField;
use jmap::error::set::{SetError, SetErrorType};
use jmap::jmap_store::orm::{JMAPOrm, TinyORM};
use jmap::jmap_store::set::{
    DefaultCreateItem, DefaultUpdateItem, SetObject, SetObjectData, SetObjectHelper,
};
use jmap::protocol::invocation::Invocation;
use jmap::protocol::json::{JSONNumber, JSONValue};
use jmap::request::set::SetRequest;
use store::batch::Document;
use store::query::DefaultIdMapper;
use store::Store;
use store::{
    Collection, Comparator, ComparisonOperator, DocumentId, FieldValue, Filter, JMAPId,
    JMAPIdPrefix, JMAPStore, LongInteger, StoreError, Tag,
};

use super::MailboxProperty;
//TODO mailbox id 0 is inbox and cannot be deleted

#[derive(Default)]
pub struct SetMailbox {
    pub current_mailbox: Option<TinyORM<MailboxProperty>>,
    pub mailbox: TinyORM<MailboxProperty>,
}

pub struct SetMailboxHelper {
    on_destroy_remove_emails: bool,
}

impl<T> SetObjectData<T> for SetMailboxHelper
where
    T: for<'x> Store<'x> + 'static,
{
    fn new(_store: &JMAPStore<T>, request: &mut SetRequest) -> jmap::Result<Self> {
        Ok(SetMailboxHelper {
            on_destroy_remove_emails: request
                .arguments
                .get("onDestroyRemoveEmails")
                .and_then(|v| v.to_bool())
                .unwrap_or(false),
        })
    }

    fn unwrap_invocation(self) -> Option<Invocation> {
        None
    }
}

impl<'y, T> SetObject<'y, T> for SetMailbox
where
    T: for<'x> Store<'x> + 'static,
{
    type Property = MailboxProperty;
    type Helper = SetMailboxHelper;
    type CreateItemResult = DefaultCreateItem;
    type UpdateItemResult = DefaultUpdateItem;

    fn new(
        helper: &mut SetObjectHelper<T, SetMailboxHelper>,
        _fields: &mut HashMap<String, JSONValue>,
        jmap_id: Option<JMAPId>,
    ) -> jmap::error::set::Result<Self> {
        Ok(if let Some(jmap_id) = jmap_id {
            let current_mailbox = helper
                .store
                .get_orm::<MailboxProperty>(helper.account_id, jmap_id.get_document_id())?
                .ok_or_else(|| {
                    SetError::new(SetErrorType::NotFound, "Mailbox not found.".to_string())
                })?;
            SetMailbox {
                mailbox: TinyORM::track_changes(&current_mailbox),
                current_mailbox: current_mailbox.into(),
            }
        } else {
            SetMailbox::default()
        })
    }

    fn set_field(
        &mut self,
        helper: &mut SetObjectHelper<T, SetMailboxHelper>,
        field: Self::Property,
        value: JSONValue,
    ) -> jmap::error::set::Result<()> {
        //TODO implement isSubscribed
        let value = match (field, &value) {
            (MailboxProperty::Name, JSONValue::String(name)) => {
                if name.len() < 255 {
                    Ok(value)
                } else {
                    Err(SetError::invalid_property(
                        field.to_string(),
                        "Mailbox name is too long.".to_string(),
                    ))
                }
            }
            (MailboxProperty::ParentId, JSONValue::String(mailbox_parent_id_str)) => {
                match helper.resolve_reference(mailbox_parent_id_str) {
                    Ok(mailbox_parent_id) => {
                        if helper.will_destroy.contains(&mailbox_parent_id) {
                            return Err(SetError::new(
                                SetErrorType::WillDestroy,
                                "Parent ID will be destroyed.",
                            ));
                        } else if !helper
                            .document_ids
                            .contains(mailbox_parent_id as DocumentId)
                        {
                            return Err(SetError::new(
                                SetErrorType::InvalidProperties,
                                "Parent ID does not exist.",
                            ));
                        }
                        Ok((mailbox_parent_id + 1).into())
                    }
                    Err(err) => {
                        return Err(SetError::invalid_property(
                            field.to_string(),
                            err.to_string(),
                        ));
                    }
                }
            }
            (MailboxProperty::ParentId, JSONValue::Null) => Ok(0u64.into()),
            (MailboxProperty::Role, JSONValue::String(role)) => {
                let role = role.to_lowercase();
                if [
                    "inbox", "trash", "spam", "junk", "drafts", "archive", "sent",
                ]
                .contains(&role.as_str())
                {
                    self.mailbox.tag(field, Tag::Default);
                    Ok(role.into())
                } else {
                    Err(SetError::invalid_property(
                        field.to_string(),
                        "Invalid role.".to_string(),
                    ))
                }
            }
            (MailboxProperty::Role, JSONValue::Null) => {
                self.mailbox.untag(&field, &Tag::Default);
                Ok(value)
            }
            (MailboxProperty::SortOrder, JSONValue::Number(JSONNumber::PosInt(_))) => Ok(value),
            (_, _) => Err(SetError::invalid_property(
                field.to_string(),
                "Unexpected value.".to_string(),
            )),
        }?;

        self.mailbox.set(field, value);

        Ok(())
    }

    fn patch_field(
        &mut self,
        _helper: &mut SetObjectHelper<T, SetMailboxHelper>,
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
        _create_id: &str,
        document: &mut Document,
    ) -> jmap::error::set::Result<Self::CreateItemResult> {
        // Assign parentId if the field is missing
        if !self.mailbox.has_property(&MailboxProperty::ParentId) {
            self.mailbox.set(MailboxProperty::ParentId, 0u64.into());
        }

        self.validate(helper, None)?;
        TinyORM::default().merge_validate(document, self.mailbox)?;
        Ok(DefaultCreateItem::new(document.document_id as JMAPId))
    }

    fn update(
        self,
        helper: &mut SetObjectHelper<T, Self::Helper>,
        document: &mut Document,
    ) -> jmap::error::set::Result<Option<Self::UpdateItemResult>> {
        self.validate(helper, document.document_id.into())?;
        if self
            .current_mailbox
            .unwrap()
            .merge_validate(document, self.mailbox)?
        {
            Ok(Some(DefaultUpdateItem::default()))
        } else {
            Ok(None)
        }
    }

    fn delete(
        helper: &mut SetObjectHelper<T, SetMailboxHelper>,
        document: &mut Document,
    ) -> jmap::error::set::Result<()> {
        // Verify that this mailbox does not have sub-mailboxes
        if !helper
            .store
            .query_store::<DefaultIdMapper>(
                helper.account_id,
                Collection::Mailbox,
                Filter::new_condition(
                    MailboxProperty::ParentId.into(),
                    ComparisonOperator::Equal,
                    FieldValue::LongInteger((document.document_id + 1) as LongInteger),
                ),
                Comparator::None,
            )?
            .is_empty()
        {
            return Err(SetError::new(
                SetErrorType::MailboxHasChild,
                "Mailbox has at least one children.",
            ));
        }

        // Verify that the mailbox is empty
        if let Some(message_doc_ids) = helper.store.get_tag(
            helper.account_id,
            Collection::Mail,
            MessageField::Mailbox.into(),
            Tag::Id(document.document_id),
        )? {
            if !helper.data.on_destroy_remove_emails {
                return Err(SetError::new(
                    SetErrorType::MailboxHasEmail,
                    "Mailbox is not empty.",
                ));
            }

            // Fetch results
            let message_doc_ids = message_doc_ids.into_iter().collect::<Vec<_>>();

            // Obtain thread ids for all messages to be deleted
            //TODO
            /*for (thread_id, message_doc_id) in helper
                .store
                .get_multi_document_tag_id(
                    helper.account_id,
                    Collection::Mail,
                    message_doc_ids.iter().copied(),
                    MessageField::ThreadId.into(),
                )?
                .into_iter()
                .zip(message_doc_ids)
            {
                if let Some(thread_id) = thread_id {
                    helper
                        .changes
                        .delete_document(Collection::Mail, message_doc_id);
                    helper.changes.log_delete(
                        Collection::Mail,
                        JMAPId::from_parts(*thread_id, message_doc_id),
                    );
                }
            }*/
        }

        // Delete index
        if let Some(orm) = helper
            .store
            .get_orm::<MailboxProperty>(helper.account_id, document.document_id)?
        {
            orm.delete(document);
        }
        Ok(())
    }
}

impl SetMailbox {
    fn validate<T>(
        &self,
        helper: &mut SetObjectHelper<T, SetMailboxHelper>,
        mailbox_id: Option<DocumentId>,
    ) -> jmap::error::set::Result<()>
    where
        T: for<'x> Store<'x> + 'static,
    {
        if let (Some(mailbox_id), Some(mut mailbox_parent_id)) = (
            mailbox_id,
            self.mailbox.get_unsigned_int(&MailboxProperty::ParentId),
        ) {
            // Validate circular parent-child relationship
            let mut success = false;
            for _ in 0..helper.store.config.mailbox_max_depth {
                if mailbox_parent_id == (mailbox_id as JMAPId) + 1 {
                    return Err(SetError::new(
                        SetErrorType::InvalidProperties,
                        "Mailbox cannot be a parent of itself.",
                    ));
                } else if mailbox_parent_id == 0 {
                    success = true;
                    break;
                }

                mailbox_parent_id = helper
                    .store
                    .get_orm::<MailboxProperty>(
                        helper.account_id,
                        (mailbox_parent_id - 1).get_document_id(),
                    )?
                    .ok_or_else(|| StoreError::InternalError("Mailbox data not found".to_string()))?
                    .get_unsigned_int(&MailboxProperty::ParentId)
                    .unwrap_or(0);
            }

            if !success {
                return Err(SetError::new(
                    SetErrorType::InvalidProperties,
                    "Mailbox parent-child relationship is too deep.",
                ));
            }
        }

        // Verify that the mailbox role is unique.
        if let Some(mailbox_role) = self.mailbox.get_string(&MailboxProperty::Role) {
            if !helper
                .store
                .query_store::<DefaultIdMapper>(
                    helper.account_id,
                    Collection::Mailbox,
                    Filter::new_condition(
                        MailboxProperty::Role.into(),
                        ComparisonOperator::Equal,
                        FieldValue::Keyword(mailbox_role.into()),
                    ),
                    Comparator::None,
                )?
                .is_empty()
            {
                return Err(SetError::new(
                    SetErrorType::InvalidProperties,
                    format!("A mailbox with role '{}' already exists.", mailbox_role),
                ));
            }
        }

        // Verify that the mailbox name is unique.
        if let Some(mailbox_name) = self.mailbox.get_string(&MailboxProperty::Name) {
            // Obtain parent mailbox id
            if let Some(parent_mailbox_id) = if let Some(mailbox_parent_id) =
                &self.mailbox.get_unsigned_int(&MailboxProperty::ParentId)
            {
                (*mailbox_parent_id).into()
            } else if let Some(current_mailbox) = &self.current_mailbox {
                if current_mailbox.get_string(&MailboxProperty::Name) != Some(mailbox_name) {
                    current_mailbox
                        .get_unsigned_int(&MailboxProperty::ParentId)
                        .unwrap_or_default()
                        .into()
                } else {
                    None
                }
            } else {
                0.into()
            } {
                for jmap_id in helper.store.query_store::<DefaultIdMapper>(
                    helper.account_id,
                    Collection::Mailbox,
                    Filter::new_condition(
                        MailboxProperty::ParentId.into(),
                        ComparisonOperator::Equal,
                        FieldValue::LongInteger(parent_mailbox_id),
                    ),
                    Comparator::None,
                )? {
                    if helper
                        .store
                        .get_orm::<MailboxProperty>(helper.account_id, jmap_id.get_document_id())?
                        .ok_or_else(|| {
                            StoreError::InternalError("Mailbox data not found".to_string())
                        })?
                        .get_string(&MailboxProperty::Name)
                        == Some(mailbox_name)
                    {
                        return Err(SetError::new(
                            SetErrorType::InvalidProperties,
                            format!("A mailbox with name '{}' already exists.", mailbox_name),
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}
