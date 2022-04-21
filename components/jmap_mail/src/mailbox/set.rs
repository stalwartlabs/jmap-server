use std::collections::HashMap;

use crate::mail::MessageField;
use jmap::error::set::SetErrorType;
use jmap::id::JMAPIdSerialize;
use jmap::jmap_store::set::{SetObject, SetObjectData, SetObjectHelper};
use jmap::protocol::json::JSONValue;
use jmap::request::set::SetRequest;
use store::batch::Document;
use store::field::{DefaultOptions, Options, Text};
use store::query::DefaultIdMapper;
use store::serialize::StoreSerialize;
use store::{batch::WriteBatch, Store};
use store::{
    AccountId, Collection, Comparator, ComparisonOperator, DocumentId, FieldValue, Filter, JMAPId,
    JMAPIdPrefix, JMAPStore, LongInteger, StoreError, Tag,
};

use super::{Mailbox, MailboxProperties};

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct SetMailbox {
    pub current_mailbox_id: Option<DocumentId>,
    pub current_mailbox: Option<Mailbox>,
    pub name: Option<String>,
    pub parent_id: Option<JMAPId>,
    pub role: Option<Option<String>>,
    pub sort_order: Option<u32>,
    pub is_subscribed: Option<bool>,
}

pub struct SetMailboxHelper {
    on_destroy_remove_emails: bool,
}

impl<T> SetObjectData<T> for SetMailboxHelper
where
    T: for<'x> Store<'x> + 'static,
{
    fn init(_store: &JMAPStore<T>, request: &SetRequest) -> jmap::Result<Self> {
        Ok(SetMailboxHelper {
            on_destroy_remove_emails: request
                .arguments
                .get("onDestroyRemoveEmails")
                .and_then(|v| v.to_bool())
                .unwrap_or(false),
        })
    }

    fn collection() -> Collection {
        Collection::Mailbox
    }
}

impl<'y, T> SetObject<'y, T> for SetMailbox
where
    T: for<'x> Store<'x> + 'static,
{
    type Property = MailboxProperties;
    type Helper = SetMailboxHelper;

    fn create(
        _helper: &mut SetObjectHelper<T, SetMailboxHelper>,
        _fields: &mut HashMap<String, JSONValue>,
    ) -> Result<Self, JSONValue> {
        Ok(SetMailbox {
            current_mailbox_id: None,
            current_mailbox: None,
            name: None,
            parent_id: None,
            role: None,
            sort_order: None,
            is_subscribed: None,
        })
    }

    fn update(
        helper: &mut SetObjectHelper<T, SetMailboxHelper>,
        _fields: &mut HashMap<String, JSONValue>,
        jmap_id: JMAPId,
    ) -> Result<Self, JSONValue> {
        let document_id = jmap_id.get_document_id();
        Ok(SetMailbox {
            current_mailbox_id: document_id.into(),
            current_mailbox: helper
                .store
                .get_document_value::<Mailbox>(
                    helper.account_id,
                    Collection::Mailbox,
                    document_id,
                    MailboxProperties::Id.into(),
                )
                .map_err(|_| JSONValue::store_error())?
                .ok_or_else(|| {
                    JSONValue::new_error(SetErrorType::NotFound, "Mailbox not found.".to_string())
                })?
                .into(),
            name: None,
            parent_id: None,
            role: None,
            sort_order: None,
            is_subscribed: None,
        })
    }

    fn set_field(
        &mut self,
        helper: &mut SetObjectHelper<T, SetMailboxHelper>,
        field: Self::Property,
        value: JSONValue,
    ) -> Result<(), JSONValue> {
        match field {
            MailboxProperties::Name => {
                self.name = value.unwrap_string();
            }
            MailboxProperties::ParentId => match value {
                JSONValue::String(mailbox_parent_id_str) => {
                    match helper.resolve_reference(&mailbox_parent_id_str) {
                        Ok(mailbox_parent_id) => {
                            if helper.will_destroy.contains(&mailbox_parent_id) {
                                return Err(JSONValue::new_error(
                                    SetErrorType::WillDestroy,
                                    "Parent ID will be destroyed.",
                                ));
                            } else if !helper
                                .document_ids
                                .contains(mailbox_parent_id as DocumentId)
                            {
                                return Err(JSONValue::new_error(
                                    SetErrorType::InvalidProperties,
                                    "Parent ID does not exist.",
                                ));
                            }
                            self.parent_id = (mailbox_parent_id + 1).into();
                        }
                        Err(err) => {
                            return Err(JSONValue::new_invalid_property(
                                "parentId",
                                err.to_string(),
                            ));
                        }
                    }
                }
                JSONValue::Null => self.parent_id = 0.into(),
                _ => {
                    return Err(JSONValue::new_invalid_property(
                        "parentId",
                        "Expected Null or String.",
                    ));
                }
            },
            MailboxProperties::Role => {
                self.role = match value {
                    JSONValue::Null => Some(None),
                    JSONValue::String(s) => Some(Some(s.to_lowercase())),
                    _ => None,
                };
            }
            MailboxProperties::SortOrder => {
                self.sort_order = value.unwrap_unsigned_int().map(|x| x as u32);
            }
            MailboxProperties::IsSubscribed => {
                //TODO implement isSubscribed
                self.is_subscribed = value.unwrap_bool();
            }
            field => {
                return Err(JSONValue::new_invalid_property(
                    field.to_string(),
                    "Field cannot be set.",
                ));
            }
        }
        Ok(())
    }

    fn patch_field(
        &mut self,
        _helper: &mut SetObjectHelper<T, SetMailboxHelper>,
        field: Self::Property,
        _property: String,
        _value: JSONValue,
    ) -> Result<(), JSONValue> {
        Err(JSONValue::new_invalid_property(
            field.to_string(),
            "Patch operations not supported on this field.",
        ))
    }

    fn write(
        self,
        helper: &mut SetObjectHelper<T, SetMailboxHelper>,
    ) -> jmap::Result<Result<Option<JSONValue>, JSONValue>> {
        if let Some(mailbox_id) = self.current_mailbox_id {
            if let Some(mut mailbox_parent_id) = self.parent_id {
                // Validate circular parent-child relationship
                let mut success = false;
                for _ in 0..helper.store.config.mailbox_max_depth {
                    if mailbox_parent_id == (mailbox_id as JMAPId) + 1 {
                        return Ok(Err(JSONValue::new_error(
                            SetErrorType::InvalidProperties,
                            "Mailbox cannot be a parent of itself.",
                        )));
                    } else if mailbox_parent_id == 0 {
                        success = true;
                        break;
                    }

                    mailbox_parent_id = helper
                        .store
                        .get_document_value::<Mailbox>(
                            helper.account_id,
                            Collection::Mailbox,
                            (mailbox_parent_id - 1).get_document_id(),
                            MailboxProperties::Id.into(),
                        )?
                        .ok_or_else(|| {
                            StoreError::InternalError("Mailbox data not found".to_string())
                        })?
                        .parent_id;
                }

                if !success {
                    return Ok(Err(JSONValue::new_error(
                        SetErrorType::InvalidProperties,
                        "Mailbox parent-child relationship is too deep.",
                    )));
                }
            };
        } else if self.name.is_none() {
            // New mailboxes need to specify a name
            return Ok(Err(JSONValue::new_error(
                SetErrorType::InvalidProperties,
                "Mailbox must have a name.",
            )));
        }

        // Verify that the mailbox role is unique.
        if let Some(mailbox_role) = self.role.as_ref().unwrap_or(&None) {
            let do_check = if let Some(current_mailbox) = &self.current_mailbox {
                if let Some(current_role) = &current_mailbox.role {
                    mailbox_role != current_role
                } else {
                    true
                }
            } else {
                true
            };

            if do_check
                && !helper
                    .store
                    .query_store::<DefaultIdMapper>(
                        helper.account_id,
                        Collection::Mailbox,
                        Filter::new_condition(
                            MailboxProperties::Role.into(),
                            ComparisonOperator::Equal,
                            FieldValue::Keyword(mailbox_role.into()),
                        ),
                        Comparator::None,
                    )?
                    .is_empty()
            {
                return Ok(Err(JSONValue::new_error(
                    SetErrorType::InvalidProperties,
                    format!("A mailbox with role '{}' already exists.", mailbox_role),
                )));
            }
        }

        // Verify that the mailbox name is unique.
        if let Some(mailbox_name) = &self.name {
            // Obtain parent mailbox id
            if let Some(parent_mailbox_id) = if let Some(mailbox_parent_id) = &self.parent_id {
                (*mailbox_parent_id).into()
            } else if let Some(current_mailbox) = &self.current_mailbox {
                if &current_mailbox.name != mailbox_name {
                    current_mailbox.parent_id.into()
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
                        MailboxProperties::ParentId.into(),
                        ComparisonOperator::Equal,
                        FieldValue::LongInteger(parent_mailbox_id),
                    ),
                    Comparator::None,
                )? {
                    if &helper
                        .store
                        .get_document_value::<Mailbox>(
                            helper.account_id,
                            Collection::Mailbox,
                            jmap_id.get_document_id(),
                            MailboxProperties::Id.into(),
                        )?
                        .ok_or_else(|| {
                            StoreError::InternalError("Mailbox data not found".to_string())
                        })?
                        .name
                        == mailbox_name
                    {
                        return Ok(Err(JSONValue::new_error(
                            SetErrorType::InvalidProperties,
                            format!("A mailbox with name '{}' already exists.", mailbox_name),
                        )));
                    }
                }
            }
        }

        if self.current_mailbox.is_some() {
            if let Some(document) = build_changed_mailbox_document(self)? {
                helper.changes.update_document(document);

                Ok(Ok(Some(JSONValue::Null)))
            } else {
                Ok(Ok(None))
            }
        } else {
            let assigned_id = helper
                .store
                .assign_document_id(helper.account_id, Collection::Mailbox)?;
            let jmap_id = assigned_id as JMAPId;

            helper.document_ids.insert(assigned_id);
            helper.changes.insert_document(build_mailbox_document(
                Mailbox {
                    name: self.name.unwrap(),
                    parent_id: self.parent_id.unwrap_or(0),
                    role: self.role.unwrap_or_default(),
                    sort_order: self.sort_order.unwrap_or(0),
                },
                assigned_id,
            )?);
            helper.changes.log_insert(Collection::Mailbox, jmap_id);

            // Generate JSON object
            let mut values: HashMap<String, JSONValue> = HashMap::new();
            values.insert("id".to_string(), jmap_id.to_jmap_string().into());

            Ok(Ok(Some(values.into())))
        }
    }

    fn delete(
        helper: &mut SetObjectHelper<T, SetMailboxHelper>,
        jmap_id: JMAPId,
    ) -> jmap::Result<Result<(), JSONValue>> {
        // Verify that this mailbox does not have sub-mailboxes
        let document_id = jmap_id.get_document_id();
        if !helper
            .store
            .query_store::<DefaultIdMapper>(
                helper.account_id,
                Collection::Mailbox,
                Filter::new_condition(
                    MailboxProperties::ParentId.into(),
                    ComparisonOperator::Equal,
                    FieldValue::LongInteger((document_id + 1) as LongInteger),
                ),
                Comparator::None,
            )?
            .is_empty()
        {
            return Ok(Err(JSONValue::new_error(
                SetErrorType::MailboxHasChild,
                "Mailbox has at least one children.",
            )));
        }

        // Verify that the mailbox is empty
        if let Some(message_doc_ids) = helper.store.get_tag(
            helper.account_id,
            Collection::Mail,
            MessageField::Mailbox.into(),
            Tag::Id(document_id),
        )? {
            if !helper.data.on_destroy_remove_emails {
                return Ok(Err(JSONValue::new_error(
                    SetErrorType::MailboxHasEmail,
                    "Mailbox is not empty.",
                )));
            }

            // Fetch results
            let message_doc_ids = message_doc_ids.into_iter().collect::<Vec<_>>();

            // Obtain thread ids for all messages to be deleted
            for (thread_id, message_doc_id) in helper
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
            }
        }

        Ok(Ok(()))
    }

    fn parse_property(property: &str) -> Option<Self::Property> {
        MailboxProperties::parse(property)
    }
}

pub trait JMAPMailMailboxSet {
    fn raft_update_mailbox(
        &self,
        batch: &mut WriteBatch,
        account_id: AccountId,
        document_id: DocumentId,
        mailbox: Mailbox,
    ) -> store::Result<()>;
}

//TODO mailbox id 0 is inbox and cannot be deleted
impl<T> JMAPMailMailboxSet for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn raft_update_mailbox(
        &self,
        batch: &mut WriteBatch,
        account_id: AccountId,
        document_id: DocumentId,
        mailbox: Mailbox,
    ) -> store::Result<()> {
        if let Some(current_mailbox) = self.get_document_value::<Mailbox>(
            account_id,
            Collection::Mailbox,
            document_id,
            MailboxProperties::Id.into(),
        )? {
            if let Some(document) = build_changed_mailbox_document(SetMailbox {
                current_mailbox: current_mailbox.into(),
                current_mailbox_id: document_id.into(),
                name: mailbox.name.into(),
                parent_id: mailbox.parent_id.into(),
                role: mailbox.role.into(),
                sort_order: mailbox.sort_order.into(),
                is_subscribed: true.into(), //TODO implement
            })? {
                batch.update_document(document);
            }
        } else {
            batch.insert_document(build_mailbox_document(mailbox, document_id)?);
        };

        Ok(())
    }
}

fn build_mailbox_document(mailbox: Mailbox, document_id: DocumentId) -> store::Result<Document> {
    let mut document = Document::new(Collection::Mailbox, document_id);

    document.text(
        MailboxProperties::Name,
        Text::tokenized(mailbox.name.clone()),
        DefaultOptions::new().sort(),
    );

    if let Some(mailbox_role) = mailbox.role.as_ref() {
        document.text(
            MailboxProperties::Role,
            Text::keyword(mailbox_role.clone()),
            DefaultOptions::new(),
        );
        document.tag(
            // TODO search by not empty, similarly to headers?
            MailboxProperties::HasRole,
            Tag::Static(0),
            DefaultOptions::new(),
        );
    }
    document.number(
        MailboxProperties::ParentId,
        mailbox.parent_id,
        DefaultOptions::new().sort(),
    );
    document.number(
        MailboxProperties::SortOrder,
        mailbox.sort_order,
        DefaultOptions::new().sort(),
    );
    document.binary(
        MailboxProperties::Id,
        mailbox.serialize().ok_or_else(|| {
            StoreError::SerializeError("Failed to serialize mailbox.".to_string())
        })?,
        DefaultOptions::new().store(),
    );
    Ok(document)
}

fn build_changed_mailbox_document(mailbox_changes: SetMailbox) -> store::Result<Option<Document>> {
    let mut mailbox = mailbox_changes.current_mailbox.unwrap();
    let document_id = mailbox_changes.current_mailbox_id.unwrap();
    let mut document = Document::new(Collection::Mailbox, document_id);

    if let Some(new_name) = mailbox_changes.name {
        if new_name != mailbox.name {
            document.text(
                MailboxProperties::Name,
                Text::tokenized(mailbox.name),
                DefaultOptions::new().sort().clear(),
            );
            document.text(
                MailboxProperties::Name,
                Text::tokenized(new_name.clone()),
                DefaultOptions::new().sort(),
            );
            mailbox.name = new_name;
        }
    }

    if let Some(new_parent_id) = mailbox_changes.parent_id {
        if new_parent_id != mailbox.parent_id {
            document.number(
                MailboxProperties::ParentId,
                mailbox.parent_id,
                DefaultOptions::new().sort().clear(),
            );
            document.number(
                MailboxProperties::ParentId,
                new_parent_id,
                DefaultOptions::new().sort(),
            );
            mailbox.parent_id = new_parent_id;
        }
    }

    if let Some(new_role) = mailbox_changes.role {
        if new_role != mailbox.role {
            let has_role = if let Some(role) = mailbox.role {
                document.text(
                    MailboxProperties::Role,
                    Text::keyword(role),
                    DefaultOptions::new().clear(),
                );
                true
            } else {
                false
            };
            if let Some(new_role) = &new_role {
                document.text(
                    MailboxProperties::Role,
                    Text::keyword(new_role.clone()),
                    DefaultOptions::new(),
                );
                if !has_role {
                    // New role was added, set tag.
                    document.tag(
                        MailboxProperties::HasRole,
                        Tag::Static(0),
                        DefaultOptions::new(),
                    );
                }
            } else if has_role {
                // Role was removed, clear tag.
                document.tag(
                    MailboxProperties::HasRole,
                    Tag::Static(0),
                    DefaultOptions::new().clear(),
                );
            }
            mailbox.role = new_role;
        }
    }

    if let Some(new_sort_order) = mailbox_changes.sort_order {
        if new_sort_order != mailbox.sort_order {
            document.number(
                MailboxProperties::SortOrder,
                mailbox.sort_order,
                DefaultOptions::new().sort().clear(),
            );
            document.number(
                MailboxProperties::SortOrder,
                new_sort_order,
                DefaultOptions::new().sort(),
            );
            mailbox.sort_order = new_sort_order;
        }
    }

    if !document.is_empty() {
        document.binary(
            MailboxProperties::Id,
            mailbox.serialize().ok_or_else(|| {
                StoreError::SerializeError("Failed to serialize mailbox.".to_string())
            })?,
            DefaultOptions::new().store(),
        );
        Ok(Some(document))
    } else {
        Ok(None)
    }
}
