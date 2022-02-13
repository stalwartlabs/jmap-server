use std::collections::HashMap;

use jmap_store::id::JMAPIdSerialize;
use jmap_store::JMAP_MAIL;
use jmap_store::{
    json::JSONValue, JMAPError, JMAPId, JMAPSet, JMAPSetErrorType, JMAPSetResponse, JMAP_MAILBOX,
};
use serde::{Deserialize, Serialize};
use store::field::{FieldOptions, Text};
use store::{
    batch::{DocumentWriter, LogAction},
    DocumentSet, Store,
};
use store::{
    AccountId, ChangeLogId, Comparator, ComparisonOperator, FieldId, FieldValue, Filter,
    LongInteger, StoreError, Tag, UncommittedDocumentId,
};

use crate::import::{bincode_deserialize, bincode_serialize};
use crate::MessageField;
use crate::{changes::JMAPMailLocalStoreChanges, JMAPMailIdImpl};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct JMAPMailbox {
    pub name: String,
    pub parent_id: JMAPId,
    pub role: Option<String>,
    pub sort_order: u32,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct JMAPMailboxSet {
    pub name: Option<String>,
    pub parent_id: Option<JMAPId>,
    pub role: Option<String>,
    pub sort_order: Option<u32>,
    pub is_subscribed: Option<bool>,
}

#[repr(u8)]
pub enum JMAPMailboxProperties {
    Name = 1,
    ParentId = 2,
    Role = 3,
    SortOrder = 4,
    IsSubscribed = 5,
}

pub const MAILBOX_FIELD_ID: u8 = 0;

impl From<JMAPMailboxProperties> for FieldId {
    fn from(field: JMAPMailboxProperties) -> Self {
        field as FieldId
    }
}

impl JMAPMailboxProperties {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "name" => Some(JMAPMailboxProperties::Name),
            "parentId" => Some(JMAPMailboxProperties::ParentId),
            "role" => Some(JMAPMailboxProperties::Role),
            "sortOrder" => Some(JMAPMailboxProperties::SortOrder),
            "isSubscribed" => Some(JMAPMailboxProperties::IsSubscribed),
            _ => None,
        }
    }
}

pub trait JMAPMailLocalStoreMailbox<'x>: Store<'x> + JMAPMailLocalStoreChanges<'x> {
    fn mailbox_set(
        &'x self,
        request: JMAPSet,
        remove_emails: bool,
    ) -> jmap_store::Result<JMAPSetResponse> {
        let old_state = self.get_state(request.account_id, JMAP_MAILBOX)?;
        if let Some(if_in_state) = request.if_in_state {
            if old_state != if_in_state {
                return Err(JMAPError::StateMismatch);
            }
        }
        let total_changes = request.create.to_object().map_or(0, |c| c.len())
            + request.update.to_object().map_or(0, |c| c.len())
            + request.destroy.to_array().map_or(0, |c| c.len());
        if total_changes > self.get_config().jmap_mail_options.mailbox_set_max_changes {
            return Err(JMAPError::RequestTooLarge);
        }

        let mut changes = Vec::with_capacity(total_changes);
        let mut response = JMAPSetResponse {
            old_state,
            ..Default::default()
        };
        let document_ids = self.get_document_ids(request.account_id, JMAP_MAILBOX)?;

        if let JSONValue::Object(create) = request.create {
            let mut created = HashMap::with_capacity(create.len());
            let mut not_created = HashMap::with_capacity(create.len());
            let mut assigned_id =
                self.assign_document_id(request.account_id, JMAP_MAILBOX, None)?;

            'create: for (pos, (create_id, properties)) in create.into_iter().enumerate() {
                if document_ids.len() + pos + 1
                    > self.get_config().jmap_mail_options.mailbox_max_total
                {
                    not_created.insert(
                        create_id,
                        JSONValue::new_error(
                            JMAPSetErrorType::Forbidden,
                            format!(
                                "Too many mailboxes (max {})",
                                self.get_config().jmap_mail_options.mailbox_max_total
                            ),
                        ),
                    );
                    continue;
                }

                let mailbox = match validate_properties(
                    self,
                    request.account_id,
                    None,
                    None,
                    properties,
                    &request.destroy,
                    self.get_config().jmap_mail_options.mailbox_max_depth,
                )? {
                    Ok(mailbox) => mailbox,
                    Err(err) => {
                        not_created.insert(create_id, err);
                        continue 'create;
                    }
                };

                let mailbox = JMAPMailbox {
                    name: mailbox.name.unwrap(),
                    parent_id: mailbox.parent_id.unwrap_or(0),
                    role: mailbox.role,
                    sort_order: mailbox.sort_order.unwrap_or(0),
                };

                assigned_id =
                    self.assign_document_id(request.account_id, JMAP_MAILBOX, assigned_id.into())?;
                let mut document = DocumentWriter::insert(JMAP_MAILBOX, assigned_id.clone());

                document.text(
                    JMAPMailboxProperties::Name,
                    Text::Tokenized(mailbox.name.to_lowercase().into()),
                    FieldOptions::Sort,
                );

                if let Some(mailbox_role) = mailbox.role.as_ref() {
                    document.text(
                        JMAPMailboxProperties::Role,
                        Text::Keyword(mailbox_role.clone().into()),
                        FieldOptions::None,
                    );
                }
                document.long_int(
                    JMAPMailboxProperties::ParentId,
                    mailbox.parent_id,
                    FieldOptions::Sort,
                );
                document.integer(
                    JMAPMailboxProperties::SortOrder,
                    mailbox.sort_order,
                    FieldOptions::Sort,
                );
                document.binary(
                    MAILBOX_FIELD_ID,
                    bincode_serialize(&mailbox)?.into(),
                    FieldOptions::Store,
                );
                let jmap_id = assigned_id.get_document_id() as JMAPId;
                document.log_insert(jmap_id);
                changes.push(document);

                // Generate JSON object
                let mut values: HashMap<String, JSONValue> = HashMap::new();
                values.insert("id".to_string(), jmap_id.to_jmap_string().into());
                created.insert(create_id, values.into());
            }

            if !created.is_empty() {
                response.created = created.into();
            }

            if !not_created.is_empty() {
                response.not_created = not_created.into();
            }
        }

        if let JSONValue::Object(update) = request.update {
            let mut updated = HashMap::with_capacity(update.len());
            let mut not_updated = HashMap::with_capacity(update.len());

            for (jmap_id_str, properties) in update {
                let jmap_id = if let Some(jmap_id) = JMAPId::from_jmap_string(&jmap_id_str) {
                    jmap_id
                } else {
                    not_updated.insert(
                        jmap_id_str,
                        JSONValue::new_error(
                            JMAPSetErrorType::InvalidProperties,
                            "Failed to parse request.",
                        ),
                    );
                    continue;
                };
                let document_id = jmap_id.get_document_id();
                if !document_ids.contains(document_id) {
                    not_updated.insert(
                        jmap_id_str,
                        JSONValue::new_error(JMAPSetErrorType::NotFound, "Mailbox ID not found."),
                    );
                    continue;
                } else if let JSONValue::Array(destroy_ids) = &request.destroy {
                    if destroy_ids
                        .iter()
                        .any(|x| x.to_string().map(|v| v == jmap_id_str).unwrap_or(false))
                    {
                        not_updated.insert(
                            jmap_id_str,
                            JSONValue::new_error(
                                JMAPSetErrorType::WillDestroy,
                                "ID will be destroyed.",
                            ),
                        );
                        continue;
                    }
                }

                let mut mailbox: JMAPMailbox = bincode_deserialize(
                    &self
                        .get_document_value::<Vec<u8>>(
                            request.account_id,
                            JMAP_MAILBOX,
                            document_id,
                            MAILBOX_FIELD_ID,
                        )?
                        .ok_or_else(|| {
                            StoreError::InternalError("Mailbox data not found".to_string())
                        })?,
                )?;

                let mailbox_changes = match validate_properties(
                    self,
                    request.account_id,
                    jmap_id.into(),
                    (&mailbox).into(),
                    properties,
                    &request.destroy,
                    self.get_config().jmap_mail_options.mailbox_max_depth,
                )? {
                    Ok(mailbox) => mailbox,
                    Err(err) => {
                        not_updated.insert(jmap_id_str, err);
                        continue;
                    }
                };

                let mut document = DocumentWriter::update(JMAP_MAILBOX, document_id);

                if let Some(new_name) = mailbox_changes.name {
                    if new_name != mailbox.name {
                        document.text(
                            JMAPMailboxProperties::Name,
                            Text::Tokenized(mailbox.name.to_lowercase().into()),
                            FieldOptions::Clear,
                        );
                        document.text(
                            JMAPMailboxProperties::Name,
                            Text::Tokenized(new_name.to_lowercase().into()),
                            FieldOptions::Sort,
                        );
                        mailbox.name = new_name;
                    }
                }

                if let Some(new_parent_id) = mailbox_changes.parent_id {
                    if new_parent_id != mailbox.parent_id {
                        document.long_int(
                            JMAPMailboxProperties::ParentId,
                            mailbox.parent_id,
                            FieldOptions::Clear,
                        );
                        document.long_int(
                            JMAPMailboxProperties::ParentId,
                            new_parent_id,
                            FieldOptions::Sort,
                        );
                        mailbox.parent_id = new_parent_id;
                    }
                }

                if let Some(new_role) = &mailbox_changes.role {
                    if mailbox_changes.role != mailbox.role {
                        if let Some(role) = mailbox.role {
                            document.text(
                                JMAPMailboxProperties::Role,
                                Text::Keyword(role.into()),
                                FieldOptions::Clear,
                            );
                        }
                        document.text(
                            JMAPMailboxProperties::Role,
                            Text::Keyword(new_role.clone().into()),
                            FieldOptions::None,
                        );
                        mailbox.role = mailbox_changes.role;
                    }
                }

                if let Some(new_sort_order) = mailbox_changes.sort_order {
                    if new_sort_order != mailbox.sort_order {
                        document.integer(
                            JMAPMailboxProperties::SortOrder,
                            mailbox.sort_order,
                            FieldOptions::Clear,
                        );
                        document.integer(
                            JMAPMailboxProperties::SortOrder,
                            new_sort_order,
                            FieldOptions::Sort,
                        );
                        mailbox.sort_order = new_sort_order;
                    }
                }

                if !document.is_empty() {
                    document.binary(
                        MAILBOX_FIELD_ID,
                        bincode_serialize(&mailbox)?.into(),
                        FieldOptions::Store,
                    );
                    document.log_update(document_id as ChangeLogId);
                    changes.push(document);
                }

                updated.insert(jmap_id_str, JSONValue::Null);
            }

            if !updated.is_empty() {
                response.updated = updated.into();
            }
            if !not_updated.is_empty() {
                response.not_updated = not_updated.into();
            }
        }

        if let JSONValue::Array(destroy_ids) = request.destroy {
            let mut destroyed = Vec::with_capacity(destroy_ids.len());
            let mut not_destroyed = HashMap::with_capacity(destroy_ids.len());

            for destroy_id in destroy_ids {
                if let JSONValue::String(destroy_id) = destroy_id {
                    if let Some(jmap_id) = JMAPId::from_jmap_string(&destroy_id) {
                        let document_id = jmap_id.get_document_id();
                        if document_ids.contains(document_id) {
                            // Verify that this mailbox does not have sub-mailboxes
                            if !self
                                .query(
                                    request.account_id,
                                    JMAP_MAILBOX,
                                    Filter::new_condition(
                                        JMAPMailboxProperties::ParentId.into(),
                                        ComparisonOperator::Equal,
                                        FieldValue::LongInteger(document_id as LongInteger),
                                    ),
                                    Comparator::None,
                                )?
                                .is_empty()
                            {
                                not_destroyed.insert(
                                    destroy_id,
                                    JSONValue::new_error(
                                        JMAPSetErrorType::MailboxHasChild,
                                        "Mailbox has at least one children.",
                                    ),
                                );
                                continue;
                            }

                            // Verify that the mailbox is empty
                            if let Some(message_doc_ids) = self.get_tag(
                                request.account_id,
                                JMAP_MAIL,
                                MessageField::Mailbox.into(),
                                Tag::Id(document_id),
                            )? {
                                if !remove_emails {
                                    not_destroyed.insert(
                                        destroy_id,
                                        JSONValue::new_error(
                                            JMAPSetErrorType::MailboxHasEmail,
                                            "Mailbox is not empty.",
                                        ),
                                    );
                                    continue;
                                }

                                // Fetch results
                                let message_doc_ids =
                                    message_doc_ids.into_iter().collect::<Vec<_>>();

                                // Obtain thread ids for all messages to be deleted
                                for (thread_id, message_doc_id) in self
                                    .get_multi_document_value(
                                        request.account_id,
                                        JMAP_MAIL,
                                        message_doc_ids.iter().copied(),
                                        MessageField::ThreadId.into(),
                                    )?
                                    .into_iter()
                                    .zip(message_doc_ids)
                                {
                                    if let Some(thread_id) = thread_id {
                                        changes.push(
                                            DocumentWriter::delete(JMAP_MAIL, message_doc_id).log(
                                                LogAction::Delete(JMAPId::from_email(
                                                    thread_id,
                                                    message_doc_id,
                                                )),
                                            ),
                                        );
                                    }
                                }
                            }

                            changes.push(
                                DocumentWriter::delete(JMAP_MAILBOX, document_id)
                                    .log(LogAction::Delete(jmap_id)),
                            );
                            destroyed.push(destroy_id.into());
                            continue;
                        }
                    }

                    not_destroyed.insert(
                        destroy_id,
                        JSONValue::new_error(JMAPSetErrorType::NotFound, "ID not found."),
                    );
                }
            }

            if !destroyed.is_empty() {
                response.destroyed = destroyed.into();
            }

            if !not_destroyed.is_empty() {
                response.not_destroyed = not_destroyed.into();
            }
        }

        if !changes.is_empty() {
            self.update_documents(request.account_id, changes, JMAP_MAILBOX.into())?;
            response.new_state = self.get_state(request.account_id, JMAP_MAILBOX)?;
        } else {
            response.new_state = response.old_state.clone();
        }

        Ok(response)
    }
}

#[allow(clippy::blocks_in_if_conditions)]
fn validate_properties<'x>(
    store: &'x impl Store<'x>,
    account_id: AccountId,
    mailbox_id: Option<JMAPId>,
    current_mailbox: Option<&JMAPMailbox>,
    properties: JSONValue,
    destroy_ids: &JSONValue,
    max_nest_level: usize,
) -> jmap_store::Result<Result<JMAPMailboxSet, JSONValue>> {
    let mut mailbox = JMAPMailboxSet::default();

    for (property, value) in if let Some(properties) = properties.unwrap_object() {
        properties
    } else {
        return Ok(Err(JSONValue::new_error(
            JMAPSetErrorType::InvalidProperties,
            "Failed to parse request, expected object.",
        )));
    } {
        match JMAPMailboxProperties::parse(&property) {
            Some(JMAPMailboxProperties::Name) => {
                mailbox.name = value.unwrap_string();
            }
            Some(JMAPMailboxProperties::ParentId) => match value {
                JSONValue::String(mailbox_parent_id_str) => {
                    if let JSONValue::Array(destroy_ids) = &destroy_ids {
                        if destroy_ids.iter().any(|x| {
                            x.to_string()
                                .map(|v| v == mailbox_parent_id_str)
                                .unwrap_or(false)
                        }) {
                            return Ok(Err(JSONValue::new_error(
                                JMAPSetErrorType::WillDestroy,
                                "Parent ID will be destroyed.",
                            )));
                        }
                    }
                    mailbox.parent_id =
                        JMAPId::from_jmap_string(&mailbox_parent_id_str).map(|x| x + 1);
                }
                JSONValue::Null => mailbox.parent_id = 0.into(),
                _ => {
                    return Ok(Err(JSONValue::new_invalid_property(
                        "parentId",
                        "Expected Null or String.",
                    )));
                }
            },
            Some(JMAPMailboxProperties::Role) => {
                mailbox.role = value.unwrap_string().map(|s| s.to_lowercase());
            }
            Some(JMAPMailboxProperties::SortOrder) => {
                mailbox.sort_order = value.unwrap_number().map(|x| x as u32);
            }
            Some(JMAPMailboxProperties::IsSubscribed) => {
                //TODO implement isSubscribed
                mailbox.is_subscribed = value.unwrap_bool();
            }
            None => {
                return Ok(Err(JSONValue::new_invalid_property(
                    property,
                    "Unknown property",
                )));
            }
        }
    }

    if let Some(mailbox_id) = mailbox_id {
        // Make sure this mailbox won't be destroyed later.
        if let JSONValue::Array(destroy_ids) = &destroy_ids {
            if destroy_ids
                .iter()
                .any(|x| x.to_jmap_id().map(|v| v == mailbox_id).unwrap_or(false))
            {
                return Ok(Err(JSONValue::new_error(
                    JMAPSetErrorType::WillDestroy,
                    "Mailbox will be destroyed.",
                )));
            }
        }

        if let Some(mut mailbox_parent_id) = mailbox.parent_id {
            // Validate circular parent-child relationship
            let mut success = false;
            for _ in 0..max_nest_level {
                if mailbox_parent_id == mailbox_id {
                    return Ok(Err(JSONValue::new_error(
                        JMAPSetErrorType::InvalidProperties,
                        "Mailbox cannot be a parent of itself.",
                    )));
                } else if mailbox_parent_id == 0 {
                    success = true;
                    break;
                }

                mailbox_parent_id = bincode_deserialize::<JMAPMailbox>(
                    &store
                        .get_document_value::<Vec<u8>>(
                            account_id,
                            JMAP_MAILBOX,
                            mailbox_parent_id.get_document_id(),
                            MAILBOX_FIELD_ID,
                        )?
                        .ok_or_else(|| {
                            StoreError::InternalError("Mailbox data not found".to_string())
                        })?,
                )?
                .parent_id;
            }

            if !success {
                return Ok(Err(JSONValue::new_error(
                    JMAPSetErrorType::InvalidProperties,
                    "Mailbox parent-child relationship is too deep.",
                )));
            }
        };
    } else if mailbox.name.is_none() {
        // New mailboxes need to specify a name
        return Ok(Err(JSONValue::new_error(
            JMAPSetErrorType::InvalidProperties,
            "Mailbox must have a name.",
        )));
    }

    // Verify that the mailbox role is unique.
    if let Some(mailbox_role) = &mailbox.role {
        if !store
            .query(
                account_id,
                JMAP_MAILBOX,
                Filter::new_condition(
                    JMAPMailboxProperties::Role.into(),
                    ComparisonOperator::Equal,
                    FieldValue::Keyword(mailbox_role.into()),
                ),
                Comparator::None,
            )?
            .is_empty()
        {
            return Ok(Err(JSONValue::new_error(
                JMAPSetErrorType::InvalidProperties,
                format!("A mailbox with role '{}' already exists.", mailbox_role),
            )));
        }
    }

    // Verify that the mailbox name is unique.
    if let Some(mailbox_name) = &mailbox.name {
        // Obtain parent mailbox id
        if let Some(parent_mailbox_id) = if let Some(mailbox_parent_id) = mailbox.parent_id {
            mailbox_parent_id.into()
        } else if let Some(current_mailbox) = current_mailbox {
            if &current_mailbox.name != mailbox_name {
                current_mailbox.parent_id.into()
            } else {
                None
            }
        } else {
            0.into()
        } {
            for document_id in store.query(
                account_id,
                JMAP_MAILBOX,
                Filter::new_condition(
                    JMAPMailboxProperties::ParentId.into(),
                    ComparisonOperator::Equal,
                    FieldValue::LongInteger(parent_mailbox_id),
                ),
                Comparator::None,
            )? {
                if &bincode_deserialize::<JMAPMailbox>(
                    &store
                        .get_document_value::<Vec<u8>>(
                            account_id,
                            JMAP_MAILBOX,
                            document_id,
                            MAILBOX_FIELD_ID,
                        )?
                        .ok_or_else(|| {
                            StoreError::InternalError("Mailbox data not found".to_string())
                        })?,
                )?
                .name
                    == mailbox_name
                {
                    return Ok(Err(JSONValue::new_error(
                        JMAPSetErrorType::InvalidProperties,
                        format!("A mailbox with name '{}' already exists.", mailbox_name),
                    )));
                }
            }
        }
    }

    Ok(Ok(mailbox))
}
