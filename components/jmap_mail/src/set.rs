use std::borrow::Cow;
use std::collections::HashMap;

use jmap_store::json::JSONValue;
use jmap_store::JMAPIdSerialize;
use jmap_store::{
    json::JSONPointer, local_store::JMAPLocalStore, JMAPError, JMAPId, JMAPSet, JMAPSetError,
    JMAPSetErrorType, JMAPSetResponse, JMAP_MAIL, JMAP_MAILBOX,
};
use store::{
    batch::{DocumentWriter, LogAction},
    DocumentSet, Store,
};
use store::{Tag, UncommittedDocumentId};

use crate::import::{bincode_deserialize, bincode_serialize};
use crate::query::MailboxId;
use crate::{JMAPMailIdImpl, JMAPMailProperties, JMAPMailStoreSet, MessageField};

//TODO make configurable
pub const MAX_CHANGES: usize = 100;

fn build_message<'x, T>(
    document: &mut DocumentWriter<'x, T>,
    fields: HashMap<Cow<'x, str>, JSONValue<'x, JMAPMailProperties<'x>>>,
    mailbox_ids: &impl DocumentSet,
) -> Result<JSONValue<'x, JMAPMailProperties<'x>>, JMAPSetError>
where
    T: UncommittedDocumentId,
{
    let jmap_id = JMAPId::from_email(0, 0);

    document.log_insert(jmap_id);

    Ok(JSONValue::Null)
}

impl<'x, T> JMAPMailStoreSet<'x> for JMAPLocalStore<T>
where
    T: Store<'x>,
{
    fn mail_set(
        &self,
        request: JMAPSet<'x, JMAPMailProperties<'x>>,
    ) -> jmap_store::Result<JMAPSetResponse<'x, JMAPMailProperties<'x>>> {
        let old_state = self.get_state(request.account_id, JMAP_MAIL)?;
        if let Some(if_in_state) = request.if_in_state {
            if old_state != if_in_state {
                return Err(JMAPError::StateMismatch);
            }
        }

        let total_changes = request.create.as_ref().map_or(0, |c| c.len())
            + request.update.as_ref().map_or(0, |c| c.len())
            + request.destroy.as_ref().map_or(0, |c| c.len());
        if total_changes > MAX_CHANGES {
            return Err(JMAPError::RequestTooLarge);
        }

        let mut changes = Vec::with_capacity(total_changes);
        let mut response = JMAPSetResponse {
            old_state,
            ..Default::default()
        };
        let document_ids = self.store.get_document_ids(request.account_id, JMAP_MAIL)?;
        let mut mailbox_ids = None;

        if let Some(create) = request.create {
            let mut created = HashMap::with_capacity(create.len());
            let mut not_created = HashMap::with_capacity(create.len());
            let mut last_assigned_id = None;

            for (create_id, message_fields) in create {
                let document_id = self.store.assign_document_id(
                    request.account_id,
                    JMAP_MAIL,
                    last_assigned_id.clone(),
                )?;
                let mut document =
                    DocumentWriter::insert(request.account_id, JMAP_MAIL, document_id.clone());
                let mailbox_ids = if let Some(mailbox_ids) = &mailbox_ids {
                    mailbox_ids
                } else {
                    mailbox_ids = self
                        .store
                        .get_document_ids(request.account_id, JMAP_MAILBOX)?
                        .into();
                    mailbox_ids.as_ref().unwrap()
                };

                match build_message(&mut document, message_fields, mailbox_ids) {
                    Ok(values) => {
                        changes.push(document);
                        last_assigned_id = Some(document_id);
                        created.insert(create_id, values);
                    }
                    Err(err) => {
                        not_created.insert(create_id, err);
                    }
                }
            }

            if !created.is_empty() {
                response.created = created.into();
            }

            if !not_created.is_empty() {
                response.not_created = not_created.into();
            }
        }

        if let Some(update) = request.update {
            let mut updated = HashMap::with_capacity(update.len());
            let mut not_updated = HashMap::with_capacity(update.len());

            for (jmap_id, properties) in update {
                let document_id = jmap_id.get_document_id();
                if !document_ids.contains(document_id) {
                    not_updated.insert(jmap_id, JMAPSetError::new(JMAPSetErrorType::NotFound));
                    continue;
                } else if let Some(destroy_ids) = &request.destroy {
                    if destroy_ids.contains(&jmap_id) {
                        not_updated
                            .insert(jmap_id, JMAPSetError::new(JMAPSetErrorType::WillDestroy));
                        continue;
                    }
                }
                let mut document =
                    DocumentWriter::update(request.account_id, JMAP_MAIL, document_id);
                let mut invalid_properties = Vec::new();

                for (field, value) in properties {
                    match field {
                        JSONPointer::Property(JMAPMailProperties::Keywords) => {
                            if let JSONValue::Object(value) = value {
                                if let Some(current_keywords) =
                                    self.store.get_document_value::<Vec<u8>>(
                                        request.account_id,
                                        JMAP_MAIL,
                                        document_id,
                                        MessageField::Keyword.into(),
                                        0,
                                    )?
                                {
                                    for tag in bincode_deserialize::<Vec<Tag>>(&current_keywords)? {
                                        document.clear_tag(MessageField::Keyword.into(), tag);
                                    }
                                }
                                let mut new_keywords = Vec::with_capacity(value.len());
                                for (keyword, value) in value {
                                    if let JSONValue::Bool(true) = value {
                                        new_keywords.push(Tag::Text(keyword));
                                    }
                                }
                                document.add_blob(
                                    MessageField::Keyword.into(),
                                    0,
                                    bincode_serialize(&new_keywords)?.into(),
                                );
                            } else {
                                invalid_properties.push("keywords".to_string());
                            }
                        }
                        JSONPointer::Property(JMAPMailProperties::MailboxIds) => {
                            if let JSONValue::Object(value) = value {
                                if let Some(current_mailboxes) =
                                    self.store.get_document_value::<Vec<u8>>(
                                        request.account_id,
                                        JMAP_MAIL,
                                        document_id,
                                        MessageField::Keyword.into(),
                                        0,
                                    )?
                                {
                                    for mailbox_id in
                                        bincode_deserialize::<Vec<MailboxId>>(&current_mailboxes)?
                                    {
                                        document.clear_tag(
                                            MessageField::Mailbox.into(),
                                            Tag::Id(mailbox_id),
                                        );
                                    }
                                }

                                for (mailbox_id, value) in value {
                                    if let (Some(mailbox_id), JSONValue::Bool(true)) =
                                        (JMAPId::from_jmap_string(mailbox_id.as_ref()), value)
                                    {
                                        let mailbox_ids = if let Some(mailbox_ids) = &mailbox_ids {
                                            mailbox_ids
                                        } else {
                                            mailbox_ids = self
                                                .store
                                                .get_document_ids(request.account_id, JMAP_MAILBOX)?
                                                .into();
                                            mailbox_ids.as_ref().unwrap()
                                        };
                                        let mailbox_id = mailbox_id.get_document_id();
                                        if mailbox_ids.contains(mailbox_id) {
                                            document.set_tag(
                                                MessageField::Mailbox.into(),
                                                Tag::Id(mailbox_id),
                                            );
                                            continue;
                                        }
                                    }
                                    invalid_properties.push(format!("mailboxIds/{}", mailbox_id));
                                }
                            } else {
                                invalid_properties.push("mailboxIds".to_string());
                            }
                        }
                        JSONPointer::Path(mut path) if path.len() == 2 => {
                            match (path.pop().unwrap(), path.pop().unwrap()) {
                                (
                                    JSONPointer::String(keyword),
                                    JSONPointer::Property(JMAPMailProperties::Keywords),
                                ) => match value {
                                    JSONValue::Null | JSONValue::Bool(false) => {
                                        document.clear_tag(
                                            MessageField::Keyword.into(),
                                            Tag::Text(keyword),
                                        );
                                    }
                                    JSONValue::Bool(true) => {
                                        document.set_tag(
                                            MessageField::Keyword.into(),
                                            Tag::Text(keyword),
                                        );
                                    }
                                    _ => {
                                        invalid_properties.push(format!("keywords/{}", keyword));
                                    }
                                },
                                (
                                    JSONPointer::String(mailbox_id),
                                    JSONPointer::Property(JMAPMailProperties::MailboxIds),
                                ) => {
                                    if let Some(mailbox_id) =
                                        JMAPId::from_jmap_string(mailbox_id.as_ref())
                                    {
                                        let mailbox_ids = if let Some(mailbox_ids) = &mailbox_ids {
                                            mailbox_ids
                                        } else {
                                            mailbox_ids = self
                                                .store
                                                .get_document_ids(request.account_id, JMAP_MAILBOX)?
                                                .into();
                                            mailbox_ids.as_ref().unwrap()
                                        };
                                        if mailbox_ids.contains(mailbox_id.get_document_id()) {
                                            match value {
                                                JSONValue::Null | JSONValue::Bool(false) => {
                                                    document.clear_tag(
                                                        MessageField::Mailbox.into(),
                                                        Tag::Id(mailbox_id.get_document_id()),
                                                    );
                                                    continue;
                                                }
                                                JSONValue::Bool(true) => {
                                                    document.set_tag(
                                                        MessageField::Mailbox.into(),
                                                        Tag::Id(mailbox_id.get_document_id()),
                                                    );
                                                    continue;
                                                }
                                                _ => (),
                                            }
                                        }
                                    }
                                    invalid_properties.push(format!("mailboxIds/{}", mailbox_id));
                                }
                                (part2, part1) => {
                                    invalid_properties.push(format!("{}/{}", part1, part2));
                                }
                            }
                        }
                        _ => {
                            invalid_properties.push(field.to_string());
                        }
                    }
                }

                if !invalid_properties.is_empty() {
                    not_updated.insert(
                        jmap_id,
                        JMAPSetError {
                            error_type: JMAPSetErrorType::InvalidProperties,
                            description: None,
                            properties: invalid_properties.into(),
                        },
                    );
                } else if !document.is_empty() {
                    document.log_update(jmap_id);
                    changes.push(document);
                    updated.insert(jmap_id, JSONValue::Null);
                } else {
                    not_updated.insert(
                        jmap_id,
                        JMAPSetError {
                            error_type: JMAPSetErrorType::InvalidPatch,
                            description: "No changes found in request.".to_string().into(),
                            properties: None,
                        },
                    );
                }
            }

            if !updated.is_empty() {
                response.updated = Some(updated);
            }
            if !not_updated.is_empty() {
                response.not_updated = Some(not_updated);
            }
        }

        if let Some(destroy_ids) = request.destroy {
            let mut destroyed = Vec::with_capacity(destroy_ids.len());
            let mut not_destroyed = HashMap::with_capacity(destroy_ids.len());

            for destroy_id in destroy_ids {
                let document_id = destroy_id.get_document_id();
                if document_ids.contains(document_id) {
                    changes.push(
                        DocumentWriter::delete(request.account_id, JMAP_MAIL, document_id)
                            .log(LogAction::Delete(destroy_id)),
                    );
                    destroyed.push(destroy_id);
                } else {
                    not_destroyed.insert(
                        destroy_id,
                        JMAPSetError {
                            error_type: JMAPSetErrorType::NotFound,
                            description: None,
                            properties: None,
                        },
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
            self.store.update_documents(changes)?;
            response.new_state = self.get_state(request.account_id, JMAP_MAIL)?;
        } else {
            response.new_state = response.old_state.clone();
        }

        Ok(response)
    }
}
