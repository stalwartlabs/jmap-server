use std::collections::HashMap;

use jmap::changes::JMAPChanges;
use jmap::id::JMAPIdReference;
use jmap::id::JMAPIdSerialize;

use jmap::request::SetRequest;
use jmap::{json::JSONValue, JMAPError, SetErrorType};
use store::batch::Document;
use store::field::{DefaultOptions, Options, Text};
use store::query::{JMAPIdMapFnc, JMAPStoreQuery};
use store::roaring::RoaringBitmap;
use store::serialize::StoreSerialize;
use store::{batch::WriteBatch, Store};
use store::{
    AccountId, Collection, Comparator, ComparisonOperator, DocumentId, FieldValue, Filter, JMAPId,
    JMAPIdPrefix, JMAPStore, LongInteger, StoreError, Tag,
};

use crate::mail::MessageField;

use super::{Mailbox, MailboxChanges, MailboxProperties};

pub trait JMAPMailMailboxSet {
    fn mailbox_set(&self, request: SetRequest) -> jmap::Result<JSONValue>;

    #[allow(clippy::too_many_arguments)]
    fn validate_properties(
        &self,
        account_id: AccountId,
        mailbox_id: Option<JMAPId>,
        mailbox_ids: &RoaringBitmap,
        current_mailbox: Option<&Mailbox>,
        properties: JSONValue,
        destroy_ids: &[JSONValue],
        created_ids: &HashMap<String, JSONValue>,
        max_nest_level: usize,
    ) -> jmap::Result<Result<MailboxChanges, JSONValue>>;

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
    fn mailbox_set(&self, mut request: SetRequest) -> jmap::Result<JSONValue> {
        let old_state = self.get_state(request.account_id, Collection::Mailbox)?;
        if let Some(if_in_state) = request.if_in_state {
            if old_state != if_in_state {
                return Err(JMAPError::StateMismatch);
            }
        }

        let mut changes = WriteBatch::new(request.account_id, self.config.is_in_cluster);
        let mut response = HashMap::new();
        let mut document_ids = self
            .get_document_ids(request.account_id, Collection::Mailbox)?
            .unwrap_or_default();

        let mut created = HashMap::with_capacity(request.create.len());
        let mut not_created = HashMap::with_capacity(request.create.len());

        'create: for (pos, (create_id, properties)) in request.create.into_iter().enumerate() {
            if document_ids.len() as usize + pos + 1 > self.config.mailbox_max_total {
                not_created.insert(
                    create_id,
                    JSONValue::new_error(
                        SetErrorType::Forbidden,
                        format!("Too many mailboxes (max {})", self.config.mailbox_max_total),
                    ),
                );
                continue;
            }

            let mailbox = match self.validate_properties(
                request.account_id,
                None,
                &document_ids,
                None,
                properties,
                &request.destroy,
                &created,
                self.config.mailbox_max_depth,
            )? {
                Ok(mailbox) => mailbox,
                Err(err) => {
                    not_created.insert(create_id, err);
                    continue 'create;
                }
            };

            let assigned_id = self.assign_document_id(request.account_id, Collection::Mailbox)?;
            let jmap_id = assigned_id as JMAPId;
            document_ids.insert(assigned_id);

            changes.insert_document(build_mailbox_document(
                Mailbox {
                    name: mailbox.name.unwrap(),
                    parent_id: mailbox.parent_id.unwrap_or(0),
                    role: mailbox.role.unwrap_or_default(),
                    sort_order: mailbox.sort_order.unwrap_or(0),
                },
                assigned_id,
            )?);
            changes.log_insert(Collection::Mailbox, jmap_id);

            // Generate JSON object
            let mut values: HashMap<String, JSONValue> = HashMap::new();
            values.insert("id".to_string(), jmap_id.to_jmap_string().into());
            created.insert(create_id, values.into());
        }

        let mut updated = HashMap::with_capacity(request.update.len());
        let mut not_updated = HashMap::with_capacity(request.update.len());

        for (jmap_id_str, properties) in request.update {
            let jmap_id = if let Some(jmap_id) = JMAPId::from_jmap_string(&jmap_id_str) {
                jmap_id
            } else {
                not_updated.insert(
                    jmap_id_str,
                    JSONValue::new_error(
                        SetErrorType::InvalidProperties,
                        "Failed to parse request.",
                    ),
                );
                continue;
            };
            let document_id = jmap_id.get_document_id();
            if !document_ids.contains(document_id) {
                not_updated.insert(
                    jmap_id_str,
                    JSONValue::new_error(SetErrorType::NotFound, "Mailbox ID not found."),
                );
                continue;
            } else if request
                .destroy
                .iter()
                .any(|x| x.to_string().map(|v| v == jmap_id_str).unwrap_or(false))
            {
                not_updated.insert(
                    jmap_id_str,
                    JSONValue::new_error(SetErrorType::WillDestroy, "ID will be destroyed."),
                );
                continue;
            }

            let mailbox = self
                .get_document_value::<Mailbox>(
                    request.account_id,
                    Collection::Mailbox,
                    document_id,
                    MailboxProperties::Id.into(),
                )?
                .ok_or_else(|| StoreError::InternalError("Mailbox data not found".to_string()))?;

            let mailbox_changes = match self.validate_properties(
                request.account_id,
                jmap_id.into(),
                &document_ids,
                (&mailbox).into(),
                properties,
                &request.destroy,
                &created,
                self.config.mailbox_max_depth,
            )? {
                Ok(mailbox) => mailbox,
                Err(err) => {
                    not_updated.insert(jmap_id_str, err);
                    continue;
                }
            };

            if let Some(document) =
                build_changed_mailbox_document(mailbox, mailbox_changes, document_id)?
            {
                changes.update_document(document);
                changes.log_update(Collection::Mailbox, jmap_id);
            }
            updated.insert(jmap_id_str, JSONValue::Null);
        }

        let mut destroyed = Vec::with_capacity(request.destroy.len());
        let mut not_destroyed = HashMap::with_capacity(request.destroy.len());

        if !request.destroy.is_empty() {
            let remove_emails = request
                .arguments
                .remove("onDestroyRemoveEmails")
                .and_then(|v| v.unwrap_bool())
                .unwrap_or(false);

            for destroy_id in request.destroy {
                if let JSONValue::String(destroy_id) = destroy_id {
                    if let Some(jmap_id) = JMAPId::from_jmap_string(&destroy_id) {
                        let document_id = jmap_id.get_document_id();
                        if document_ids.contains(document_id) {
                            // Verify that this mailbox does not have sub-mailboxes
                            if !self
                                .query::<JMAPIdMapFnc>(JMAPStoreQuery::new(
                                    request.account_id,
                                    Collection::Mailbox,
                                    Filter::new_condition(
                                        MailboxProperties::ParentId.into(),
                                        ComparisonOperator::Equal,
                                        FieldValue::LongInteger((document_id + 1) as LongInteger),
                                    ),
                                    Comparator::None,
                                ))?
                                .is_empty()
                            {
                                not_destroyed.insert(
                                    destroy_id,
                                    JSONValue::new_error(
                                        SetErrorType::MailboxHasChild,
                                        "Mailbox has at least one children.",
                                    ),
                                );
                                continue;
                            }

                            // Verify that the mailbox is empty
                            if let Some(message_doc_ids) = self.get_tag(
                                request.account_id,
                                Collection::Mail,
                                MessageField::Mailbox.into(),
                                Tag::Id(document_id),
                            )? {
                                if !remove_emails {
                                    not_destroyed.insert(
                                        destroy_id,
                                        JSONValue::new_error(
                                            SetErrorType::MailboxHasEmail,
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
                                    .get_multi_document_tag_id(
                                        request.account_id,
                                        Collection::Mail,
                                        message_doc_ids.iter().copied(),
                                        MessageField::ThreadId.into(),
                                    )?
                                    .into_iter()
                                    .zip(message_doc_ids)
                                {
                                    if let Some(thread_id) = thread_id {
                                        changes.delete_document(Collection::Mail, message_doc_id);
                                        changes.log_delete(
                                            Collection::Mail,
                                            JMAPId::from_parts(*thread_id, message_doc_id),
                                        );
                                    }
                                }
                            }

                            changes.delete_document(Collection::Mailbox, document_id);
                            changes.log_delete(Collection::Mailbox, jmap_id);

                            destroyed.push(destroy_id.into());
                            continue;
                        }
                    }

                    not_destroyed.insert(
                        destroy_id,
                        JSONValue::new_error(SetErrorType::NotFound, "ID not found."),
                    );
                }
            }
        }

        response.insert("created".to_string(), created.into());
        response.insert("notCreated".to_string(), not_created.into());

        response.insert("updated".to_string(), updated.into());
        response.insert("notUpdated".to_string(), not_updated.into());

        response.insert("destroyed".to_string(), destroyed.into());
        response.insert("notDestroyed".to_string(), not_destroyed.into());

        response.insert(
            "newState".to_string(),
            if !changes.is_empty() {
                self.write(changes)?;
                self.get_state(request.account_id, Collection::Mailbox)?
            } else {
                old_state.clone()
            }
            .into(),
        );
        response.insert("oldState".to_string(), old_state.into());

        Ok(response.into())
    }

    #[allow(clippy::blocks_in_if_conditions)]
    #[allow(clippy::too_many_arguments)]
    fn validate_properties(
        &self,
        account_id: AccountId,
        mailbox_id: Option<JMAPId>,
        mailbox_ids: &RoaringBitmap,
        current_mailbox: Option<&Mailbox>,
        properties: JSONValue,
        destroy_ids: &[JSONValue],
        created_ids: &HashMap<String, JSONValue>,
        max_nest_level: usize,
    ) -> jmap::Result<Result<MailboxChanges, JSONValue>> {
        let mut mailbox = MailboxChanges::default();

        for (property, value) in if let Some(properties) = properties.unwrap_object() {
            properties
        } else {
            return Ok(Err(JSONValue::new_error(
                SetErrorType::InvalidProperties,
                "Failed to parse request, expected object.",
            )));
        } {
            match MailboxProperties::parse(&property) {
                Some(MailboxProperties::Name) => {
                    mailbox.name = value.unwrap_string();
                }
                Some(MailboxProperties::ParentId) => match value {
                    JSONValue::String(mailbox_parent_id_str) => {
                        if destroy_ids.iter().any(|x| {
                            x.to_string()
                                .map(|v| v == mailbox_parent_id_str)
                                .unwrap_or(false)
                        }) {
                            return Ok(Err(JSONValue::new_error(
                                SetErrorType::WillDestroy,
                                "Parent ID will be destroyed.",
                            )));
                        }

                        match JMAPId::from_jmap_ref(&mailbox_parent_id_str, created_ids) {
                            Ok(mailbox_parent_id) => {
                                if !mailbox_ids.contains(mailbox_parent_id as DocumentId) {
                                    return Ok(Err(JSONValue::new_error(
                                        SetErrorType::InvalidProperties,
                                        "Parent ID does not exist.",
                                    )));
                                }
                                mailbox.parent_id = (mailbox_parent_id + 1).into();
                            }
                            Err(err) => {
                                return Ok(Err(JSONValue::new_invalid_property(
                                    "parentId",
                                    err.to_string(),
                                )));
                            }
                        }
                    }
                    JSONValue::Null => mailbox.parent_id = 0.into(),
                    _ => {
                        return Ok(Err(JSONValue::new_invalid_property(
                            "parentId",
                            "Expected Null or String.",
                        )));
                    }
                },
                Some(MailboxProperties::Role) => {
                    mailbox.role = match value {
                        JSONValue::Null => Some(None),
                        JSONValue::String(s) => Some(Some(s.to_lowercase())),
                        _ => None,
                    };
                }
                Some(MailboxProperties::SortOrder) => {
                    mailbox.sort_order = value.unwrap_unsigned_int().map(|x| x as u32);
                }
                Some(MailboxProperties::IsSubscribed) => {
                    //TODO implement isSubscribed
                    mailbox.is_subscribed = value.unwrap_bool();
                }
                _ => {
                    return Ok(Err(JSONValue::new_invalid_property(
                        property,
                        "Unknown property",
                    )));
                }
            }
        }

        if let Some(mailbox_id) = mailbox_id {
            // Make sure this mailbox won't be destroyed later.
            if destroy_ids
                .iter()
                .any(|x| x.to_jmap_id().map(|v| v == mailbox_id).unwrap_or(false))
            {
                return Ok(Err(JSONValue::new_error(
                    SetErrorType::WillDestroy,
                    "Mailbox will be destroyed.",
                )));
            }

            if let Some(mut mailbox_parent_id) = mailbox.parent_id {
                // Validate circular parent-child relationship
                let mut success = false;
                for _ in 0..max_nest_level {
                    if mailbox_parent_id == mailbox_id + 1 {
                        return Ok(Err(JSONValue::new_error(
                            SetErrorType::InvalidProperties,
                            "Mailbox cannot be a parent of itself.",
                        )));
                    } else if mailbox_parent_id == 0 {
                        success = true;
                        break;
                    }

                    mailbox_parent_id = self
                        .get_document_value::<Mailbox>(
                            account_id,
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
        } else if mailbox.name.is_none() {
            // New mailboxes need to specify a name
            return Ok(Err(JSONValue::new_error(
                SetErrorType::InvalidProperties,
                "Mailbox must have a name.",
            )));
        }

        // Verify that the mailbox role is unique.
        if let Some(mailbox_role) = mailbox.role.as_ref().unwrap_or(&None) {
            let do_check = if let Some(current_mailbox) = current_mailbox {
                if let Some(current_role) = &current_mailbox.role {
                    mailbox_role != current_role
                } else {
                    true
                }
            } else {
                true
            };

            if do_check
                && !self
                    .query::<JMAPIdMapFnc>(JMAPStoreQuery::new(
                        account_id,
                        Collection::Mailbox,
                        Filter::new_condition(
                            MailboxProperties::Role.into(),
                            ComparisonOperator::Equal,
                            FieldValue::Keyword(mailbox_role.into()),
                        ),
                        Comparator::None,
                    ))?
                    .is_empty()
            {
                return Ok(Err(JSONValue::new_error(
                    SetErrorType::InvalidProperties,
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
                for jmap_id in self.query::<JMAPIdMapFnc>(JMAPStoreQuery::new(
                    account_id,
                    Collection::Mailbox,
                    Filter::new_condition(
                        MailboxProperties::ParentId.into(),
                        ComparisonOperator::Equal,
                        FieldValue::LongInteger(parent_mailbox_id),
                    ),
                    Comparator::None,
                ))? {
                    if &self
                        .get_document_value::<Mailbox>(
                            account_id,
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

        Ok(Ok(mailbox))
    }

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
            if let Some(document) = build_changed_mailbox_document(
                current_mailbox,
                MailboxChanges {
                    name: mailbox.name.into(),
                    parent_id: mailbox.parent_id.into(),
                    role: mailbox.role.into(),
                    sort_order: mailbox.sort_order.into(),
                    is_subscribed: true.into(), //TODO implement
                },
                document_id,
            )? {
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

fn build_changed_mailbox_document(
    mut mailbox: Mailbox,
    mailbox_changes: MailboxChanges,
    document_id: DocumentId,
) -> store::Result<Option<Document>> {
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
