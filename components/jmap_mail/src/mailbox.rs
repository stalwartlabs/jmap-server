use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;

use jmap_store::changes::JMAPLocalChanges;
use jmap_store::id::JMAPIdSerialize;
use jmap_store::local_store::JMAPLocalStore;
use jmap_store::query::{build_query, paginate_results};
use jmap_store::{
    json::JSONValue, JMAPError, JMAPId, JMAPSet, JMAPSetErrorType, JMAPSetResponse, JMAP_MAILBOX,
};
use jmap_store::{JMAPGet, JMAPGetResponse, JMAPQuery, JMAPQueryResponse, JMAP_MAIL};
use serde::{Deserialize, Serialize};
use store::field::{FieldOptions, Text};
use store::{
    batch::{DocumentWriter, LogAction},
    DocumentSet, Store,
};
use store::{
    AccountId, ChangeLogId, Comparator, ComparisonOperator, DocumentId, DocumentSetBitOps,
    FieldComparator, FieldId, FieldValue, Filter, LongInteger, StoreError, Tag,
    UncommittedDocumentId,
};

use crate::import::{bincode_deserialize, bincode_serialize};
use crate::MessageField;
use crate::{JMAPMailIdImpl, JMAPMailMailbox};

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
    pub role: Option<Option<String>>,
    pub sort_order: Option<u32>,
    pub is_subscribed: Option<bool>,
}

pub struct JMAPMailMailboxSetArguments {
    pub remove_emails: bool,
}

pub enum JMAPMailMailboxFilterCondition {
    ParentId(JMAPId),
    Name(String),
    Role(String),
    HasAnyRole,
    IsSubscribed,
}

pub enum JMAPMailMailboxComparator {
    Name,
    Role,
    ParentId,
}

pub struct JMAPMailMailboxQueryArguments {
    pub sort_as_tree: bool,
    pub filter_as_tree: bool,
}

#[repr(u8)]
pub enum JMAPMailboxProperties {
    Id = 0,
    Name = 1,
    ParentId = 2,
    Role = 3,
    SortOrder = 4,
    IsSubscribed = 5,
    TotalEmails = 6,
    UnreadEmails = 7,
    TotalThreads = 8,
    UnreadThreads = 9,
    MyRights = 10,
}

impl JMAPMailboxProperties {
    pub fn as_str(&self) -> &'static str {
        match self {
            JMAPMailboxProperties::Id => "id",
            JMAPMailboxProperties::Name => "name",
            JMAPMailboxProperties::ParentId => "parentId",
            JMAPMailboxProperties::Role => "role",
            JMAPMailboxProperties::SortOrder => "sortOrder",
            JMAPMailboxProperties::IsSubscribed => "isSubscribed",
            JMAPMailboxProperties::TotalEmails => "totalEmails",
            JMAPMailboxProperties::UnreadEmails => "unreadEmails",
            JMAPMailboxProperties::TotalThreads => "totalThreads",
            JMAPMailboxProperties::UnreadThreads => "unreadThreads",
            JMAPMailboxProperties::MyRights => "myRights",
        }
    }
}

impl Display for JMAPMailboxProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<JMAPMailboxProperties> for FieldId {
    fn from(field: JMAPMailboxProperties) -> Self {
        field as FieldId
    }
}

impl JMAPMailboxProperties {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "id" => Some(JMAPMailboxProperties::Id),
            "name" => Some(JMAPMailboxProperties::Name),
            "parentId" => Some(JMAPMailboxProperties::ParentId),
            "role" => Some(JMAPMailboxProperties::Role),
            "sortOrder" => Some(JMAPMailboxProperties::SortOrder),
            "isSubscribed" => Some(JMAPMailboxProperties::IsSubscribed),
            "totalEmails" => Some(JMAPMailboxProperties::TotalEmails),
            "unreadEmails" => Some(JMAPMailboxProperties::UnreadEmails),
            "totalThreads" => Some(JMAPMailboxProperties::TotalThreads),
            "unreadThreads" => Some(JMAPMailboxProperties::UnreadThreads),
            "myRights" => Some(JMAPMailboxProperties::MyRights),
            _ => None,
        }
    }
}

impl<'x, T> JMAPMailMailbox<'x> for JMAPLocalStore<T>
where
    T: Store<'x>,
{
    fn mailbox_set(
        &'x self,
        request: JMAPSet<JMAPMailMailboxSetArguments>,
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
        if total_changes > self.mail_config.mailbox_set_max_changes {
            return Err(JMAPError::RequestTooLarge);
        }

        let mut changes = Vec::with_capacity(total_changes);
        let mut response = JMAPSetResponse {
            old_state,
            ..Default::default()
        };
        let document_ids = self
            .store
            .get_document_ids(request.account_id, JMAP_MAILBOX)?;

        if let JSONValue::Object(create) = request.create {
            let mut created = HashMap::with_capacity(create.len());
            let mut not_created = HashMap::with_capacity(create.len());

            'create: for (pos, (create_id, properties)) in create.into_iter().enumerate() {
                if document_ids.len() + pos + 1 > self.mail_config.mailbox_max_total {
                    not_created.insert(
                        create_id,
                        JSONValue::new_error(
                            JMAPSetErrorType::Forbidden,
                            format!(
                                "Too many mailboxes (max {})",
                                self.mail_config.mailbox_max_total
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
                    self.mail_config.mailbox_max_depth,
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
                    role: mailbox.role.unwrap_or_default(),
                    sort_order: mailbox.sort_order.unwrap_or(0),
                };

                let assigned_id = self
                    .store
                    .assign_document_id(request.account_id, JMAP_MAILBOX)?;
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
                    document.tag(
                        JMAPMailboxProperties::Role,
                        Tag::Static(0),
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
                    JMAPMailboxProperties::Id,
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
                        .store
                        .get_document_value::<Vec<u8>>(
                            request.account_id,
                            JMAP_MAILBOX,
                            document_id,
                            JMAPMailboxProperties::Id.into(),
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
                    self.mail_config.mailbox_max_depth,
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

                if let Some(new_role) = mailbox_changes.role {
                    if new_role != mailbox.role {
                        let has_role = if let Some(role) = mailbox.role {
                            document.text(
                                JMAPMailboxProperties::Role,
                                Text::Keyword(role.into()),
                                FieldOptions::Clear,
                            );
                            true
                        } else {
                            false
                        };
                        if let Some(new_role) = &new_role {
                            document.text(
                                JMAPMailboxProperties::Role,
                                Text::Keyword(new_role.clone().into()),
                                FieldOptions::None,
                            );
                            if !has_role {
                                // New role was added, set tag.
                                document.tag(
                                    JMAPMailboxProperties::Role,
                                    Tag::Static(0),
                                    FieldOptions::None,
                                );
                            }
                        } else if has_role {
                            // Role was removed, clear tag.
                            document.tag(
                                JMAPMailboxProperties::Role,
                                Tag::Static(0),
                                FieldOptions::Clear,
                            );
                        }
                        mailbox.role = new_role;
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
                        JMAPMailboxProperties::Id,
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
                                .store
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
                            if let Some(message_doc_ids) = self.store.get_tag(
                                request.account_id,
                                JMAP_MAIL,
                                MessageField::Mailbox.into(),
                                Tag::Id(document_id),
                            )? {
                                if !request.arguments.remove_emails {
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
                                    .store
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
            self.store.update_documents(request.account_id, changes)?;
            response.new_state = self.get_state(request.account_id, JMAP_MAILBOX)?;
        } else {
            response.new_state = response.old_state.clone();
        }

        Ok(response)
    }

    fn mailbox_get(
        &'x self,
        request: JMAPGet<JMAPMailboxProperties, ()>,
    ) -> jmap_store::Result<jmap_store::JMAPGetResponse> {
        let properties = request.properties.unwrap_or_else(|| {
            vec![
                JMAPMailboxProperties::Id,
                JMAPMailboxProperties::Name,
                JMAPMailboxProperties::ParentId,
                JMAPMailboxProperties::Role,
                JMAPMailboxProperties::SortOrder,
                JMAPMailboxProperties::IsSubscribed,
                JMAPMailboxProperties::TotalEmails,
                JMAPMailboxProperties::UnreadEmails,
                JMAPMailboxProperties::TotalThreads,
                JMAPMailboxProperties::UnreadThreads,
                JMAPMailboxProperties::MyRights,
            ]
        });

        let request_ids = if let Some(request_ids) = request.ids {
            if request_ids.len() > self.mail_config.get_max_results {
                return Err(JMAPError::RequestTooLarge);
            } else {
                request_ids
            }
        } else {
            self.store
                .get_document_ids(request.account_id, JMAP_MAILBOX)?
                .into_iter()
                .take(self.mail_config.get_max_results)
                .map(|id| id as JMAPId)
                .collect::<Vec<JMAPId>>()
        };

        let document_ids = self.store.get_document_ids(request.account_id, JMAP_MAIL)?;
        let mut not_found = Vec::new();
        let mut results = Vec::with_capacity(request_ids.len());

        for jmap_id in request_ids {
            let document_id = jmap_id.get_document_id();
            if !document_ids.contains(document_id) {
                not_found.push(jmap_id);
                continue;
            }
            let mut mailbox: JMAPMailbox = bincode_deserialize(
                &self
                    .store
                    .get_document_value::<Vec<u8>>(
                        request.account_id,
                        JMAP_MAILBOX,
                        document_id,
                        JMAPMailboxProperties::Id.into(),
                    )?
                    .ok_or_else(|| {
                        StoreError::InternalError("Mailbox data not found".to_string())
                    })?,
            )?;

            let mut result: HashMap<String, JSONValue> = HashMap::new();

            for property in &properties {
                if let Entry::Vacant(entry) = result.entry(property.to_string()) {
                    let value = match property {
                        JMAPMailboxProperties::Id => jmap_id.to_jmap_string().into(),
                        JMAPMailboxProperties::Name => std::mem::take(&mut mailbox.name).into(),
                        JMAPMailboxProperties::ParentId => {
                            if mailbox.parent_id > 0 {
                                (mailbox.parent_id - 1).to_jmap_string().into()
                            } else {
                                JSONValue::Null
                            }
                        }
                        JMAPMailboxProperties::Role => std::mem::take(&mut mailbox.role)
                            .map(|v| v.into())
                            .unwrap_or_default(),
                        JMAPMailboxProperties::SortOrder => mailbox.sort_order.into(),
                        JMAPMailboxProperties::IsSubscribed => true.into(), //TODO implement
                        JMAPMailboxProperties::MyRights => JSONValue::Null, //TODO implement
                        JMAPMailboxProperties::TotalEmails => {
                            get_mailbox_tag(self, request.account_id, document_id)?
                                .map(|v| v.len())
                                .unwrap_or(0)
                                .into()
                        }
                        JMAPMailboxProperties::UnreadEmails => {
                            get_mailbox_unread_tag(self, request.account_id, document_id)?
                                .map(|v| v.len())
                                .unwrap_or(0)
                                .into()
                        }
                        JMAPMailboxProperties::TotalThreads => count_threads(
                            self,
                            request.account_id,
                            get_mailbox_tag(self, request.account_id, document_id)?,
                        )?
                        .into(),
                        JMAPMailboxProperties::UnreadThreads => count_threads(
                            self,
                            request.account_id,
                            get_mailbox_unread_tag(self, request.account_id, document_id)?,
                        )?
                        .into(),
                    };

                    entry.insert(value);
                }
            }

            results.push(result.into());
        }

        Ok(JMAPGetResponse {
            state: self.get_state(request.account_id, JMAP_MAILBOX)?,
            list: if !results.is_empty() {
                JSONValue::Array(results)
            } else {
                JSONValue::Null
            },
            not_found: if not_found.is_empty() {
                None
            } else {
                not_found.into()
            },
        })
    }

    fn mailbox_query(
        &'x self,
        request: JMAPQuery<
            JMAPMailMailboxFilterCondition,
            JMAPMailMailboxComparator,
            JMAPMailMailboxQueryArguments,
        >,
    ) -> jmap_store::Result<JMAPQueryResponse> {
        let mut doc_ids = build_query(
            &self.store,
            request.account_id,
            JMAP_MAILBOX,
            request.filter,
            request.sort,
            |cond| {
                Ok(match cond {
                    JMAPMailMailboxFilterCondition::ParentId(parent_id) => Filter::eq(
                        JMAPMailboxProperties::ParentId.into(),
                        FieldValue::LongInteger(parent_id),
                    ),
                    JMAPMailMailboxFilterCondition::Name(text) => Filter::eq(
                        JMAPMailboxProperties::Name.into(),
                        FieldValue::Text(text.to_lowercase()),
                    ),
                    JMAPMailMailboxFilterCondition::Role(text) => {
                        Filter::eq(JMAPMailboxProperties::Role.into(), FieldValue::Text(text))
                    }
                    JMAPMailMailboxFilterCondition::HasAnyRole => Filter::eq(
                        JMAPMailboxProperties::Role.into(),
                        FieldValue::Tag(Tag::Id(0)),
                    ),
                    JMAPMailMailboxFilterCondition::IsSubscribed => todo!(), //TODO implement
                })
            },
            |comp| {
                Ok(Comparator::Field(FieldComparator {
                    field: match comp.property {
                        JMAPMailMailboxComparator::Name => JMAPMailboxProperties::Name,
                        JMAPMailMailboxComparator::Role => JMAPMailboxProperties::Role,
                        JMAPMailMailboxComparator::ParentId => JMAPMailboxProperties::ParentId,
                    }
                    .into(),
                    ascending: comp.is_ascending,
                }))
            },
        )?
        .into_iter()
        .collect::<Vec<DocumentId>>();

        let query_state = self.get_state(request.account_id, JMAP_MAILBOX)?;
        let num_results = doc_ids.len();

        if num_results > 0 && (request.arguments.filter_as_tree || request.arguments.sort_as_tree) {
            let mut hierarchy = HashMap::new();
            let mut tree = HashMap::new();

            for doc_id in self
                .store
                .get_document_ids(request.account_id, JMAP_MAILBOX)?
            {
                let mailbox: JMAPMailbox = bincode_deserialize(
                    &self
                        .store
                        .get_document_value::<Vec<u8>>(
                            request.account_id,
                            JMAP_MAILBOX,
                            doc_id,
                            JMAPMailboxProperties::Id.into(),
                        )?
                        .ok_or_else(|| {
                            StoreError::InternalError("Mailbox data not found".to_string())
                        })?,
                )?;
                hierarchy.insert(doc_id as JMAPId, mailbox.parent_id);
                tree.entry(mailbox.parent_id)
                    .or_insert_with(HashSet::new)
                    .insert(doc_id as JMAPId);
            }

            if request.arguments.filter_as_tree {
                let mut filtered_ids = Vec::with_capacity(doc_ids.len());

                for &doc_id in &doc_ids {
                    let mut keep = false;
                    let mut jmap_id = doc_id as JMAPId;

                    for _ in 0..self.mail_config.mailbox_max_depth {
                        if let Some(&parent_id) = hierarchy.get(&jmap_id) {
                            if parent_id == 0 {
                                keep = true;
                                break;
                            } else if !doc_ids.contains(&((parent_id - 1) as DocumentId)) {
                                break;
                            } else {
                                jmap_id = parent_id;
                            }
                        } else {
                            break;
                        }
                    }
                    if keep {
                        filtered_ids.push(doc_id);
                    }
                }
                if filtered_ids.len() != doc_ids.len() {
                    doc_ids = filtered_ids;
                }
            }

            if request.arguments.sort_as_tree && doc_ids.len() > 1 {
                let mut stack = Vec::new();
                let mut sorted_list = Vec::with_capacity(doc_ids.len());
                let mut jmap_id = 0;

                'outer: for _ in 0..(doc_ids.len() * 10 * self.mail_config.mailbox_max_depth) {
                    let (children, mut it) = if let Some(children) = tree.remove(&jmap_id) {
                        (children, doc_ids.iter())
                    } else if let Some(prev) = stack.pop() {
                        prev
                    } else {
                        break;
                    };

                    while let Some(&doc_id) = it.next() {
                        jmap_id = doc_id as JMAPId;
                        if children.contains(&jmap_id) {
                            sorted_list.push(doc_id);
                            if sorted_list.len() == doc_ids.len() {
                                break 'outer;
                            } else {
                                stack.push((children, it));
                                continue 'outer;
                            }
                        }
                    }
                }
                doc_ids = sorted_list;
            }
        }

        let (results, start_position) = paginate_results(
            doc_ids.into_iter(),
            num_results,
            request.limit,
            request.position,
            request.anchor,
            request.anchor_offset,
            false,
            None::<fn(DocumentId) -> jmap_store::Result<Option<JMAPId>>>,
            None::<fn(Vec<DocumentId>) -> jmap_store::Result<Vec<JMAPId>>>,
        )?;

        Ok(JMAPQueryResponse {
            account_id: request.account_id,
            include_total: request.calculate_total,
            query_state,
            position: start_position,
            total: num_results,
            limit: request.limit,
            ids: results,
            is_immutable: false,
        })
    }
}

fn count_threads<'x, T>(
    store: &'x JMAPLocalStore<T>,
    account_id: AccountId,
    document_ids: Option<T::Set>,
) -> store::Result<usize>
where
    T: Store<'x>,
{
    Ok(if let Some(document_ids) = document_ids {
        let mut thread_ids = HashSet::new();
        store
            .store
            .get_multi_document_value(
                account_id,
                JMAP_MAIL,
                document_ids.into_iter(),
                MessageField::ThreadId.into(),
            )?
            .into_iter()
            .for_each(|thread_id: Option<DocumentId>| {
                if let Some(thread_id) = thread_id {
                    thread_ids.insert(thread_id);
                }
            });
        thread_ids.len()
    } else {
        0
    })
}

fn get_mailbox_tag<'x, T>(
    store: &'x JMAPLocalStore<T>,
    account_id: AccountId,
    document_id: DocumentId,
) -> store::Result<Option<T::Set>>
where
    T: Store<'x>,
{
    store.store.get_tag(
        account_id,
        JMAP_MAIL,
        MessageField::Mailbox.into(),
        Tag::Id(document_id),
    )
}

fn get_mailbox_unread_tag<'x, T>(
    store: &'x JMAPLocalStore<T>,
    account_id: AccountId,
    document_id: DocumentId,
) -> store::Result<Option<T::Set>>
where
    T: Store<'x>,
{
    match get_mailbox_tag(store, account_id, document_id) {
        Ok(Some(mailbox)) => {
            match store.store.get_tag(
                account_id,
                JMAP_MAIL,
                MessageField::Keyword.into(),
                Tag::Text("$unread".to_string()), //TODO use id keywords
            ) {
                Ok(Some(mut unread)) => {
                    unread.intersection(&mailbox);
                    if !unread.is_empty() {
                        Ok(Some(unread))
                    } else {
                        Ok(None)
                    }
                }
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            }
        }
        other => other,
    }
}

#[allow(clippy::blocks_in_if_conditions)]
fn validate_properties<'x, T>(
    store: &'x JMAPLocalStore<T>,
    account_id: AccountId,
    mailbox_id: Option<JMAPId>,
    current_mailbox: Option<&JMAPMailbox>,
    properties: JSONValue,
    destroy_ids: &JSONValue,
    max_nest_level: usize,
) -> jmap_store::Result<Result<JMAPMailboxSet, JSONValue>>
where
    T: Store<'x>,
{
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
                mailbox.role = match value {
                    JSONValue::Null => Some(None),
                    JSONValue::String(s) => Some(Some(s.to_lowercase())),
                    _ => None,
                };
            }
            Some(JMAPMailboxProperties::SortOrder) => {
                mailbox.sort_order = value.unwrap_unsigned_int().map(|x| x as u32);
            }
            Some(JMAPMailboxProperties::IsSubscribed) => {
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
                        .store
                        .get_document_value::<Vec<u8>>(
                            account_id,
                            JMAP_MAILBOX,
                            mailbox_parent_id.get_document_id(),
                            JMAPMailboxProperties::Id.into(),
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
            && !store
                .store
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
            for document_id in store.store.query(
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
                        .store
                        .get_document_value::<Vec<u8>>(
                            account_id,
                            JMAP_MAILBOX,
                            document_id,
                            JMAPMailboxProperties::Id.into(),
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
