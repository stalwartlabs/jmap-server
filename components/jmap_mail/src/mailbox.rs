use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;

use jmap::changes::{
    query_changes, JMAPChanges, JMAPChangesRequest, JMAPChangesResponse, JMAPQueryChangesResponse,
};
use jmap::id::JMAPIdSerialize;
use jmap::{json::JSONValue, JMAPError, JMAPSet, JMAPSetErrorType, JMAPSetResponse};
use jmap::{
    JMAPComparator, JMAPGet, JMAPGetResponse, JMAPQueryChangesRequest, JMAPQueryRequest,
    JMAPQueryResponse,
};

use store::batch::Document;
use store::field::{FieldOptions, Text};
use store::query::{JMAPIdMapFnc, JMAPStoreQuery};
use store::roaring::RoaringBitmap;
use store::serialize::{StoreDeserialize, StoreSerialize};
use store::{batch::WriteBatch, Store};
use store::{
    bincode, AccountId, Collection, Comparator, ComparisonOperator, DocumentId, FieldComparator,
    FieldId, FieldValue, Filter, JMAPId, JMAPIdPrefix, JMAPStore, LongInteger, StoreError, Tag,
};

use crate::MessageField;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Mailbox {
    pub name: String,
    pub parent_id: JMAPId,
    pub role: Option<String>,
    pub sort_order: u32,
}

impl StoreSerialize for Mailbox {
    fn serialize(&self) -> Option<Vec<u8>> {
        bincode::serialize(self).ok()
    }
}

impl StoreDeserialize for Mailbox {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct MailboxChanges {
    pub name: Option<String>,
    pub parent_id: Option<JMAPId>,
    pub role: Option<Option<String>>,
    pub sort_order: Option<u32>,
    pub is_subscribed: Option<bool>,
}

pub struct JMAPMailboxSetArguments {
    pub remove_emails: bool,
}

#[derive(Debug)]
pub struct JMAPMailboxChangesResponse {
    pub updated_properties: Vec<JMAPMailboxProperties>,
}
pub enum JMAPMailboxFilterCondition {
    ParentId(JMAPId),
    Name(String),
    Role(String),
    HasAnyRole,
    IsSubscribed,
}

pub enum JMAPMailboxComparator {
    Name,
    Role,
    ParentId,
}

pub struct JMAPMailboxQueryArguments {
    pub sort_as_tree: bool,
    pub filter_as_tree: bool,
}

#[derive(Debug, PartialEq, Eq)]
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

pub trait JMAPMailMailbox {
    fn mailbox_set(
        &self,
        request: JMAPSet<JMAPMailboxSetArguments>,
    ) -> jmap::Result<JMAPSetResponse>;

    fn mailbox_get(
        &self,
        request: JMAPGet<JMAPMailboxProperties, ()>,
    ) -> jmap::Result<jmap::JMAPGetResponse>;

    fn mailbox_query(
        &self,
        request: JMAPQueryRequest<
            JMAPMailboxFilterCondition,
            JMAPMailboxComparator,
            JMAPMailboxQueryArguments,
        >,
    ) -> jmap::Result<JMAPQueryResponse>;

    fn mailbox_changes(
        &self,
        request: JMAPChangesRequest,
    ) -> jmap::Result<JMAPChangesResponse<JMAPMailboxChangesResponse>>;

    fn mailbox_query_changes(
        &self,
        request: JMAPQueryChangesRequest<
            JMAPMailboxFilterCondition,
            JMAPMailboxComparator,
            JMAPMailboxQueryArguments,
        >,
    ) -> jmap::Result<JMAPQueryChangesResponse>;

    fn count_threads(
        &self,
        account_id: AccountId,
        document_ids: Option<RoaringBitmap>,
    ) -> store::Result<usize>;

    fn get_mailbox_tag(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Option<RoaringBitmap>>;

    fn get_mailbox_unread_tag(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Option<RoaringBitmap>>;

    #[allow(clippy::too_many_arguments)]
    fn validate_properties(
        &self,
        account_id: AccountId,
        mailbox_id: Option<JMAPId>,
        mailbox_ids: &RoaringBitmap,
        current_mailbox: Option<&Mailbox>,
        properties: JSONValue,
        destroy_ids: &JSONValue,
        max_nest_level: usize,
    ) -> jmap::Result<Result<MailboxChanges, JSONValue>>;

    fn raft_update_mailbox(
        &self,
        batch: &mut WriteBatch,
        account_id: AccountId,
        mailbox_id: JMAPId,
        mailbox: Mailbox,
    ) -> store::Result<()>;
}

//TODO mailbox id 0 is inbox and cannot be deleted
impl<T> JMAPMailMailbox for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mailbox_set(
        &self,
        request: JMAPSet<JMAPMailboxSetArguments>,
    ) -> jmap::Result<JMAPSetResponse> {
        let old_state = self.get_state(request.account_id, Collection::Mailbox)?;
        if let Some(if_in_state) = request.if_in_state {
            if old_state != if_in_state {
                return Err(JMAPError::StateMismatch);
            }
        }
        let total_changes = request.create.to_object().map_or(0, |c| c.len())
            + request.update.to_object().map_or(0, |c| c.len())
            + request.destroy.to_array().map_or(0, |c| c.len());
        if total_changes > self.config.mailbox_set_max_changes {
            return Err(JMAPError::RequestTooLarge);
        }

        let mut changes = WriteBatch::new(request.account_id);
        let mut response = JMAPSetResponse {
            old_state,
            ..Default::default()
        };
        let document_ids = self
            .get_document_ids(request.account_id, Collection::Mailbox)?
            .unwrap_or_default();

        if let JSONValue::Object(create) = request.create {
            let mut created = HashMap::with_capacity(create.len());
            let mut not_created = HashMap::with_capacity(create.len());

            'create: for (pos, (create_id, properties)) in create.into_iter().enumerate() {
                if document_ids.len() as usize + pos + 1 > self.config.mailbox_max_total {
                    not_created.insert(
                        create_id,
                        JSONValue::new_error(
                            JMAPSetErrorType::Forbidden,
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
                    self.config.mailbox_max_depth,
                )? {
                    Ok(mailbox) => mailbox,
                    Err(err) => {
                        not_created.insert(create_id, err);
                        continue 'create;
                    }
                };

                let assigned_id =
                    self.assign_document_id(request.account_id, Collection::Mailbox)?;
                let jmap_id = assigned_id as JMAPId;

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

                let mailbox = self
                    .get_document_value::<Mailbox>(
                        request.account_id,
                        Collection::Mailbox,
                        document_id,
                        JMAPMailboxProperties::Id.into(),
                    )?
                    .ok_or_else(|| {
                        StoreError::InternalError("Mailbox data not found".to_string())
                    })?;

                let mailbox_changes = match self.validate_properties(
                    request.account_id,
                    jmap_id.into(),
                    &document_ids,
                    (&mailbox).into(),
                    properties,
                    &request.destroy,
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
                                .query::<JMAPIdMapFnc>(JMAPStoreQuery::new(
                                    request.account_id,
                                    Collection::Mailbox,
                                    Filter::new_condition(
                                        JMAPMailboxProperties::ParentId.into(),
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
                                        JMAPSetErrorType::MailboxHasChild,
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
                                    .get_multi_document_value(
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
                                            JMAPId::from_parts(thread_id, message_doc_id),
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
            self.write(changes)?;
            response.new_state = self.get_state(request.account_id, Collection::Mailbox)?;
        } else {
            response.new_state = response.old_state.clone();
        }

        Ok(response)
    }

    fn mailbox_get(
        &self,
        request: JMAPGet<JMAPMailboxProperties, ()>,
    ) -> jmap::Result<jmap::JMAPGetResponse> {
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
            if request_ids.len() > self.config.get_max_results {
                return Err(JMAPError::RequestTooLarge);
            } else {
                request_ids
            }
        } else {
            self.get_document_ids(request.account_id, Collection::Mailbox)?
                .unwrap_or_default()
                .into_iter()
                .take(self.config.get_max_results)
                .map(|id| id as JMAPId)
                .collect::<Vec<JMAPId>>()
        };

        let document_ids = self
            .get_document_ids(request.account_id, Collection::Mailbox)?
            .unwrap_or_default();
        let mut not_found = Vec::new();
        let mut results = Vec::with_capacity(request_ids.len());

        for jmap_id in request_ids {
            let document_id = jmap_id.get_document_id();
            if !document_ids.contains(document_id) {
                not_found.push(jmap_id);
                continue;
            }
            let mut mailbox = if properties.iter().any(|p| {
                matches!(
                    p,
                    JMAPMailboxProperties::Name
                        | JMAPMailboxProperties::ParentId
                        | JMAPMailboxProperties::Role
                        | JMAPMailboxProperties::SortOrder
                )
            }) {
                Some(
                    self.get_document_value::<Mailbox>(
                        request.account_id,
                        Collection::Mailbox,
                        document_id,
                        JMAPMailboxProperties::Id.into(),
                    )?
                    .ok_or_else(|| {
                        StoreError::InternalError("Mailbox data not found".to_string())
                    })?,
                )
            } else {
                None
            };

            let mut result: HashMap<String, JSONValue> = HashMap::new();

            for property in &properties {
                if let Entry::Vacant(entry) = result.entry(property.to_string()) {
                    let value = match property {
                        JMAPMailboxProperties::Id => jmap_id.to_jmap_string().into(),
                        JMAPMailboxProperties::Name => {
                            std::mem::take(&mut mailbox.as_mut().unwrap().name).into()
                        }
                        JMAPMailboxProperties::ParentId => {
                            if mailbox.as_ref().unwrap().parent_id > 0 {
                                (mailbox.as_ref().unwrap().parent_id - 1)
                                    .to_jmap_string()
                                    .into()
                            } else {
                                JSONValue::Null
                            }
                        }
                        JMAPMailboxProperties::Role => {
                            std::mem::take(&mut mailbox.as_mut().unwrap().role)
                                .map(|v| v.into())
                                .unwrap_or_default()
                        }
                        JMAPMailboxProperties::SortOrder => {
                            mailbox.as_ref().unwrap().sort_order.into()
                        }
                        JMAPMailboxProperties::IsSubscribed => true.into(), //TODO implement
                        JMAPMailboxProperties::MyRights => JSONValue::Null, //TODO implement
                        JMAPMailboxProperties::TotalEmails => self
                            .get_mailbox_tag(request.account_id, document_id)?
                            .map(|v| v.len())
                            .unwrap_or(0)
                            .into(),
                        JMAPMailboxProperties::UnreadEmails => self
                            .get_mailbox_unread_tag(request.account_id, document_id)?
                            .map(|v| v.len())
                            .unwrap_or(0)
                            .into(),
                        JMAPMailboxProperties::TotalThreads => self
                            .count_threads(
                                request.account_id,
                                self.get_mailbox_tag(request.account_id, document_id)?,
                            )?
                            .into(),
                        JMAPMailboxProperties::UnreadThreads => self
                            .count_threads(
                                request.account_id,
                                self.get_mailbox_unread_tag(request.account_id, document_id)?,
                            )?
                            .into(),
                    };

                    entry.insert(value);
                }
            }

            results.push(result.into());
        }

        Ok(JMAPGetResponse {
            state: self.get_state(request.account_id, Collection::Mailbox)?,
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
        &self,
        mut request: JMAPQueryRequest<
            JMAPMailboxFilterCondition,
            JMAPMailboxComparator,
            JMAPMailboxQueryArguments,
        >,
    ) -> jmap::Result<JMAPQueryResponse> {
        let cond_fnc = |cond| {
            Ok(match cond {
                JMAPMailboxFilterCondition::ParentId(parent_id) => Filter::eq(
                    JMAPMailboxProperties::ParentId.into(),
                    FieldValue::LongInteger(parent_id),
                ),
                JMAPMailboxFilterCondition::Name(text) => Filter::eq(
                    JMAPMailboxProperties::Name.into(),
                    FieldValue::Text(text.to_lowercase()),
                ),
                JMAPMailboxFilterCondition::Role(text) => {
                    Filter::eq(JMAPMailboxProperties::Role.into(), FieldValue::Text(text))
                }
                JMAPMailboxFilterCondition::HasAnyRole => Filter::eq(
                    JMAPMailboxProperties::Role.into(),
                    FieldValue::Tag(Tag::Static(0)),
                ),
                JMAPMailboxFilterCondition::IsSubscribed => todo!(), //TODO implement
            })
        };

        let sort_fnc = |comp: JMAPComparator<JMAPMailboxComparator>| {
            Ok(Comparator::Field(FieldComparator {
                field: match comp.property {
                    JMAPMailboxComparator::Name => JMAPMailboxProperties::Name,
                    JMAPMailboxComparator::Role => JMAPMailboxProperties::Role,
                    JMAPMailboxComparator::ParentId => JMAPMailboxProperties::ParentId,
                }
                .into(),
                ascending: comp.is_ascending,
            }))
        };

        let account_id = request.account_id;
        let mut results = self
            .query::<JMAPIdMapFnc>(request.build_query(
                Collection::Mailbox,
                cond_fnc,
                sort_fnc,
                None,
            )?)?
            .into_iter()
            .collect::<Vec<JMAPId>>();

        if !results.is_empty()
            && (request.arguments.filter_as_tree || request.arguments.sort_as_tree)
        {
            let mut hierarchy = HashMap::new();
            let mut tree = HashMap::new();

            for doc_id in self
                .get_document_ids(request.account_id, Collection::Mailbox)?
                .unwrap_or_default()
            {
                let mailbox = self
                    .get_document_value::<Mailbox>(
                        request.account_id,
                        Collection::Mailbox,
                        doc_id,
                        JMAPMailboxProperties::Id.into(),
                    )?
                    .ok_or_else(|| {
                        StoreError::InternalError("Mailbox data not found".to_string())
                    })?;
                hierarchy.insert((doc_id + 1) as JMAPId, mailbox.parent_id);
                tree.entry(mailbox.parent_id)
                    .or_insert_with(HashSet::new)
                    .insert((doc_id + 1) as JMAPId);
            }

            if request.arguments.filter_as_tree {
                let mut filtered_ids = Vec::with_capacity(results.len());

                for &doc_id in &results {
                    let mut keep = false;
                    let mut jmap_id = (doc_id + 1) as JMAPId;

                    for _ in 0..self.config.mailbox_max_depth {
                        if let Some(&parent_id) = hierarchy.get(&jmap_id) {
                            if parent_id == 0 {
                                keep = true;
                                break;
                            } else if !results.contains(&(parent_id - 1)) {
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
                if filtered_ids.len() != results.len() {
                    results = filtered_ids;
                }
            }

            if request.arguments.sort_as_tree && results.len() > 1 {
                let mut stack = Vec::new();
                let mut sorted_list = Vec::with_capacity(results.len());
                let mut jmap_id = 0;

                'outer: for _ in 0..(results.len() * 10 * self.config.mailbox_max_depth) {
                    let (mut children, mut it) = if let Some(children) = tree.remove(&jmap_id) {
                        (children, results.iter())
                    } else if let Some(prev) = stack.pop() {
                        prev
                    } else {
                        break;
                    };

                    while let Some(&doc_id) = it.next() {
                        jmap_id = (doc_id + 1) as JMAPId;
                        if children.remove(&jmap_id) {
                            sorted_list.push(doc_id);
                            if sorted_list.len() == results.len() {
                                break 'outer;
                            } else {
                                stack.push((children, it));
                                continue 'outer;
                            }
                        }
                    }

                    if !children.is_empty() {
                        jmap_id = *children.iter().next().unwrap();
                        children.remove(&jmap_id);
                        stack.push((children, it));
                    }
                }
                results = sorted_list;
            }
        }

        if request.limit == 0 || request.limit > self.config.query_max_results {
            request.limit = self.config.query_max_results;
        }

        request.into_response(
            results.into_iter(),
            self.get_state(account_id, Collection::Mailbox)?,
            false,
        )
    }

    fn mailbox_changes(
        &self,
        request: JMAPChangesRequest,
    ) -> jmap::Result<JMAPChangesResponse<JMAPMailboxChangesResponse>> {
        let mut changes = self.get_jmap_changes(
            request.account_id,
            Collection::Mailbox,
            request.since_state.clone(),
            request.max_changes,
        )?;

        let mut updated_properties = None;
        if !changes.arguments.is_empty() {
            if changes.updated.is_empty() {
                updated_properties = vec![
                    JMAPMailboxProperties::TotalEmails,
                    JMAPMailboxProperties::UnreadEmails,
                    JMAPMailboxProperties::TotalThreads,
                    JMAPMailboxProperties::UnreadThreads,
                ]
                .into();
                changes.updated = changes.arguments;
            } else {
                for jmap_id in changes.arguments {
                    debug_assert!(!changes.updated.contains(&jmap_id));
                    changes.updated.push(jmap_id);
                }
            }
        }

        Ok(JMAPChangesResponse {
            old_state: changes.old_state,
            new_state: changes.new_state,
            has_more_changes: changes.has_more_changes,
            total_changes: changes.total_changes,
            created: changes.created,
            updated: changes.updated,
            destroyed: changes.destroyed,
            arguments: JMAPMailboxChangesResponse {
                updated_properties: updated_properties.unwrap_or_default(),
            },
        })
    }

    fn mailbox_query_changes(
        &self,
        request: JMAPQueryChangesRequest<
            JMAPMailboxFilterCondition,
            JMAPMailboxComparator,
            JMAPMailboxQueryArguments,
        >,
    ) -> jmap::Result<JMAPQueryChangesResponse> {
        let mut changes = self.get_jmap_changes(
            request.account_id,
            Collection::Mailbox,
            request.since_query_state,
            request.max_changes,
        )?;

        if !changes.arguments.is_empty() {
            for jmap_id in &changes.arguments {
                debug_assert!(!changes.updated.contains(jmap_id));
                changes.updated.push(*jmap_id);
            }
        }

        let query_results = if changes.total_changes > 0 || request.calculate_total {
            Some(self.mailbox_query(JMAPQueryRequest {
                account_id: request.account_id,
                filter: request.filter,
                sort: request.sort,
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 0,
                calculate_total: true,
                arguments: request.arguments,
            })?)
        } else {
            None
        };

        Ok(query_changes(changes, query_results, request.up_to_id))
    }

    fn count_threads(
        &self,
        account_id: AccountId,
        document_ids: Option<RoaringBitmap>,
    ) -> store::Result<usize> {
        Ok(if let Some(document_ids) = document_ids {
            let mut thread_ids = HashSet::new();
            self.get_multi_document_value(
                account_id,
                Collection::Mail,
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

    fn get_mailbox_tag(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Option<RoaringBitmap>> {
        self.get_tag(
            account_id,
            Collection::Mail,
            MessageField::Mailbox.into(),
            Tag::Id(document_id),
        )
    }

    fn get_mailbox_unread_tag(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Option<RoaringBitmap>> {
        match self.get_mailbox_tag(account_id, document_id) {
            Ok(Some(mailbox)) => {
                match self.get_tag(
                    account_id,
                    Collection::Mail,
                    MessageField::Keyword.into(),
                    Tag::Text("$unread".to_string()), //TODO use id keywords
                ) {
                    Ok(Some(mut unread)) => {
                        unread &= &mailbox;
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
    #[allow(clippy::too_many_arguments)]
    fn validate_properties(
        &self,
        account_id: AccountId,
        mailbox_id: Option<JMAPId>,
        mailbox_ids: &RoaringBitmap,
        current_mailbox: Option<&Mailbox>,
        properties: JSONValue,
        destroy_ids: &JSONValue,
        max_nest_level: usize,
    ) -> jmap::Result<Result<MailboxChanges, JSONValue>> {
        let mut mailbox = MailboxChanges::default();

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

                        if let Some(mailbox_parent_id) =
                            JMAPId::from_jmap_string(&mailbox_parent_id_str)
                        {
                            if !mailbox_ids.contains(mailbox_parent_id as DocumentId) {
                                return Ok(Err(JSONValue::new_error(
                                    JMAPSetErrorType::InvalidProperties,
                                    "Parent ID does not exist.",
                                )));
                            }
                            mailbox.parent_id = (mailbox_parent_id + 1).into();
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
                    if mailbox_parent_id == mailbox_id + 1 {
                        return Ok(Err(JSONValue::new_error(
                            JMAPSetErrorType::InvalidProperties,
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
                            JMAPMailboxProperties::Id.into(),
                        )?
                        .ok_or_else(|| {
                            StoreError::InternalError("Mailbox data not found".to_string())
                        })?
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
                && !self
                    .query::<JMAPIdMapFnc>(JMAPStoreQuery::new(
                        account_id,
                        Collection::Mailbox,
                        Filter::new_condition(
                            JMAPMailboxProperties::Role.into(),
                            ComparisonOperator::Equal,
                            FieldValue::Keyword(mailbox_role.into()),
                        ),
                        Comparator::None,
                    ))?
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
                for jmap_id in self.query::<JMAPIdMapFnc>(JMAPStoreQuery::new(
                    account_id,
                    Collection::Mailbox,
                    Filter::new_condition(
                        JMAPMailboxProperties::ParentId.into(),
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
                            JMAPMailboxProperties::Id.into(),
                        )?
                        .ok_or_else(|| {
                            StoreError::InternalError("Mailbox data not found".to_string())
                        })?
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

    fn raft_update_mailbox(
        &self,
        batch: &mut WriteBatch,
        account_id: AccountId,
        mailbox_id: JMAPId,
        mailbox: Mailbox,
    ) -> store::Result<()> {
        let document_id = mailbox_id.get_document_id();
        if let Some(current_mailbox) = self.get_document_value::<Mailbox>(
            account_id,
            Collection::Mailbox,
            document_id,
            JMAPMailboxProperties::Id.into(),
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
        JMAPMailboxProperties::Name,
        Text::Tokenized(mailbox.name.to_lowercase()),
        FieldOptions::Sort,
    );

    if let Some(mailbox_role) = mailbox.role.as_ref() {
        document.text(
            JMAPMailboxProperties::Role,
            Text::Keyword(mailbox_role.clone()),
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
        mailbox.serialize().ok_or_else(|| {
            StoreError::SerializeError("Failed to serialize mailbox.".to_string())
        })?,
        FieldOptions::Store,
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
                JMAPMailboxProperties::Name,
                Text::Tokenized(mailbox.name.to_lowercase()),
                FieldOptions::Clear,
            );
            document.text(
                JMAPMailboxProperties::Name,
                Text::Tokenized(new_name.to_lowercase()),
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
                    Text::Keyword(role),
                    FieldOptions::Clear,
                );
                true
            } else {
                false
            };
            if let Some(new_role) = &new_role {
                document.text(
                    JMAPMailboxProperties::Role,
                    Text::Keyword(new_role.clone()),
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
            mailbox.serialize().ok_or_else(|| {
                StoreError::SerializeError("Failed to serialize mailbox.".to_string())
            })?,
            FieldOptions::Store,
        );
        Ok(Some(document))
    } else {
        Ok(None)
    }
}
