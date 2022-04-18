use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;

use jmap::changes::JMAPChanges;
use jmap::id::JMAPIdReference;
use jmap::id::JMAPIdSerialize;
use jmap::query::QueryResult;
use jmap::request::{
    ChangesRequest, GetRequest, JSONArgumentParser, QueryChangesRequest, QueryRequest, SetRequest,
};
use jmap::{json::JSONValue, JMAPError, SetErrorType};
use store::batch::Document;
use store::field::{DefaultOptions, Options, Text};
use store::query::{JMAPIdMapFnc, JMAPStoreQuery};
use store::roaring::RoaringBitmap;
use store::serialize::{StoreDeserialize, StoreSerialize};
use store::{batch::WriteBatch, Store};
use store::{
    bincode, AccountId, Collection, Comparator, ComparisonOperator, DocumentId, FieldComparator,
    FieldId, FieldValue, Filter, JMAPId, JMAPIdPrefix, JMAPStore, LongInteger, StoreError, Tag,
};

use crate::{Keyword, MessageField};

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

/*
pub struct JMAPMailboxSetArguments {
    pub remove_emails: bool,
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
}*/

#[derive(Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum MailboxProperties {
    Id = 0,
    Name = 1,
    ParentId = 2,
    Role = 3,
    HasRole = 4,
    SortOrder = 5,
    IsSubscribed = 6,
    TotalEmails = 7,
    UnreadEmails = 8,
    TotalThreads = 9,
    UnreadThreads = 10,
    MyRights = 11,
}

impl MailboxProperties {
    pub fn as_str(&self) -> &'static str {
        match self {
            MailboxProperties::Id => "id",
            MailboxProperties::Name => "name",
            MailboxProperties::ParentId => "parentId",
            MailboxProperties::Role => "role",
            MailboxProperties::HasRole => "hasRole",
            MailboxProperties::SortOrder => "sortOrder",
            MailboxProperties::IsSubscribed => "isSubscribed",
            MailboxProperties::TotalEmails => "totalEmails",
            MailboxProperties::UnreadEmails => "unreadEmails",
            MailboxProperties::TotalThreads => "totalThreads",
            MailboxProperties::UnreadThreads => "unreadThreads",
            MailboxProperties::MyRights => "myRights",
        }
    }
}

impl Display for MailboxProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<MailboxProperties> for FieldId {
    fn from(field: MailboxProperties) -> Self {
        field as FieldId
    }
}

impl From<MailboxProperties> for JSONValue {
    fn from(value: MailboxProperties) -> Self {
        JSONValue::String(value.as_str().to_string())
    }
}

impl MailboxProperties {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "id" => Some(MailboxProperties::Id),
            "name" => Some(MailboxProperties::Name),
            "parentId" => Some(MailboxProperties::ParentId),
            "role" => Some(MailboxProperties::Role),
            "sortOrder" => Some(MailboxProperties::SortOrder),
            "isSubscribed" => Some(MailboxProperties::IsSubscribed),
            "totalEmails" => Some(MailboxProperties::TotalEmails),
            "unreadEmails" => Some(MailboxProperties::UnreadEmails),
            "totalThreads" => Some(MailboxProperties::TotalThreads),
            "unreadThreads" => Some(MailboxProperties::UnreadThreads),
            "myRights" => Some(MailboxProperties::MyRights),
            _ => None,
        }
    }
}

impl JSONArgumentParser for MailboxProperties {
    fn parse_argument(argument: JSONValue) -> jmap::Result<Self> {
        let argument = argument
            .unwrap_string()
            .ok_or_else(|| JMAPError::InvalidArguments("Expected string argument.".to_string()))?;
        MailboxProperties::parse(&argument).ok_or_else(|| {
            JMAPError::InvalidArguments(format!("Unknown mailbox property: '{}'.", argument))
        })
    }
}

pub trait JMAPMailMailbox {
    fn mailbox_set(&self, request: SetRequest) -> jmap::Result<JSONValue>;

    fn mailbox_get(&self, request: GetRequest) -> jmap::Result<JSONValue>;

    fn mailbox_query(&self, request: QueryRequest) -> jmap::Result<JSONValue>;

    fn mailbox_changes(&self, request: ChangesRequest) -> jmap::Result<JSONValue>;

    fn mailbox_query_changes(&self, request: QueryChangesRequest) -> jmap::Result<JSONValue>;

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
        mail_document_ids: &Option<RoaringBitmap>,
    ) -> store::Result<Option<RoaringBitmap>>;

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
impl<T> JMAPMailMailbox for JMAPStore<T>
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

    fn mailbox_get(&self, request: GetRequest) -> jmap::Result<JSONValue> {
        let properties = request
            .properties
            .parse_array_items(true)?
            .unwrap_or_else(|| {
                vec![
                    MailboxProperties::Id,
                    MailboxProperties::Name,
                    MailboxProperties::ParentId,
                    MailboxProperties::Role,
                    MailboxProperties::SortOrder,
                    MailboxProperties::IsSubscribed,
                    MailboxProperties::TotalEmails,
                    MailboxProperties::UnreadEmails,
                    MailboxProperties::TotalThreads,
                    MailboxProperties::UnreadThreads,
                    MailboxProperties::MyRights,
                ]
            });

        let request_ids = if let Some(request_ids) = request.ids {
            if request_ids.len() > self.config.max_objects_in_get {
                return Err(JMAPError::RequestTooLarge);
            } else {
                request_ids
            }
        } else {
            self.get_document_ids(request.account_id, Collection::Mailbox)?
                .unwrap_or_default()
                .into_iter()
                .take(self.config.max_objects_in_get)
                .map(|id| id as JMAPId)
                .collect::<Vec<JMAPId>>()
        };

        let document_ids = self
            .get_document_ids(request.account_id, Collection::Mailbox)?
            .unwrap_or_default();
        let mail_document_ids = self.get_document_ids(request.account_id, Collection::Mail)?;
        let mut not_found = Vec::new();
        let mut results = Vec::with_capacity(request_ids.len());

        for jmap_id in request_ids {
            let document_id = jmap_id.get_document_id();
            if !document_ids.contains(document_id) {
                not_found.push(jmap_id.to_jmap_string().into());
                continue;
            }
            let mut mailbox = if properties.iter().any(|p| {
                matches!(
                    p,
                    MailboxProperties::Name
                        | MailboxProperties::ParentId
                        | MailboxProperties::Role
                        | MailboxProperties::SortOrder
                )
            }) {
                Some(
                    self.get_document_value::<Mailbox>(
                        request.account_id,
                        Collection::Mailbox,
                        document_id,
                        MailboxProperties::Id.into(),
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
                        MailboxProperties::Id => jmap_id.to_jmap_string().into(),
                        MailboxProperties::Name => {
                            std::mem::take(&mut mailbox.as_mut().unwrap().name).into()
                        }
                        MailboxProperties::ParentId => {
                            if mailbox.as_ref().unwrap().parent_id > 0 {
                                (mailbox.as_ref().unwrap().parent_id - 1)
                                    .to_jmap_string()
                                    .into()
                            } else {
                                JSONValue::Null
                            }
                        }
                        MailboxProperties::Role => {
                            std::mem::take(&mut mailbox.as_mut().unwrap().role)
                                .map(|v| v.into())
                                .unwrap_or_default()
                        }
                        MailboxProperties::SortOrder => mailbox.as_ref().unwrap().sort_order.into(),
                        MailboxProperties::IsSubscribed => true.into(), //TODO implement
                        MailboxProperties::MyRights => JSONValue::Object(HashMap::new()), //TODO implement
                        MailboxProperties::TotalEmails => self
                            .get_mailbox_tag(request.account_id, document_id)?
                            .map(|v| v.len())
                            .unwrap_or(0)
                            .into(),
                        MailboxProperties::UnreadEmails => self
                            .get_mailbox_unread_tag(
                                request.account_id,
                                document_id,
                                &mail_document_ids,
                            )?
                            .map(|v| v.len())
                            .unwrap_or(0)
                            .into(),
                        MailboxProperties::TotalThreads => self
                            .count_threads(
                                request.account_id,
                                self.get_mailbox_tag(request.account_id, document_id)?,
                            )?
                            .into(),
                        MailboxProperties::UnreadThreads => self
                            .count_threads(
                                request.account_id,
                                self.get_mailbox_unread_tag(
                                    request.account_id,
                                    document_id,
                                    &mail_document_ids,
                                )?,
                            )?
                            .into(),
                        MailboxProperties::HasRole => JSONValue::Null,
                    };

                    entry.insert(value);
                }
            }

            results.push(result.into());
        }

        let mut obj = HashMap::new();
        obj.insert(
            "state".to_string(),
            self.get_state(request.account_id, Collection::Mailbox)?
                .into(),
        );
        obj.insert("list".to_string(), results.into());
        obj.insert("notFound".to_string(), not_found.into());
        Ok(obj.into())
    }

    fn mailbox_query(&self, mut request: QueryRequest) -> jmap::Result<JSONValue> {
        let cond_fnc = |cond: HashMap<String, JSONValue>| {
            if let Some((cond_name, cond_value)) = cond.into_iter().next() {
                Ok(match cond_name.as_str() {
                    "parentId" => Filter::eq(
                        MailboxProperties::ParentId.into(),
                        FieldValue::LongInteger(
                            cond_value
                                .parse_jmap_id(true)?
                                .map(|id| id + 1)
                                .unwrap_or(0),
                        ),
                    ),
                    "name" => Filter::eq(
                        MailboxProperties::Name.into(),
                        FieldValue::Text(cond_value.parse_string()?.to_lowercase()),
                    ),
                    "role" => Filter::eq(
                        MailboxProperties::Role.into(),
                        FieldValue::Text(cond_value.parse_string()?),
                    ),
                    "hasAnyRole" => {
                        let filter = Filter::eq(
                            MailboxProperties::HasRole.into(),
                            FieldValue::Tag(Tag::Static(0)),
                        );
                        if !cond_value.parse_bool()? {
                            Filter::not(vec![filter])
                        } else {
                            filter
                        }
                    }
                    "isSubscribed" => todo!(), //TODO implement
                    _ => {
                        return Err(JMAPError::UnsupportedFilter(format!(
                            "Unsupported filter '{}'.",
                            cond_name
                        )))
                    }
                })
            } else {
                Ok(Filter::None)
            }
        };

        let sort_fnc = |comp: jmap::query::Comparator| {
            Ok(Comparator::Field(FieldComparator {
                field: match comp.property.as_ref() {
                    "name" => MailboxProperties::Name,
                    "role" => MailboxProperties::Role,
                    "parentId" => MailboxProperties::ParentId,
                    _ => {
                        return Err(JMAPError::UnsupportedSort(format!(
                            "Unsupported sort property '{}'.",
                            comp.property
                        )))
                    }
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

        let filter_as_tree = request
            .arguments
            .get("filterAsTree")
            .and_then(|v| v.to_bool())
            .unwrap_or(false);

        let sort_as_tree = request
            .arguments
            .get("sortAsTree")
            .and_then(|v| v.to_bool())
            .unwrap_or(false);

        if !results.is_empty() && (filter_as_tree || sort_as_tree) {
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
                        MailboxProperties::Id.into(),
                    )?
                    .ok_or_else(|| {
                        StoreError::InternalError("Mailbox data not found".to_string())
                    })?;
                hierarchy.insert((doc_id + 1) as JMAPId, mailbox.parent_id);
                tree.entry(mailbox.parent_id)
                    .or_insert_with(HashSet::new)
                    .insert((doc_id + 1) as JMAPId);
            }

            if filter_as_tree {
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

            if sort_as_tree && results.len() > 1 {
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
        )
    }

    fn mailbox_changes(&self, request: ChangesRequest) -> jmap::Result<JSONValue> {
        let mut changes = self.get_jmap_changes(
            request.account_id,
            Collection::Mailbox,
            request.since_state.clone(),
            request.max_changes,
        )?;

        if changes.has_children_changes {
            changes.result.as_object_mut().insert(
                "updatedProperties".to_string(),
                vec![
                    MailboxProperties::TotalEmails.into(),
                    MailboxProperties::UnreadEmails.into(),
                    MailboxProperties::TotalThreads.into(),
                    MailboxProperties::UnreadThreads.into(),
                ]
                .into(),
            );
        }

        Ok(changes.result)
    }

    fn mailbox_query_changes(&self, request: QueryChangesRequest) -> jmap::Result<JSONValue> {
        let changes = self.get_jmap_changes(
            request.account_id,
            Collection::Mailbox,
            request.since_query_state,
            request.max_changes,
        )?;

        let result = if changes.total_changes > 0 || request.calculate_total {
            self.mailbox_query(QueryRequest {
                account_id: request.account_id,
                filter: request.filter,
                sort: request.sort,
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 0,
                calculate_total: true,
                arguments: request.arguments,
            })?
        } else {
            JSONValue::Null
        };

        Ok(changes.query(
            QueryResult {
                is_immutable: false,
                result,
            },
            request.up_to_id,
        ))
    }

    fn count_threads(
        &self,
        account_id: AccountId,
        document_ids: Option<RoaringBitmap>,
    ) -> store::Result<usize> {
        Ok(if let Some(document_ids) = document_ids {
            let mut thread_ids = HashSet::new();
            self.get_multi_document_tag_id(
                account_id,
                Collection::Mail,
                document_ids.into_iter(),
                MessageField::ThreadId.into(),
            )?
            .into_iter()
            .for_each(|thread_id| {
                if let Some(thread_id) = thread_id {
                    thread_ids.insert(*thread_id);
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
        mail_document_ids: &Option<RoaringBitmap>,
    ) -> store::Result<Option<RoaringBitmap>> {
        if let Some(mail_document_ids) = mail_document_ids {
            match self.get_mailbox_tag(account_id, document_id) {
                Ok(Some(mailbox)) => {
                    match self.get_tag(
                        account_id,
                        Collection::Mail,
                        MessageField::Keyword.into(),
                        Tag::Static(Keyword::SEEN),
                    ) {
                        Ok(Some(mut seen)) => {
                            seen ^= mail_document_ids;
                            seen &= &mailbox;
                            if !seen.is_empty() {
                                Ok(Some(seen))
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
        } else {
            Ok(None)
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
