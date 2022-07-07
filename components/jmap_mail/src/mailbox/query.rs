use super::schema::{Comparator, Filter, Mailbox, Property};
use crate::mail::sharing::JMAPShareMail;
use jmap::error::method::MethodError;
use jmap::jmap_store::query::{ExtraFilterFnc, QueryHelper, QueryObject};
use jmap::orm::serialize::JMAPOrm;
use jmap::request::query::{QueryRequest, QueryResponse};
use jmap::request::ACLEnforce;
use jmap::types::jmap::JMAPId;
use std::collections::{HashMap, HashSet};
use store::core::acl::ACL;
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::core::tag::Tag;
use store::read::comparator::{self, FieldComparator};
use store::read::default_filter_mapper;
use store::read::filter::{self, Query};
use store::Store;
use store::{AccountId, JMAPStore};

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct QueryArguments {
    #[serde(rename = "sortAsTree")]
    sort_as_tree: Option<bool>,
    #[serde(rename = "filterAsTree")]
    filter_as_tree: Option<bool>,
}

impl QueryObject for Mailbox {
    type QueryArguments = QueryArguments;

    type Filter = Filter;

    type Comparator = Comparator;
}

pub trait JMAPMailboxQuery<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mailbox_query(&self, request: QueryRequest<Mailbox>) -> jmap::Result<QueryResponse>;
}

impl<T> JMAPMailboxQuery<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mailbox_query(&self, request: QueryRequest<Mailbox>) -> jmap::Result<QueryResponse> {
        let mut helper = QueryHelper::new(
            self,
            request,
            (|account_id: AccountId, member_of: &[AccountId]| {
                self.mail_shared_folders(account_id, member_of, ACL::ReadItems)
            })
            .into(),
        )?;
        let account_id = helper.account_id;
        let primary_account_id = helper.request.acl.as_ref().unwrap().primary_id();
        let sort_as_tree = helper.request.arguments.sort_as_tree.unwrap_or(false);
        let filter_as_tree = helper.request.arguments.filter_as_tree.unwrap_or(false);

        helper.parse_filter(|filter| {
            Ok(match filter {
                Filter::ParentId { value } => filter::Filter::eq(
                    Property::ParentId.into(),
                    Query::LongInteger(value.map(|id| u64::from(id) + 1).unwrap_or(0)),
                ),
                Filter::Name { value } => {
                    #[cfg(feature = "debug")]
                    {
                        // Used for concurrent requests tests
                        if value == "__sleep" {
                            std::thread::sleep(std::time::Duration::from_secs(1));
                        }
                    }
                    filter::Filter::eq(Property::Name.into(), Query::Tokenize(value.to_lowercase()))
                }
                Filter::Role { value } => {
                    if let Some(value) = value {
                        filter::Filter::eq(
                            Property::Role.into(),
                            Query::Tokenize(value.to_lowercase()),
                        )
                    } else {
                        filter::Filter::not(vec![filter::Filter::eq(
                            Property::Role.into(),
                            Query::Tag(Tag::Default),
                        )])
                    }
                }
                Filter::HasAnyRole { value } => {
                    let filter =
                        filter::Filter::eq(Property::Role.into(), Query::Tag(Tag::Default));
                    if !value {
                        filter::Filter::not(vec![filter])
                    } else {
                        filter
                    }
                }
                Filter::IsSubscribed { value } => {
                    let filter = filter::Filter::eq(
                        Property::IsSubscribed.into(),
                        Query::Integer(primary_account_id),
                    );
                    if !value {
                        filter::Filter::not(vec![filter])
                    } else {
                        filter
                    }
                }
                Filter::Unsupported { value } => {
                    return Err(MethodError::UnsupportedFilter(value));
                }
            })
        })?;

        helper.parse_comparator(|comparator| {
            Ok(comparator::Comparator::Field(FieldComparator {
                field: {
                    match comparator.property {
                        Comparator::Name => Property::Name,
                        Comparator::SortOrder => Property::SortOrder,
                        Comparator::ParentId => Property::ParentId,
                    }
                }
                .into(),
                ascending: comparator.is_ascending,
            }))
        })?;

        if filter_as_tree || sort_as_tree {
            helper.query(
                default_filter_mapper,
                Some(|mut results: Vec<JMAPId>| {
                    let mut hierarchy = HashMap::new();
                    let mut tree = HashMap::new();

                    for doc_id in self
                        .get_document_ids(account_id, Collection::Mailbox)?
                        .unwrap_or_default()
                    {
                        let parent_id = self
                            .get_orm::<Mailbox>(account_id, doc_id)?
                            .ok_or_else(|| {
                                StoreError::InternalError("Mailbox data not found".to_string())
                            })?
                            .get(&Property::ParentId)
                            .and_then(|v| v.as_id())
                            .unwrap_or_default();
                        hierarchy.insert((doc_id + 1) as u64, parent_id);
                        tree.entry(parent_id)
                            .or_insert_with(HashSet::new)
                            .insert((doc_id + 1) as u64);
                    }

                    if filter_as_tree {
                        let mut filtered_ids = Vec::with_capacity(results.len());

                        for &doc_id in &results {
                            let mut keep = false;
                            let mut jmap_id = u64::from(doc_id) + 1;

                            for _ in 0..self.config.mailbox_max_depth {
                                if let Some(&parent_id) = hierarchy.get(&jmap_id) {
                                    if parent_id == 0 {
                                        keep = true;
                                        break;
                                    } else if !results.contains(&JMAPId::from(parent_id - 1)) {
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
                            let (mut children, mut it) =
                                if let Some(children) = tree.remove(&jmap_id) {
                                    (children, results.iter())
                                } else if let Some(prev) = stack.pop() {
                                    prev
                                } else {
                                    break;
                                };

                            while let Some(&doc_id) = it.next() {
                                jmap_id = u64::from(doc_id) + 1;
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
                    Ok(results)
                }),
            )
        } else {
            helper.query(default_filter_mapper, None::<ExtraFilterFnc>)
        }
    }
}
